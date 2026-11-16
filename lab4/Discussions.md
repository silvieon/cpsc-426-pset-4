A4: TTLs
What tradeoffs did you make with your TTL strategy? How fast does it clean up data and how expensive is it in terms of number of keys?

If the server was write intensive, would you design it differently? How so?

We store an absolute expiry time with each value and use two mechanisms. For correctness, Get (and GetShardContents) checks the expiry time on every read and treats expired values as missing, so clients never see expired data regardless of when cleanup runs. For memory, a background goroutine sweeps all shards every 250ms and deletes expired entries, so data is cleaned up within roughly 250ms of expiring. The tradeoff is that the design is simple and adds no cost to Set (no extra index to maintain), but each sweep is O(N) in the total number of keys, not just the expired ones, so servers holding many keys do lots of checks even when nothing is expiring. The sweep also holds each shard's write lock while scanning it, which blocks all Gets and Sets to that shard, so latency may spike during sweeps of large shards.

For a write-intensive server, writes would contend with the sweep for the shard write locks more often, and more keys would make each sweep longer. One option is a min-heap (or expiry-time buckets) ordered by expiry, so cleanup only touches keys that have actually expired, but that adds an O(log n) update to every Set, which is exactly the path a write-heavy server wants to keep cheap. Options that avoid taxing writes are sweeping in small chunks (releasing the lock every few hundred keys so writers never wait behind a full-shard scan) or Redis-style random sampling, where cleanup cost scales with how many keys are expiring rather than the total.

B2: Load balancing
What flaws could you see in the load balancing strategy you chose? Consider cases where nodes may naturally have load imbalance already -- a node may have more CPUs available, or a node may have more shards assigned.

What other strategies could help in this scenario? You may assume you can make any changes to the protocol or cluster operation.

Our random load balancing treats all replicas of a shard equally, without any regard for the resources a node has or how much other work it is doing. A node with fewer CPUs gets the same share as a larger one and becomes the bottleneck. Balancing also only happens among the replicas of a single shard, so total load on a node depends on how many shards it hosts: a node that is the only replica for several shards gets all of their traffic, and random choice can't move it anywhere. We also don't react to hot shards, slow nodes, or dead nodes (a dead replica is still picked first about 1/R of the time, costing a failed attempt).

On the client side, weighted random selection (weighting nodes by capacity, e.g. CPU count) handles uneven hardware, and choosing the replica with the fewest outstanding requests (or "power of two choices": pick two at random and use the less busy one) or with the lowest recent latency adapts to load as it happens. Servers could also report their load in responses so clients can steer away from busy nodes. Imbalance from shard count, though, has to be fixed in the shard map itself: moving shards off overloaded nodes, or adding replicas for hot shards so their reads can be spread out.

B3: Failovers
For this lab you will try every node until one succeeds. What flaws do you see in this strategy? What could you do to help in these scenarios?

Because attempts are sequential, each failed node adds its full wait time before the next replica is tried, so failures show up directly as latency. There is also no timeout per attempt: every attempt shares the caller's context, so a single hung node can use up the entire deadline and the healthy replicas never get tried. The client also doesn't remember which nodes failed, so a dead node is retried on every request and the cost is paid again and again. Every type of error is retried, even ones another node can't fix (e.g. InvalidArgument for an empty key fails on every node). Finally, failover can make overload worse: if a node fails because it is overloaded, its traffic moves to the other replicas, which can overload them too.

Fixes: give each attempt its own timeout (a slice of the overall deadline); use hedged requests (if the first node hasn't answered within the typical latency, also send to a second and take whichever responds first); use health checks or circuit breakers to skip nodes that recently failed; only retry errors where another node could help (Unavailable, timeouts), and on NotFound refresh the shard map before retrying; and cap retries with a retry budget or backoff so failover can't overload the remaining nodes.

B4: Set
What are the ramifications of partial failures on Set calls? What anomalies could a client observe?

Partial failures on Set calls leave replicas holding different data. A Set that partially fails updates some replicas but not others, and there is no rollback, versioning, or repair, so the replicas stay inconsistent until the key expires or is written again.

Anomalies a client could observe:
- Reads that flip back and forth: Get picks a random replica, so repeated Gets of the same key can return the new value, then the old one (or nothing), then the new one again.
- A "failed" write that is visible anyway: the client gets an error, but later Gets return the value because it succeeded on some replicas. Our node-failure experiment in D2 shows this.
- Mismatched TTLs: if the client retries after a partial failure, the retry restarts the TTL on the replicas that already had the value, so the key expires at different times on different replicas.
- Stale data spreading through migration: a new replica may copy the shard from a replica that missed the write, spreading the old value to more nodes.
- Permanent disagreement from concurrent writes: two clients Set the same key at once and the writes reach replicas in different orders, so each replica keeps a different value, even though neither Set failed.
- Deleted keys reappearing: Delete has the same behavior, so a key can still be read from a replica that missed the delete.

D2: Stress testing

Both experiments use a 3-node cluster started from `shardmaps/test-3-node.json`, with one server per node:

```
go run cmd/server/server.go --shardmap <map> --node n1   # likewise n2, n3
```

Shard layout of `test-3-node.json`: shard 1 -> {n1, n3}, 2 -> {n1}, 3 -> {n2, n3}, 4 -> {n1, n2, n3}, 5 -> {n2}.
(The only change from the provided file was moving the ports to 9101-9103, so the runs didn't collide with another cluster already on 9001-9003.)

### Experiment 1: Graceful live shard migration

**Command:**
`go run cmd/stress/tester.go --shardmap shardmaps\migrate.json --duration=45s`

**Shard map:** started as a copy of `test-3-node.json`, then edited while the test ran to move shard 2 from n1 to n2 in two steps:
1. At ~15s: `"2": ["n1"]` -> `"2": ["n1", "n2"]` (n2 copies shard 2 from n1 via `GetShardContents`)
2. At ~30s: `"2": ["n1", "n2"]` -> `"2": ["n2"]` (n1 drops shard 2)

**Testing for:** Part C migration under live traffic. Do clients see errors or lost data while a shard changes owners?

**Results:**
```
Get requests: 4520/4520 succeeded = 100.000000% success rate
Set requests: 1370/1370 succeeded = 100.000000% success rate
Correct responses: 4499/4499 = 100.000000%
Total requests: 5890 = 130.886634 QPS
```
Server debug logs confirm all three nodes applied both map updates mid-run, and n2 copied shard 2 with no errors. Adding the new replica before removing the old one meant shard 2 always had a node that held its data, so the move was invisible to clients.

**Interesting result (Windows quirk):** The first attempt passed the path as `shardmaps/migrate.json`, and the servers silently ignored every edit. The shard map watcher (`kv/file_shardmap.go`) compares the fsnotify event name to the `--shardmap` argument as a plain string. On Windows, fsnotify reports `shardmaps\migrate.json` with a backslash, so the names never match. Passing the path with backslashes fixed it.

### Experiment 2: Node failure

**Command:**
`go run cmd/stress/tester.go --shardmap shardmaps/test-3-node.json --duration=45s`

The n2 process was killed at ~15s into the run and not restarted.

**Testing for:** Client failover (B3) and partial-failure Sets (B4) when a node dies.

**Results:**
```
Get requests: 3808/4520 succeeded = 84.247788% success rate
Set requests: 856/1370 succeeded = 62.481752% success rate
Correct responses: 3787/3787 = 100.000000%
Total requests: 5890 = 130.884580 QPS
```
The failure rates match what the shard map predicts. n2 was down for about 2/3 of the run:
- **Gets:** only shard 5 has n2 as its *sole* replica, so about 1/5 of keys become unreadable. Expected failure ≈ 2/3 × 1/5 ≈ 13%; observed 15.8%. Gets for shards 3 and 4 still succeeded by failing over to n3 / n1.
- **Sets:** a Set fails if *any* replica fails. n2 hosts shards 3, 4 and 5, so about 3/5 of keys. Expected failure ≈ 2/3 × 3/5 = 40%; observed 37.5%.

Correct responses stayed at 100%. Failed Sets still wrote to the surviving replicas, and the checker accounts for partial-failure writes, so no reads were inconsistent. This shows the B4 tradeoff: a "failed" Set may still be visible to later Gets.