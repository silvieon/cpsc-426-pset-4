package kv

import (
	"context"
	"time"
	"fmt" // i added
	"sync" // i added
	"math/rand" // i added

	"cs426.yale.edu/lab4/kv/proto" // i added
	"github.com/sirupsen/logrus"
)

type Kv struct {
	shardMap   *ShardMap
	clientPool ClientPool

	// Add any client-side state you want here
}

func MakeKv(shardMap *ShardMap, clientPool ClientPool) *Kv {
	kv := &Kv{
		shardMap:   shardMap,
		clientPool: clientPool,
	}
	// Add any initialization logic
	return kv
}

func (kv *Kv) Get(ctx context.Context, key string) (string, bool, error) {
	// Trace-level logging -- you can remove or use to help debug in your tests
	// with `-log-level=trace`. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Get() request")

	// panic("TODO: Part B")

	// find which shard has the key
	shard := GetShardForKey(key, kv.shardMap.NumShards())

	// find nodes that host this shard
	nodes := kv.shardMap.NodesForShard(shard)

	// error of no nodes host shard
	if len(nodes) == 0 {
		return "", false, fmt.Errorf("no nodes host shard %d", shard)
	}

	// choose first node for now
	// node := nodes[0]
	// choose random node (load balancing)
	// node := nodes[rand.Intn(len(nodes))]

	start := rand.Intn(len(nodes))

    var lastErr error

    // try each node at most once
    for i := 0; i < len(nodes); i++ {

		node := nodes[(start + i)%len(nodes)]

		// get a client for this node
		client, err := kv.clientPool.GetClient(node)
		if err != nil {
			lastErr = err
			continue
		}

		// send get request to the server
		response, err := client.Get(ctx, &proto.GetRequest{
			Key: key,
		})
		if err != nil {
			lastErr = err
            continue
		}

		return response.Value, response.WasFound, nil
	}

	// all nodes failed
	return "", false, lastErr
}

func (kv *Kv) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Set() request")

	// panic("TODO: Part B")

	// find which shard has key
	shard := GetShardForKey(key, kv.shardMap.NumShards())

	// find all nodes that host this shard
	nodes := kv.shardMap.NodesForShard(shard)

	if len(nodes) == 0 {
		// this shouldn't happen right?
		return fmt.Errorf("no nodes host shard %d", shard)
	}

	var wg sync.WaitGroup
	errors := make(chan error, len(nodes))

	// send set to each node concurrently
	for _, node := range nodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()

			client, err := kv.clientPool.GetClient(node)
			if err != nil {
				errors <- err
				return
			}

			_, err = client.Set(ctx, &proto.SetRequest{
				Key: key,
				Value: value,
				TtlMs: ttl.Milliseconds(),
			})

			if err != nil {
				errors <- err
			}
		}(node)
	}

	// wait for all reqs to finish
	wg.Wait()
	close(errors)

	// return err if a node failed

	for err := range errors {
		return err
	}

	return nil

}

func (kv *Kv) Delete(ctx context.Context, key string) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Delete() request")

	// panic("TODO: Part B")

	// find the shard containing the key
    shard := GetShardForKey(key, kv.shardMap.NumShards())

    // find all nodes hosting that shard
    nodes := kv.shardMap.NodesForShard(shard)

    if len(nodes) == 0 {
        return fmt.Errorf("no nodes host shard %d", shard)
    }

    var wg sync.WaitGroup
    errors := make(chan error, len(nodes))

	// send delete to each replica concurrently
    for _, node := range nodes {
        wg.Add(1)

        go func(node string) {
            defer wg.Done()

            client, err := kv.clientPool.GetClient(node)
            if err != nil {
                errors <- err
                return
            }

            _, err = client.Delete(ctx, &proto.DeleteRequest{
                Key: key,
            })
            if err != nil {
                errors <- err
            }
        }(node)
    }

	// wait for all del reqs to finish
    wg.Wait()
    close(errors)

    // return err if a replica failed
    for err := range errors {
        return err
    }

    return nil
}
