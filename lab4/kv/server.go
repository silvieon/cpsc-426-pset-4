package kv

import (
	"context"
	"math/rand/v2"

	"sync" // i added
	"time" // i added

	"google.golang.org/grpc/codes"  // i added
	"google.golang.org/grpc/status" // i added

	"cs426.yale.edu/lab4/kv/proto"
	"github.com/sirupsen/logrus"
)

type KvServerImpl struct {
	proto.UnimplementedKvServer
	nodeName string

	shardMap   *ShardMap
	listener   *ShardMapListener
	clientPool ClientPool
	shutdown   chan struct{}

	// i added these two
	mu     sync.RWMutex // server.mu will protect server.shards for when we add and remove shards
	shards map[int]*shardData
}

// my structs
// kv entry - one stored item. ex:
// value: world, expiresAt: time.Now().Add(10 * time.Second)
type entry struct {
	value     string
	expiresAt time.Time
}

// collection ef key value pairs belonging to one shard
type shardData struct {
	mu    sync.RWMutex // will protect the actual key-value pairs within each shard
	items map[string]entry
}

// HELPER FUNCTIONS ------------------------------------------------

// DONE: Part C
func (server *KvServerImpl) handleShardMapUpdate() {
	target := make(map[int]bool)
	for _, shard := range server.shardMap.ShardsForNode(server.nodeName) {
		target[shard] = true
	}

	server.mu.RLock()
	current := make(map[int]bool)
	for shard := range server.shards {
		current[shard] = true
	}
	server.mu.RUnlock()

	server.mu.Lock()
	for shard := range current {
		if !target[shard] {
			delete(server.shards, shard)
		}
	}
	server.mu.Unlock()

	var wg sync.WaitGroup
	for shard := range target {
		if current[shard] {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := &shardData{
				items: server.copyShard(shard),
			}
			server.mu.Lock()
			server.shards[shard] = data
			server.mu.Unlock()
		}()
	}
	wg.Wait()
}

// my function to get rid of expired entries
func (server *KvServerImpl) cleanupExpired() {
	// snapshot of shards
	server.mu.RLock()
	shards := make([]*shardData, 0, len(server.shards))
	for _, data := range server.shards {
		shards = append(shards, data)
	}
	server.mu.RUnlock()

	now := time.Now()

	// get rid of expired entries from each shard
	for _, data := range shards {
		data.mu.Lock()

		for key, value := range data.items {
			if !now.Before(value.expiresAt) {
				delete(data.items, key)
			}
		}

		// release old map's storage when empty
		if len(data.items) == 0 {
			data.items = make(map[string]entry)
		}

		data.mu.Unlock()
	}
}

// made by me
// cleans up expired entries every 250 milliseconds
func (server *KvServerImpl) cleanupLoop() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-server.shutdown:
			return
		case <-ticker.C:
			server.cleanupExpired()
		}
	}
}

func (server *KvServerImpl) shardMapListenLoop() {
	listener := server.listener.UpdateChannel()
	for {
		select {
		case <-server.shutdown:
			return
		case _, ok := <-listener:
			if !ok {
				return
			}
			server.handleShardMapUpdate()
		}
	}
}

func (server *KvServerImpl) lookupShard(shard int) (*shardData, error) {
	server.mu.RLock()
	defer server.mu.RUnlock()

	data, ok := server.shards[shard]
	if !ok {
		return nil, status.Error(codes.NotFound, "shard not found")
	}

	return data, nil
}

// // helper function i added
// func (server *KvServerImpl) getOrCreateShard(shard int) *shardData {
// 	// protects the shard map
// 	server.mu.Lock()
// 	defer server.mu.Unlock()

// 	// exists
// 	if data, ok := server.shards[shard]; ok {
// 		return data
// 	}

// 	// does not exist
// 	data := &shardData {
// 		// --> make an empty map for that shard
// 		items: make(map[string]entry),
// 	}

// 	server.shards[shard] = data
// 	return data
// }

// // my function - check if a server hosts a shard
// func (server *KvServerImpl) hostsShard(shard int) bool {
//     for _, s := range server.shardMap.ShardsForNode(server.nodeName) {
//         if s == shard {
//             return true
//         }
//     }
//     return false
// }

func (server *KvServerImpl) copyShard(shard int) map[string]entry {
	peers := make([]string, 0)
	for _, node := range server.shardMap.NodesForShard(shard) {
		if node != server.nodeName {
			peers = append(peers, node)
		}
	}

	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})

	if len(peers) == 0 {
		return make(map[string]entry) // brand-new shard, nothing to copy
	}

	for _, peer := range peers {
		client, err := server.clientPool.GetClient(peer)
		if err != nil {
			logrus.WithFields(logrus.Fields{"node": server.nodeName, "peer": peer, "shard": shard}).WithError(err).Warn("GetClient failed")
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := client.GetShardContents(ctx, &proto.GetShardContentsRequest{Shard: int32(shard)})
		cancel() // release the timer right away; don't defer inside a loop
		if err != nil {
			logrus.WithFields(logrus.Fields{"node": server.nodeName, "peer": peer, "shard": shard}).WithError(err).Warn("failed to get shard contents from peer")
			continue
		}

		items := make(map[string]entry)
		now := time.Now()
		for _, value := range resp.Values {
			expiration := now.Add(time.Duration(value.TtlMsRemaining) * time.Millisecond)
			items[value.Key] = entry{
				value:     value.Value,
				expiresAt: expiration,
			}
		}

		return items
	}

	logrus.Errorf("node %s failed to copy shard %d from any peer", server.nodeName, shard)
	return make(map[string]entry)
}

// END HELPER FUNCTIONS --------------------------------------------

func MakeKvServer(nodeName string, shardMap *ShardMap, clientPool ClientPool) *KvServerImpl {
	listener := shardMap.MakeListener()
	server := KvServerImpl{
		nodeName:   nodeName,
		shardMap:   shardMap,
		listener:   &listener,
		clientPool: clientPool,
		shutdown:   make(chan struct{}),
		shards:     make(map[int]*shardData), // i added
	}
	server.handleShardMapUpdate()
	go server.shardMapListenLoop()

	// i added
	// asynchronous go routine to clean up expired entries
	go server.cleanupLoop()
	// end i added

	return &server
}

func (server *KvServerImpl) Shutdown() {
	// server.shutdown <- struct{}{}
	close(server.shutdown) // because now have two goroutines listening so i had to change
	server.listener.Close()
}

func (server *KvServerImpl) Get(
	ctx context.Context,
	request *proto.GetRequest,
) (*proto.GetResponse, error) {
	// Trace-level logging for node receiving this request (enable by running with -log-level=trace),
	// feel free to use Trace() or Debug() logging in your code to help debug tests later without
	// cluttering logs by default. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Get() request")

	// panic("TODO: Part A")

	// check key isn't empty
	if request.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}

	// find shard containing key
	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	data, err := server.lookupShard(shard)
	if err != nil {
		return nil, err
	}

	// acquire shard's read lock
	data.mu.RLock()
	defer data.mu.RUnlock()

	response, exists := data.items[request.Key]

	if !exists {
		return &proto.GetResponse{WasFound: false}, nil
	}

	if !time.Now().Before(response.expiresAt) {
		return &proto.GetResponse{WasFound: false}, nil
	}

	return &proto.GetResponse{
		Value:    response.value,
		WasFound: true,
	}, nil

	// look up request.Key in data.items
	// if key doesn't exist or has expired return WasFound: false
	// otherwise return value with WasFound true
}

func (server *KvServerImpl) Set(
	ctx context.Context,
	request *proto.SetRequest,
) (*proto.SetResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Set() request")

	// panic("TODO: Part A")

	// check key isn't empty
	if request.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}

	// calculate which shard the key belongs to
	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	// find that shard's storage
	data, err := server.lookupShard(shard)
	if err != nil {
		return nil, err
	}

	// acquire its write lock
	data.mu.Lock()
	defer data.mu.Unlock()

	// calculate when value expires
	expiration := time.Now().Add(time.Duration(request.TtlMs) * time.Millisecond)

	// store the value and expiration timestamp
	data.items[request.Key] = entry{
		value:     request.Value,
		expiresAt: expiration,
	}

	// release the lock
	// return success
	return &proto.SetResponse{}, nil
}

func (server *KvServerImpl) Delete(
	ctx context.Context,
	request *proto.DeleteRequest,
) (*proto.DeleteResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Delete() request")

	// panic("TODO: Part A")
	if request.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	data, err := server.lookupShard(shard)
	if err != nil {
		return nil, err
	}

	data.mu.Lock()
	defer data.mu.Unlock()

	// go automatically handles the case where the key doesn't exist
	delete(data.items, request.Key)

	return &proto.DeleteResponse{}, nil
}

// DONE: Part C
func (server *KvServerImpl) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (*proto.GetShardContentsResponse, error) {
	data, err := server.lookupShard(int(request.Shard))
	if err != nil {
		return nil, err
	}

	data.mu.RLock()
	defer data.mu.RUnlock()

	values := make([]*proto.GetShardValue, 0, len(data.items))

	for key, e := range data.items {
		remainingTtlMs := time.Until(e.expiresAt).Milliseconds()
		if remainingTtlMs <= 0 {
			continue // skip expired entries
		}
		values = append(values, &proto.GetShardValue{
			Key:            key,
			Value:          e.value,
			TtlMsRemaining: remainingTtlMs,
		})
	}

	return &proto.GetShardContentsResponse{Values: values}, nil
}
