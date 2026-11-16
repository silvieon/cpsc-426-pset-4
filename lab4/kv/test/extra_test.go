package kvtest

import (
	"context"
	"fmt"
	"time"

	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"cs426.yale.edu/lab4/kv"
	"cs426.yale.edu/lab4/kv/proto"
)

// Like previous labs, you must write some tests on your own.
// Add your test cases in this file and submit them as extra_test.go.
// You must add at least 5 test cases, though you can add as many as you like.
//
// You can use any type of test already used in this lab: server
// tests, client tests, or integration tests.
//
// You can also write unit tests of any utility functions you have in utils.go
//
// Tests are run from an external package, so you are testing the public API
// only. You can make methods public (e.g. utils) by making them Capitalized.

func TestExtraTtlShortenedOnOverwrite(t *testing.T) {
	// Your server must appropriately update TTLs, even if the value already
	// exists in cache -- in this case we set a TTL of 100ms, then overwrite
	// with a TTL of 100s.
	setup := MakeTestSetup(MakeBasicOneShard())

	err := setup.NodeSet("n1", "abc", "123", 100*time.Second)
	assert.Nil(t, err)

	err = setup.NodeSet("n1", "abc", "456", 100*time.Millisecond)
	assert.Nil(t, err)

	time.Sleep(200 * time.Millisecond)

	_, wasFound, err := setup.NodeGet("n1", "abc")
	assert.Nil(t, err)
	assert.False(t, wasFound)

	setup.Shutdown()
}

func TestExtraDeleteIdempotency(t *testing.T) {
	// Deleting a key that doesn't exist should not cause an error.
	// Deleting a key that has expired should not cause an error.
	// Deleting a key that has already been deleted should not cause an error.
	setup := MakeTestSetup(MakeBasicOneShard())

	// key that doesn't exist
	assert.Nil(t, setup.NodeDelete("n1", "never-set"))

	// key that has expired
	assert.Nil(t, setup.NodeSet("n1", "abc", "123", 50*time.Millisecond))
	time.Sleep(100 * time.Millisecond)
	assert.Nil(t, setup.NodeDelete("n1", "abc"))

	// key that has already been deleted
	assert.Nil(t, setup.NodeSet("n1", "xyz", "789", 10*time.Second))
	assert.Nil(t, setup.NodeDelete("n1", "xyz"))
	assert.Nil(t, setup.NodeDelete("n1", "xyz"))

	_, wasFound, err := setup.NodeGet("n1", "xyz")
	assert.Nil(t, err)
	assert.False(t, wasFound)

	setup.Shutdown()
}

func TestExtraGetShardContents(t *testing.T) {
	setup := MakeTestSetup(MakeTwoNodeMultiShard())
	numShards := setup.NumShards()

	var keyShard1, keyShard2 string
	for i := 0; keyShard1 == "" || keyShard2 == ""; i++ {
		key := fmt.Sprintf("key%d", i)
		switch kv.GetShardForKey(key, numShards) {
		case 1:
			keyShard1 = key
		case 2:
			keyShard2 = key
		}
	}

	assert.Nil(t, setup.NodeSet("n1", keyShard1, "one", 10*time.Second))
	assert.Nil(t, setup.NodeSet("n1", keyShard2, "two", 10*time.Second))

	time.Sleep(100 * time.Millisecond)

	resp, err := setup.nodes["n1"].GetShardContents(
		context.Background(),
		&proto.GetShardContentsRequest{Shard: 1},
	)
	assert.Nil(t, err)
	assert.Len(t, resp.Values, 1)
	assert.Equal(t, keyShard1, resp.Values[0].Key)
	assert.Equal(t, "one", resp.Values[0].Value)

	assert.Less(t, resp.Values[0].TtlMsRemaining, (10 * time.Second).Milliseconds())

	_, err = setup.nodes["n1"].GetShardContents(
		context.Background(),
		&proto.GetShardContentsRequest{Shard: 6},
	)
	assertShardNotAssigned(t, err)

	setup.Shutdown()
}

func TestExtraShardCopyPreservesTtl(t *testing.T) {
	setup := MakeTestSetup(MakeTwoNodeBothAssignedSingleShard())

	assert.Nil(t, setup.NodeSet("n1", "abc", "123", 1*time.Second))
	time.Sleep(500 * time.Millisecond)

	setup.UpdateShardMapping(map[int][]string{1: {"n1"}})
	setup.UpdateShardMapping(map[int][]string{1: {"n1", "n2"}})

	val, wasFound, err := setup.NodeGet("n2", "abc")
	assert.Nil(t, err)
	assert.True(t, wasFound)
	assert.Equal(t, val, "123")

	time.Sleep(600 * time.Millisecond)

	_, wasFound, err = setup.NodeGet("n2", "abc")
	assert.Nil(t, err)
	assert.False(t, wasFound)

	setup.Shutdown()
}

func TestExtraReaddedShardNoPeersStartsEmpty(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	assert.Nil(t, setup.NodeSet("n1", "abc", "123", 10*time.Second))

	setup.UpdateShardMapping(map[int][]string{1: {}})
	setup.UpdateShardMapping(map[int][]string{1: {"n1"}})

	_, wasFound, err := setup.NodeGet("n1", "abc")
	assert.Nil(t, err)
	assert.False(t, wasFound)

	setup.Shutdown()
}

func TestExtraSetPartialFailureWritesHealthyReplica(t *testing.T) {
	setup := MakeTestSetup(MakeTwoNodeBothAssignedSingleShard())
	setup.clientPool.OverrideGetClientError("n2", errors.New("n2 is down"))

	err := setup.Set("abc", "123", 10*time.Second)
	assert.NotNil(t, err)

	val, wasFound, err := setup.NodeGet("n1", "abc")
	assert.Nil(t, err)
	assert.True(t, wasFound)
	assert.Equal(t, "123", val)

	// Repeat so the random starting node hits n2 at least some of the time.
	for range 10 {
		val, wasFound, err = setup.Get("abc")
		assert.Nil(t, err)
		assert.True(t, wasFound)
		assert.Equal(t, "123", val)
	}

	setup.Shutdown()
}
