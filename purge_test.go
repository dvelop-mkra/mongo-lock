// Copyright 2018, Square, Inc.

package lock_test

import (
	"context"
	"sort"

	"testing"
	"time"

	lock "github.com/square/mongo-lock"
	"github.com/stretchr/testify/assert"
)

func TestPurge(t *testing.T) {
	// setup and teardown are defined in lock_test.go
	collection := setup(t)
	defer teardown(t, collection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	client := lock.NewClient(collection)

	// Create some locks.
	err := client.XLock(ctx, "resource1", "aaaa", lock.LockDetails{})
	assert.NoError(t, err)
	err = client.XLock(ctx, "resource2", "bbbb", lock.LockDetails{TTL: 1})
	assert.NoError(t, err)
	err = client.SLock(ctx, "resource3", "cccc", lock.LockDetails{TTL: 1}, -1)
	assert.NoError(t, err)

	// Sleep for a second to let TTLs expire
	time.Sleep(time.Duration(1500) * time.Millisecond)

	// Purge the locks.
	purger := lock.NewPurger(client)
	purged, err := purger.Purge(ctx)
	assert.NoError(t, err)
	assert.Len(t, purged, 2, "expected 2 locks to be purged")

	var purgedSorted lock.LockStatusesByCreatedAtDesc
	purgedSorted = purged
	sort.Sort(purgedSorted)
	assert.Equal(t, "resource3", purged[0].Resource, "expected resource3 to be purged first")
	assert.Equal(t, "resource2", purged[1].Resource, "expected resource2 to be purged second")
}

func TestPurgeSameLockIdDiffTTLs(t *testing.T) {
	// setup and teardown are defined in lock_test.go
	collection := setup(t)
	defer teardown(t, collection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	client := lock.NewClient(collection)

	// Create some locks with different TTLs, all owned by the same lockId.
	err := client.XLock(ctx, "resource1", "aaaa", lock.LockDetails{}) // no TTL
	assert.NoError(t, err)
	err = client.XLock(ctx, "resource2", "aaaa", lock.LockDetails{TTL: 30})
	assert.NoError(t, err)
	err = client.SLock(ctx, "resource3", "aaaa", lock.LockDetails{TTL: 1}, -1)
	assert.NoError(t, err)

	// Sleep for a second to let some TTLs expire
	time.Sleep(time.Duration(1500) * time.Millisecond)

	// Purge the locks.
	purger := lock.NewPurger(client)
	purged, err := purger.Purge(ctx)
	assert.NoError(t, err)
	assert.Len(t, purged, 3, "expected 3 locks to be purged")

	var purgedSorted lock.LockStatusesByCreatedAtDesc
	purgedSorted = purged
	sort.Sort(purgedSorted)
	assert.Equal(t, "resource3", purged[0].Resource, "expected resource3 to be purged first")
	assert.Equal(t, "resource2", purged[1].Resource, "expected resource2 to be purged second")
	assert.Equal(t, "resource1", purged[2].Resource, "expected resource1 to be purged last")
}
