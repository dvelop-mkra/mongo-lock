// Copyright 2018, Square, Inc.

package lock_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	lock "github.com/square/mongo-lock"
)

func getRandomString() string {
	n := 5
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%X", b)
}

var testDb *mongo.Client

func setup(t *testing.T) *mongo.Collection {
	collection := getRandomString()

	var err error
	if testDb == nil {
		testDb, err = mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))

		if err != nil {
			t.Fatal(err)
		}
	}

	// Add the required unique index on the 'resource' field.
	index := mongo.IndexModel{
		Keys:    bson.M{"resource": 1},
		Options: options.Index().SetUnique(true).SetSparse(true),
	}

	_, err = testDb.Database("test").Collection(collection).Indexes().CreateOne(context.Background(), index)
	if err != nil {
		t.Fatal(err)
	}

	return testDb.Database("test").Collection(collection)
}

func teardown(t *testing.T, c *mongo.Collection) {
	if testDb == nil {
		t.Errorf("must call setup before teardown")
	}

	if err := c.Drop(context.Background()); err != nil {
		t.Error(err)
	}
}

type index struct {
	Name string
	Keys bson.D
}

func TestCreateIndexes(t *testing.T) {
	collection := setup(t)
	defer teardown(t, collection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	client := lock.NewClient(collection)

	err := client.CreateIndexes(ctx)
	assert.NoError(t, err)

	cur, err := collection.Indexes().List(ctx)
	assert.NoError(t, err)
	defer cur.Close(ctx)

	expectedIndexes := []index{
		{Name: "_id_", Keys: bson.D{bson.E{"_id", int32(1)}}},
		{Name: "resource_1", Keys: bson.D{bson.E{"resource", int32(1)}}},
		{Name: "exclusive.lockId_1", Keys: bson.D{bson.E{"exclusive.lockId", int32(1)}}},
		{Name: "exclusive.expiresAt_1", Keys: bson.D{bson.E{"exclusive.expiresAt", int32(1)}}},
		{Name: "shared.locks.lockId_1", Keys: bson.D{bson.E{"shared.locks.lockId", int32(1)}}},
		{Name: "shared.locks.expiresAt_1", Keys: bson.D{bson.E{"shared.locks.expiresAt", int32(1)}}},
	}

	indexes := make([]index, 0, 6)
	for cur.Next(ctx) {
		var result bson.D

		err := cur.Decode(&result)

		if err != nil {
			t.Error(err)
		}

		var indexName string
		var keys bson.D
		for _, elem := range result {
			if elem.Key == "name" {
				indexName = elem.Value.(string)
			}
			if elem.Key == "key" {
				keys = elem.Value.(bson.D)
			}
		}
		indexes = append(indexes, index{
			Name: indexName,
			Keys: keys,
		})
	}

	if err := cur.Err(); err != nil {
		t.Error(err)
	}

	if len(indexes) != 6 {
		t.Errorf("expected 6 indexes. found %d", len(indexes))
	}
	assert.Equal(t, indexes, expectedIndexes)
}

func TestLockExclusive(t *testing.T) {
	collection := setup(t)
	defer teardown(t, collection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	client := lock.NewClient(collection)

	// Create some locks.
	err := client.XLock(ctx, "resource1", "aaaa", lock.LockDetails{})
	assert.NoError(t, err)
	err = client.XLock(ctx, "resource2", "aaaa", lock.LockDetails{})
	assert.NoError(t, err)
	err = client.XLock(ctx, "resource3", "bbbb", lock.LockDetails{})
	assert.NoError(t, err)

	// Try to lock something that's already locked.
	err = client.XLock(ctx, "resource1", "aaaa", lock.LockDetails{})
	if err != lock.ErrAlreadyLocked {
		t.Errorf("err = %s, expected the lock to fail due to the resource already being locked", err)
	}
	err = client.XLock(ctx, "resource1", "zzzz", lock.LockDetails{})
	if err != lock.ErrAlreadyLocked {
		t.Errorf("err = %s, expected the lock to fail due to the resource already being locked", err)
	}

	// Create a lock with some resource name and some lockId
	// that resource expires after some time
	// then with same resource name and lockId can able to create lock.
	err = client.XLock(ctx, "resource4", "cccc", lock.LockDetails{TTL: 1})
	assert.NoError(t, err)

	// Waiting to expire the lock with resource name "resource4" and lockId "cccc".
	time.Sleep(1100 * time.Millisecond)

	// Try to lock "reource4" with "cccc", which is already expired.
	err = client.XLock(ctx, "resource4", "cccc", lock.LockDetails{})
	assert.NoError(t, err)

}

func TestLockShared(t *testing.T) {
	collection := setup(t)
	defer teardown(t, collection)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	client := lock.NewClient(collection)

	// Create some locks.
	err := client.SLock(ctx, "resource1", "aaaa", lock.LockDetails{}, 10)
	assert.NoError(t, err)
	err = client.SLock(ctx, "resource1", "bbbb", lock.LockDetails{}, 10)
	assert.NoError(t, err)
	err = client.SLock(ctx, "resource2", "bbbb", lock.LockDetails{}, 10)
	assert.NoError(t, err)

	// Try to create a shared lock that already exists.
	err = client.SLock(ctx, "resource1", "aaaa", lock.LockDetails{}, 10)
	assert.Equal(t, lock.ErrAlreadyLocked, err)
	err = client.SLock(ctx, "resource2", "bbbb", lock.LockDetails{}, 10)
	assert.Equal(t, lock.ErrAlreadyLocked, err)
}

func TestLockMaxConcurrent(t *testing.T) {
	collection := setup(t)
	defer teardown(t, collection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	client := lock.NewClient(collection)

	// Create some locks.
	err := client.SLock(ctx, "resource1", "aaaa", lock.LockDetails{}, 2)
	assert.NoError(t, err)
	err = client.SLock(ctx, "resource1", "bbbb", lock.LockDetails{}, 2)
	assert.NoError(t, err)

	// Try to create a third lock, which will be more than maxConcurrent.
	err = client.SLock(ctx, "resource1", "cccc", lock.LockDetails{}, 2)
	assert.Equal(t, lock.ErrAlreadyLocked, err)
}

func TestLockInteractions(t *testing.T) {
	collection := setup(t)
	defer teardown(t, collection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	client := lock.NewClient(collection)

	// Trying to create a shared lock on a resource that already has an
	// exclusive lock in it should return an error.
	err := client.XLock(ctx, "resource1", "aaaa", lock.LockDetails{})
	assert.NoError(t, err)
	err = client.SLock(ctx, "resource1", "bbbb", lock.LockDetails{}, -1)
	assert.Equal(t, lock.ErrAlreadyLocked, err)

	// Trying to create an exclusive lock on a resource that already has a
	// shared lock in it should return an error.
	err = client.SLock(ctx, "resource2", "aaaa", lock.LockDetails{}, -1)
	assert.NoError(t, err)
	err = client.XLock(ctx, "resource2", "bbbb", lock.LockDetails{})
	assert.Equal(t, lock.ErrAlreadyLocked, err)
}

func TestUnlock(t *testing.T) {
	collection := setup(t)
	defer teardown(t, collection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	client := lock.NewClient(collection)

	// Unlock an exclusive lock.
	err := client.XLock(ctx, "resource1", "aaaa", lock.LockDetails{})
	assert.NoError(t, err)
	unlocked, err := client.Unlock(ctx, "aaaa")
	assert.NoError(t, err)
	assert.Len(t, unlocked, 1, "expected to unlock exactly 1 resource")
	assert.Equal(t, "resource1", unlocked[0].Resource, "unlocked the wrong resource")

	// Unlock a shared lock.
	err = client.SLock(ctx, "resource2", "bbbb", lock.LockDetails{}, -1)
	assert.NoError(t, err)
	unlocked, err = client.Unlock(ctx, "bbbb")
	assert.NoError(t, err)
	assert.Len(t, unlocked, 1, "expected to unlock exactly 1 resource")
	assert.Equal(t, "resource2", unlocked[0].Resource, "unlocked the wrong resource")

	// Try to unlock a lockId that doesn't exist.
	unlocked, err = client.Unlock(ctx, "zzzz")
	assert.NoError(t, err)
	assert.Len(t, unlocked, 0, "expected to not unlock any resources")
}

func TestUnlockOrder(t *testing.T) {
	collection := setup(t)
	defer teardown(t, collection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	client := lock.NewClient(collection)

	// Create some locks.
	err := client.XLock(ctx, "resource1", "aaaa", lock.LockDetails{})
	assert.NoError(t, err)
	err = client.SLock(ctx, "resource4", "aaaa", lock.LockDetails{}, -1)
	assert.NoError(t, err)
	err = client.XLock(ctx, "resource3", "bbbb", lock.LockDetails{})
	assert.NoError(t, err)
	err = client.SLock(ctx, "resource2", "bbbb", lock.LockDetails{}, -1)
	assert.NoError(t, err)
	err = client.SLock(ctx, "resource2", "aaaa", lock.LockDetails{}, -1)
	assert.NoError(t, err)

	// Make sure they are unlocked in the order of newest to oldest.
	unlocked, err := client.Unlock(ctx, "aaaa")
	assert.NoError(t, err)

	actual := []string{}
	for _, l := range unlocked {
		actual = append(actual, l.Resource)
	}

	assert.Equal(t, []string{"resource2", "resource4", "resource1"}, actual, "unlocked resources in the wrong order")
}

func TestStatusFilterTTLgte(t *testing.T) {
	collection := setup(t)
	defer teardown(t, collection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	client := lock.NewClient(collection)

	_, err := initLockStatusLocks(client)
	assert.NoError(t, err)

	///////////////////////////////////////////////////////////////////////
	// Filter on TTL greater than.
	///////////////////////////////////////////////////////////////////////
	f := lock.Filter{
		TTLgte: 3700,
	}
	actual, err := client.Status(ctx, f)
	assert.NoError(t, err)

	// These must be in the order of LockStatusesByCreatedAtDesc.
	expected := []lock.LockStatus{
		{
			Resource: "resource4",
			LockId:   "cccc",
			Type:     lock.LOCK_TYPE_SHARED,
		},
		{
			Resource: "resource2",
			LockId:   "cccc",
			Type:     lock.LOCK_TYPE_SHARED,
		},
	}

	validateLockStatuses(t, actual, expected)
	assert.NoError(t, err)
}

func TestStatusFilterTTLlt(t *testing.T) {
	collection := setup(t)
	defer teardown(t, collection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	client := lock.NewClient(collection)

	_, err := initLockStatusLocks(client)
	assert.NoError(t, err)

	///////////////////////////////////////////////////////////////////////
	// Filter on TTL less than. Shouldn't include locks with no TTL.
	///////////////////////////////////////////////////////////////////////
	f := lock.Filter{
		TTLlt: 600,
	}
	actual, err := client.Status(ctx, f)
	assert.NoError(t, err)

	expected := []lock.LockStatus{}

	validateLockStatuses(t, actual, expected)
	assert.NoError(t, err)
}

func TestStatusFilterCreatedAfter(t *testing.T) {
	collection := setup(t)
	defer teardown(t, collection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	client := lock.NewClient(collection)

	recordedTime, err := initLockStatusLocks(client)
	assert.NoError(t, err)

	///////////////////////////////////////////////////////////////////////
	// Filter on CreatedAfter.
	///////////////////////////////////////////////////////////////////////
	f := lock.Filter{
		CreatedAfter: recordedTime,
	}
	actual, err := client.Status(ctx, f)
	assert.NoError(t, err)

	// These must be in the order of LockStatusesByCreatedAtDesc.
	expected := []lock.LockStatus{
		{
			Resource: "resource4",
			LockId:   "cccc",
			Type:     lock.LOCK_TYPE_SHARED,
		},
		{
			Resource: "resource3",
			LockId:   "bbbb",
			Type:     lock.LOCK_TYPE_EXCLUSIVE,
			Owner:    "smith",
		},
		{
			Resource: "resource2",
			LockId:   "cccc",
			Type:     lock.LOCK_TYPE_SHARED,
		},
	}

	validateLockStatuses(t, actual, expected)
	assert.NoError(t, err)
}

func TestStatusFilterCreatedBefore(t *testing.T) {
	collection := setup(t)
	defer teardown(t, collection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	client := lock.NewClient(collection)

	recordedTime, err := initLockStatusLocks(client)
	assert.NoError(t, err)

	///////////////////////////////////////////////////////////////////////
	// Filter on CreatedBefore.
	///////////////////////////////////////////////////////////////////////
	f := lock.Filter{
		CreatedBefore: recordedTime,
	}
	actual, err := client.Status(ctx, f)
	assert.NoError(t, err)

	// These must be in the order of LockStatusesByCreatedAtDesc.
	expected := []lock.LockStatus{
		{
			Resource: "resource2",
			LockId:   "bbbb",
			Type:     lock.LOCK_TYPE_SHARED,
			Owner:    "smith",
		},
		{
			Resource: "resource2",
			LockId:   "aaaa",
			Type:     lock.LOCK_TYPE_SHARED,
			Owner:    "john",
			Host:     "host.name",
		},
		{
			Resource: "resource1",
			LockId:   "aaaa",
			Type:     lock.LOCK_TYPE_EXCLUSIVE,
			Owner:    "john",
			Host:     "host.name",
		},
	}

	validateLockStatuses(t, actual, expected)
	assert.NoError(t, err)
}

func TestStatusFilterOwner(t *testing.T) {
	collection := setup(t)
	defer teardown(t, collection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	client := lock.NewClient(collection)

	_, err := initLockStatusLocks(client)
	assert.NoError(t, err)

	///////////////////////////////////////////////////////////////////////
	// Filter on Owner.
	///////////////////////////////////////////////////////////////////////
	f := lock.Filter{
		Owner: "smith",
	}
	actual, err := client.Status(ctx, f)
	assert.NoError(t, err)

	// These must be in the order of LockStatusesByCreatedAtDesc.
	expected := []lock.LockStatus{
		{
			Resource: "resource3",
			LockId:   "bbbb",
			Type:     lock.LOCK_TYPE_EXCLUSIVE,
			Owner:    "smith",
		},
		{
			Resource: "resource2",
			LockId:   "bbbb",
			Type:     lock.LOCK_TYPE_SHARED,
			Owner:    "smith",
		},
	}

	validateLockStatuses(t, actual, expected)
	assert.NoError(t, err)
}

func TestStatusFilterMultiple(t *testing.T) {
	collection := setup(t)
	defer teardown(t, collection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	client := lock.NewClient(collection)

	_, err := initLockStatusLocks(client)
	assert.NoError(t, err)

	///////////////////////////////////////////////////////////////////////
	// Filter on TTL, Resource, and LockId.
	///////////////////////////////////////////////////////////////////////
	f := lock.Filter{
		TTLlt:    5000,
		Resource: "resource1",
		LockId:   "aaaa",
	}
	actual, err := client.Status(ctx, f)
	assert.NoError(t, err)

	// These must be in the order of LockStatusesByCreatedAtDesc.
	expected := []lock.LockStatus{
		{
			Resource: "resource1",
			LockId:   "aaaa",
			Type:     lock.LOCK_TYPE_EXCLUSIVE,
			Owner:    "john",
			Host:     "host.name",
		},
	}

	validateLockStatuses(t, actual, expected)
	assert.NoError(t, err)
}

func TestStatusTTLValue(t *testing.T) {
	collection := setup(t)
	defer teardown(t, collection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	client := lock.NewClient(collection)

	// Create a lock with a TTL.
	err := client.XLock(ctx, "resource1", "aaaa", lock.LockDetails{TTL: 3600})
	assert.NoError(t, err)
	// Create a lock without a TTL.
	err = client.XLock(ctx, "resource2", "bbbb", lock.LockDetails{})
	assert.NoError(t, err)
	// Create a lock with a low TTL.
	err = client.XLock(ctx, "resource3", "cccc", lock.LockDetails{TTL: 1})
	assert.NoError(t, err)

	// Make sure we get back a similar TTL when querying the status of the
	// lock with a TTL.
	f := lock.Filter{
		LockId: "aaaa",
	}
	actual, err := client.Status(ctx, f)
	assert.NoError(t, err)
	assert.Len(t, actual, 1, "expected to get the status of exactly 1 lock")
	assert.True(t, actual[0].TTL > 3575 && actual[0].TTL <= 3600, "ttl = %d, expected it to be between 3575 and 3600", actual[0].TTL)

	// Make sure we get back -1 for the TTL for the lock without one.
	f = lock.Filter{
		LockId: "bbbb",
	}
	actual, err = client.Status(ctx, f)
	assert.NoError(t, err)
	assert.Len(t, actual, 1, "expected to get the status of exactly 1 lock")
	assert.Equal(t, -1, int(actual[0].TTL), "ttl = %d, expected %d", actual[0].TTL, -1)

	// Sleep for 2 seconds to ensure that the lock on resource3 expired at
	// least 2 seconds ago.
	time.Sleep(time.Duration(2100) * time.Millisecond)

	// Make sure we get back 0 for the TTL of the expired lock.
	f = lock.Filter{
		LockId: "cccc",
	}
	actual, err = client.Status(ctx, f)
	assert.NoError(t, err)
	assert.Len(t, actual, 1, "expected to get the status of exactly 1 lock")
	assert.Equal(t, 0, int(actual[0].TTL), "ttl = %d, expected %d", actual[0].TTL, 0)
}

func TestRenew(t *testing.T) {
	collection := setup(t)
	defer teardown(t, collection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	client := lock.NewClient(collection)

	// Create a lock that we are not renewing on a resource that will get another
	// lock that we will attempt to renew. If the renew operation is done wrong
	// this lock will be renewed instead of the proper one.
	err := client.SLock(ctx, "resource4", "cccc", lock.LockDetails{TTL: 3600}, -1)
	assert.NoError(t, err)

	// Create some locks.
	err = client.XLock(ctx, "resource1", "aaaa", lock.LockDetails{TTL: 3600})
	assert.NoError(t, err)
	err = client.SLock(ctx, "resource4", "aaaa", lock.LockDetails{TTL: 3600}, -1)
	assert.NoError(t, err)
	err = client.XLock(ctx, "resource3", "bbbb", lock.LockDetails{})
	assert.NoError(t, err)
	err = client.SLock(ctx, "resource2", "bbbb", lock.LockDetails{}, -1)
	assert.NoError(t, err)
	err = client.SLock(ctx, "resource2", "aaaa", lock.LockDetails{TTL: 3600}, -1)
	assert.NoError(t, err)

	// Verify that locks with the given lockId have their TTL updated.
	renewed, err := client.Renew(ctx, "aaaa", 7200)
	assert.NoError(t, err)
	assert.Len(t, renewed, 3, "expected to renew exactly 3 locks")

	f := lock.Filter{
		LockId: "aaaa",
	}
	actual, err := client.Status(ctx, f)
	assert.NoError(t, err)
	assert.Len(t, actual, 3, "expected to get the status of 3 locks with lockId=aaaa")
	for _, a := range actual {
		assert.True(t, a.TTL > 7175 && a.TTL <= 7200, "ttl = %d for resource=%s lockId=%s, expected it to be between 7175 and 7200")
	}
}

func TestRenewLockIdNotFound(t *testing.T) {
	collection := setup(t)
	defer teardown(t, collection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	client := lock.NewClient(collection)

	// Create a lock.
	err := client.XLock(ctx, "resource1", "aaaa", lock.LockDetails{TTL: 3600})
	assert.NoError(t, err)

	renewed, err := client.Renew(ctx, "bbbb", 7200)
	assert.Equal(t, lock.ErrLockNotFound, err)
	assert.Len(t, renewed, 0, "expected to not renew any locks")
}

func TestRenewTTLExpired(t *testing.T) {
	collection := setup(t)
	defer teardown(t, collection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	client := lock.NewClient(collection)

	// Create some locks.
	err := client.XLock(ctx, "resource1", "aaaa", lock.LockDetails{TTL: 3600})
	assert.NoError(t, err)
	err = client.SLock(ctx, "resource4", "aaaa", lock.LockDetails{TTL: 1}, -1)
	assert.NoError(t, err)

	// Sleep for a short time so that we know the TTL of the second lock
	// will be < 1.
	time.Sleep(time.Duration(100) * time.Millisecond)

	// Make sure the renew fails due to the TTL being expired on one of
	// the locks.
	renewed, err := client.Renew(ctx, "aaaa", 7200)
	assert.Equal(t, lock.ErrLockNotFound, err)
	assert.Len(t, renewed, 1, "expected to renew exactly 1 lock since the other lock with the same lockId should be expired")
}

// ------------------------------------------------------------------------- //

// initLockStatusLocks initializes locks that are used for the LockStatus tests.
// It returns a time.Time that can be used in tests to filter on CreatedAt.
func initLockStatusLocks(client *lock.Client) (time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	// Create a bunch of different locks.
	aaaaDetails := lock.LockDetails{
		Owner: "john",
		Host:  "host.name",
		TTL:   3600,
	}
	bbbbDetails := lock.LockDetails{
		Owner: "smith",
	}
	ccccDetails := lock.LockDetails{
		TTL: 7200,
	}
	err := client.XLock(ctx, "resource1", "aaaa", aaaaDetails)
	if err != nil {
		return time.Time{}, err
	}
	err = client.SLock(ctx, "resource2", "aaaa", aaaaDetails, -1)
	if err != nil {
		return time.Time{}, err
	}
	err = client.SLock(ctx, "resource2", "bbbb", bbbbDetails, -1)
	if err != nil {
		return time.Time{}, err
	}

	// Capture a timestamp after the locks that have already been created,
	// and before the additional locks we are about to create. This is used
	// by tests that filter on CreatedAt.
	time.Sleep(time.Duration(1) * time.Millisecond)
	recordedTime := time.Now()
	time.Sleep(time.Duration(1) * time.Millisecond)

	err = client.SLock(ctx, "resource2", "cccc", ccccDetails, -1)
	if err != nil {
		return time.Time{}, err
	}
	err = client.XLock(ctx, "resource3", "bbbb", bbbbDetails)
	if err != nil {
		return time.Time{}, err
	}
	err = client.SLock(ctx, "resource4", "cccc", ccccDetails, -1)
	if err != nil {
		return time.Time{}, err
	}

	return recordedTime, nil
}

// validateLockStatuses compares two slices of LockStatuses, returning an error
// if they are not the same. It zeros out some of the fields on the structs in
// the "actual" argument to make comparisons easier (and still accurate for the
// most part).
func validateLockStatuses(t *testing.T, actual, expected []lock.LockStatus) {
	t.Helper()
	// Sort actual to make checks deterministic. expected should already
	// be in the LockStatusesByCreatedAtDesc order, but we still need to
	// convert it to the correct type.
	var actualSorted lock.LockStatusesByCreatedAtDesc
	var expectedSorted lock.LockStatusesByCreatedAtDesc
	actualSorted = actual
	expectedSorted = expected
	sort.Sort(actualSorted)

	for i := range actualSorted {
		assert.Equal(t, expectedSorted[i].Resource, actualSorted[i].Resource, "lock %d: resource does not match", i)
		assert.Equal(t, expectedSorted[i].LockId, actualSorted[i].LockId, "lock %d: lockId does not match", i)
		assert.Equal(t, expectedSorted[i].Type, actualSorted[i].Type, "lock %d: type does not match", i)
		assert.Equal(t, expectedSorted[i].Owner, actualSorted[i].Owner, "lock %d: owner does not match", i)
		assert.Equal(t, expectedSorted[i].Host, actualSorted[i].Host, "lock %d: host does not match", i)
		assert.Equal(t, expectedSorted[i].Comment, actualSorted[i].Comment, "lock %d: comment does not match", i)
	}
}
