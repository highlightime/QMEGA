package pebble_modified

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	dbFile1 := "test_db1"
	dbFile2 := "test_db2"
	cacheSize := 64
	fileHandles := 32
	namespace := "testdb"
	readonly := false
	ephemeral := false

	// Clean up any existing test databases
	defer os.RemoveAll(dbFile1)
	defer os.RemoveAll(dbFile2)

	// Create new databases
	db, err := New(dbFile1, dbFile2, cacheSize, fileHandles, namespace, readonly, ephemeral)
	if err != nil {
		t.Fatalf("Failed to open databases: %v", err)
	}
	defer db.Close()
}

func TestHas(t *testing.T) {
	dbFile1 := "test_db1"
	dbFile2 := "test_db2"
	cacheSize := 64
	fileHandles := 32
	namespace := "testdb"
	readonly := false
	ephemeral := false

	// Clean up any existing test databases
	defer os.RemoveAll(dbFile1)
	defer os.RemoveAll(dbFile2)

	// Create new databases
	db, err := New(dbFile1, dbFile2, cacheSize, fileHandles, namespace, readonly, ephemeral)
	assert.NoError(t, err, "Failed to open databases")
	defer db.Close()

	// Test Has with an empty database
	hasKey, err := db.Has([]byte("key"))
	assert.NoError(t, err, "Failed to check key in database 1")
	assert.False(t, hasKey, "Expected key 'key' to be absent in database 1")

	// Test Put with an empty database
	err = db.Put([]byte("key"), []byte("value"))
	assert.NoError(t, err, "Failed to put key in database")

	// Test Get with a database having the key
	getValue, err := db.Get([]byte("key"))
	assert.NoError(t, err, "Failed to get key in database")
	assert.NotNil(t, getValue, "Expected to get a value for 'key'")
	t.Logf("Got value: %v", getValue)

	// Test Delete with a database having the key
	err = db.Delete([]byte("key"))
	assert.NoError(t, err, "Failed to delete key in database")

	// Test Get with an empty database
	getValue, err = db.Get([]byte("key"))
	assert.Error(t, err, "Expected an error when getting deleted key")

	// Test NewBatch
	b := db.NewBatch()
	assert.NotNil(t, b, "Failed to create batch")

	err = b.Put([]byte("key"), []byte("value"))
	assert.NoError(t, err, "Failed to put key in batch")

	err = b.Write()
	assert.NoError(t, err, "Failed to write batch")

	// Test Snapshot with a database having the key
	s, err := db.NewSnapshot()
	assert.NoError(t, err, "Failed to create snapshot")

	hasKey, err = s.Has([]byte("key"))
	assert.NoError(t, err, "Failed to check key in snapshot")
	assert.True(t, hasKey, "Expected key 'key' to be present in snapshot")

	getValue, err = s.Get([]byte("key"))
	assert.NoError(t, err, "Failed to get key in snapshot")
	assert.NotNil(t, getValue, "Expected to get a value for 'key' in snapshot")
	s.Release()

	// Test NewBatchWithSize
	b = db.NewBatchWithSize(100)
	assert.NotNil(t, b, "Failed to create batch with size")

	err = b.Delete([]byte("key"))
	assert.NoError(t, err, "Failed to delete key in batch")

	b.Reset()
	b.Replay(db)

	// Test NewIterator
	iter := db.NewIterator([]byte("key"), []byte("key"))
	assert.NotNil(t, iter, "Failed to create iterator")

	k := iter.Key()
	assert.Nil(t, k, "Failed to get key from iterator")

	v := iter.Value()
	assert.Nil(t, v, "Failed to get value from iterator")

	e := iter.Error()
	assert.Nil(t, e, "Failed to get error from iterator")

	// Ensure iterator reaches the end
	assert.False(t, iter.Next(), "Expected iterator to be exhausted")

	assert.NoError(t,db.Compact([]byte("key"), []byte("key1")),"Failed to compact")
	
	iter.Release()
	db.Close()
}
