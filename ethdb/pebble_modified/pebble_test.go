package pebble_modified

import (
	"os"
	"testing"
	"fmt"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	ssdThreshold := 0
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
	db, err := New(ssdThreshold,dbFile1, dbFile2, cacheSize, fileHandles, namespace, readonly, ephemeral)
	if err != nil {
		t.Fatalf("Failed to open databases: %v", err)
	}
	defer db.Close()
}

func TestOverThreshold(t *testing.T) {
	fmt.Println("==============TestOverThreshold==============")
	ssdThreshold :=0
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
	db, err := New(ssdThreshold, dbFile1, dbFile2, cacheSize, fileHandles, namespace, readonly, ephemeral)
	assert.NoError(t, err, "Failed to open databases")
	defer db.Close()

	// Test Has with an empty database
	hasKey, err := db.Has([]byte("key"))
	assert.NoError(t, err, "Failed to check key in database")
	assert.False(t, hasKey, "Expected key 'key' to be absent in database")

	// Test Put with an empty database
	fmt.Println("# Put key1, key4, key6 in cold db")
	err = db.PutForTest([]byte("key1"), []byte("value1"))
	assert.NoError(t, err, "Failed to put key in database")
	err = db.PutForTest([]byte("key4"), []byte("value4"))
	assert.NoError(t, err, "Failed to put key in database")
	err = db.PutForTest([]byte("key6"), []byte("value6"))
	assert.NoError(t, err, "Failed to put key in database")

	// Test Get with a database having the key
	fmt.Println("# Get key1")
	getValue, err := db.Get([]byte("key1"))
	assert.NoError(t, err, "Failed to get key in database")
	assert.NotNil(t, getValue, "Expected to get a value for 'key'")
	assert.Equal(t, []byte("value1"), getValue, "Expected value 'value1' for 'key1' in snapshot")

	// Test NewBatch
	fmt.Println("# Batch Put key1, key10, key99, key2, key3, key4 in hot db")
	b := db.NewBatch()
	assert.NotNil(t, b, "Failed to create batch")

	err = b.Put([]byte("key1"), []byte("value1"))
	assert.NoError(t, err, "Failed to put key in batch")

	err = b.Put([]byte("key10"), []byte("value10"))
	assert.NoError(t, err, "Failed to put key in batch")

	err = b.Put([]byte("key99"), []byte("value99"))
	assert.NoError(t, err, "Failed to put key in batch")

	err = b.Put([]byte("key2"), []byte("value2"))
	assert.NoError(t, err, "Failed to put key in batch")

	err = b.Put([]byte("key3"), []byte("value3"))
	assert.NoError(t, err, "Failed to put key in batch")

	err = b.Put([]byte("key4"), []byte("value4"))
	assert.NoError(t, err, "Failed to put key in batch")

	err = b.Write()
	assert.NoError(t, err, "Failed to write batch")

	// Test NewIterator
	fmt.Println("# Iterator prefix key")
	iter := db.NewIterator([]byte("key"), []byte(""))
	assert.NotNil(t, iter, "Failed to create iterator")

	// Test Next of Iterator
	i := 1
	answer:=[]string{"key1","key10","key2","key3","key4","key6","key99"}
	for iter.Next() {
		k := iter.Key()
		v := iter.Value()
		fmt.Println("i: ", i)
		fmt.Printf("Got key: %s, value: %s\n", k, v)
		assert.Equal(t, []byte(answer[i-1]), k, "Expected key 'key' to be present in database")
		i++
	}

	if err := iter.Error(); err != nil {
		t.Errorf("test iteration failed: %v", err)
	}

	assert.NoError(t,db.Compact([]byte("key1"), []byte("key5")),"Failed to compact")
	iter.Release()

	// Test Snapshot with a database having the key
	s, err := db.NewSnapshot()
	assert.NoError(t, err, "Failed to create snapshot")

	fmt.Println("# Snapshot Has key1")
	hasKey, err = s.Has([]byte("key1"))
	assert.NoError(t, err, "Failed to check key in snapshot")
	assert.True(t, hasKey, "Expected key 'key' to be present in snapshot")

	fmt.Println("# Snapshot Get key1")
	getValue, err = s.Get([]byte("key1"))
	assert.NoError(t, err, "Failed to get key in snapshot")
	assert.NotNil(t, getValue, "Expected to get a value for 'key' in snapshot")
	assert.Equal(t, []byte("value1"), getValue, "Expected value 'value1' for 'key1' in snapshot")
	s.Release()

	// Test NewBatchWithSize
	b = db.NewBatchWithSize(100)
	assert.NotNil(t, b, "Failed to create batch with size")

	err = b.Delete([]byte("key1"))
	assert.NoError(t, err, "Failed to delete key in batch")

	b.Reset()
	b.Replay(db)

	
	db.Close()
}

func TestUnderThreshold(t *testing.T) {
	fmt.Println("==============TestUnderThreshold==============")
	ssdThreshold :=100
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
	db, err := New(ssdThreshold, dbFile1, dbFile2, cacheSize, fileHandles, namespace, readonly, ephemeral)
	assert.NoError(t, err, "Failed to open databases")
	defer db.Close()

	// Test Delete with an empty database
	fmt.Println("# Delete key1")
	err = db.Delete([]byte("key1"))
	assert.Nil(t, err, "Not failed to delete key in database")

	// Test Has with an empty database
	fmt.Println("# Has key1")
	hasKey, err := db.Has([]byte("key1"))
	assert.NoError(t, err, "Failed to check key in database")
	assert.False(t, hasKey, "Expected key 'key' to be absent in database")

	// Test Put with an empty database
	fmt.Println("# Put key1")
	err = db.PutForTest([]byte("key1"), []byte("value1"))
	assert.NoError(t, err, "Failed to put key in database")

	// Test Get with a database having the key
	fmt.Println("# Get key1")
	getValue, err := db.Get([]byte("key1"))
	assert.NoError(t, err, "Failed to get key in database")
	assert.NotNil(t, getValue, "Expected to get a value for 'key'")
	assert.Equal(t, []byte("value1"), getValue, "Expected value 'value1' for 'key1' in snapshot")
	// t.Logf("Got value: %v", getValue)

	// Test Delete with a database having the key
	fmt.Println("# Delete key1")
	err = db.Delete([]byte("key1"))
	assert.NoError(t, err, "Failed to delete key in database")

	// Test Get with a database after deleting the key
	fmt.Println("# Get key1")
	_, err = db.Get([]byte("key1"))
	assert.Error(t, err, "Expected to get an error for 'key'")

	// Test NewBatch
	fmt.Println("# Batch Put key1, key2, key99, key3, key9999")
	b := db.NewBatch()
	assert.NotNil(t, b, "Failed to create batch")

	err = b.Put([]byte("key1"), []byte("value1"))
	assert.NoError(t, err, "Failed to put key in batch")

	err = b.Put([]byte("key2"), []byte("value2"))
	assert.NoError(t, err, "Failed to put key in batch")

	err = b.Put([]byte("key99"), []byte("value99"))
	assert.NoError(t, err, "Failed to put key in batch")

	err = b.Put([]byte("key3"), []byte("value3"))
	assert.NoError(t, err, "Failed to put key in batch")

	err = b.Put([]byte("key9999"), []byte("value9999"))
	assert.NoError(t, err, "Failed to put key in batch")

	err = b.Write()
	assert.NoError(t, err, "Failed to write batch")

	// Test NewIterator
	fmt.Println("# Iterator prefix key")
	iter := db.NewIterator([]byte("key"), []byte(""))
	assert.NotNil(t, iter, "Failed to create iterator")

	i := 1
	answer:=[]string{"key1","key2","key3","key99", "key9999"}
	for iter.Next() {
		k := iter.Key()
		v := iter.Value()
		fmt.Println("i: ", i)
		fmt.Printf("Got key: %s, value: %s\n", k, v)
		assert.Equal(t, []byte(answer[i-1]), k, "Expected key 'key' to be present in database")
		i++
	}

	if err := iter.Error(); err != nil {
		t.Errorf("test iteration failed: %v", err)
	}
	assert.NoError(t,db.Compact([]byte("key1"), []byte("key5")),"Failed to compact")
	iter.Release()

	// Test Snapshot with a database having the key
	s, err := db.NewSnapshot()
	assert.NoError(t, err, "Failed to create snapshot")

	fmt.Println("# Snapshot Has key1")
	hasKey, err = s.Has([]byte("key1"))
	assert.NoError(t, err, "Failed to check key in snapshot")
	assert.True(t, hasKey, "Expected key 'key' to be present in snapshot")

	fmt.Println("# Snapshot Get key1")
	getValue, err = s.Get([]byte("key1"))
	assert.NoError(t, err, "Failed to get key in snapshot")
	assert.NotNil(t, getValue, "Expected to get a value for 'key' in snapshot")
	assert.Equal(t, []byte("value1"), getValue, "Expected value 'value1' for 'key1' in snapshot")
	s.Release()

	// Test NewBatchWithSize
	b = db.NewBatchWithSize(100)
	assert.NotNil(t, b, "Failed to create batch with size")

	err = b.Delete([]byte("key1"))
	assert.NoError(t, err, "Failed to delete key in batch")

	b.Reset()
	b.Replay(db)

	
	db.Close()
}
