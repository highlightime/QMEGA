package pebble

import (
	"os"
	"testing"
	"fmt"
	"github.com/stretchr/testify/assert"
	"crypto/rand"
	"sync"
)
// generateRandomBytes creates a random byte slice of a given length
func generateRandomBytes(length int) []byte {
	b := make([]byte, length)
	_, err := rand.Read(b)
	if err != nil {
		panic(fmt.Sprintf("Failed to generate random bytes: %v", err))
	}
	return b
}
func TestNew(t *testing.T) {
	ssdThreshold := 50
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
	ssdThreshold :=50
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
	err = db.Put([]byte("key1"), []byte("cold1"))
	assert.NoError(t, err, "Failed to put key in database")
	err = db.Put([]byte("key4"), []byte("cold4"))
	assert.NoError(t, err, "Failed to put key in database")
	err = db.Put([]byte("key6"), []byte("cold6"))
	assert.NoError(t, err, "Failed to put key in database")

	// Test Get with a database having the key
	fmt.Println("# Get key1")
	getValue, err := db.Get([]byte("key1"))
	assert.NoError(t, err, "Failed to get key in database")
	assert.NotNil(t, getValue, "Expected to get a value for 'key'")
	assert.Equal(t, []byte("cold1"), getValue, "Expected value 'cold1' for 'key1' in snapshot")

	// Test NewBatch
	fmt.Println("# Batch Put key1, key10, key99, key2, key3, key4 in hot db")
	b := db.NewBatch()
	assert.NotNil(t, b, "Failed to create batch")

	err = b.Put([]byte("key1"), []byte("hot1"))
	assert.NoError(t, err, "Failed to put key in batch")

	err = b.Put([]byte("key10"), []byte("hot10"))
	assert.NoError(t, err, "Failed to put key in batch")

	err = b.Put([]byte("key99"), []byte("hot99"))
	assert.NoError(t, err, "Failed to put key in batch")

	err = b.Put([]byte("key2"), []byte("hot2"))
	assert.NoError(t, err, "Failed to put key in batch")

	err = b.Put([]byte("key3"), []byte("hot3"))
	assert.NoError(t, err, "Failed to put key in batch")

	err = b.Put([]byte("key4"), []byte("hot4"))
	assert.NoError(t, err, "Failed to put key in batch")

	err = b.Write()
	assert.NoError(t, err, "Failed to write batch")

	
	numRecords := 50 // Adjust this number as needed to increase size
	keySize := 32         // Size of each key in bytes
	valueSize := 1000000     // Size of each value in bytes
	batch := db.NewBatch()
	// Test batch
	for i := 0; i < numRecords; i++ {
		// Generate a random key and value
		key := generateRandomBytes(keySize)
		value := generateRandomBytes(valueSize)

		// Insert the key-value pair into the batch
		err = batch.Put(key, value)
		if err != nil {
			t.Fatalf("Failed to put key in database: %v", err)
		}
	}
	batch.Write()

	var wg sync.WaitGroup
	numGoroutines := 10

	for j := 0; j < numGoroutines; j++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Create a new batch for each goroutine
			b2 := db.NewBatch()
			for i := 0; i < numRecords; i++ {
				// Generate a random key and value
				key := generateRandomBytes(keySize)
				value := generateRandomBytes(valueSize)

				// Insert the key-value pair into the batch
				err := b2.Put(key, value)
				if err != nil {
					t.Fatalf("Worker %d: Failed to put key in database: %v", workerID, err)
				}
			}
			// Write the batch
			err := b2.Write()
			assert.NoError(t, err, "Worker %d: Failed to write batch", workerID)
		}(j)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	

	// Test NewIterator
	fmt.Println("# Iterator prefix key")
	iter := db.NewIterator([]byte("key"), []byte(""))
	assert.NotNil(t, iter, "Failed to create iterator")

	// Test Next of Iterator
	i := 1
	answerKey:=[]string{"key1","key10","key2","key3","key4","key6","key99"}
	answerValue:=[]string{"hot1","hot10","hot2","hot3","hot4","cold6","hot99"}
	for iter.Next() {
		k := iter.Key()
		v := iter.Value()
		fmt.Println("i: ", i)
		fmt.Printf("Got key: %s, value: %s\n", k, v)
		assert.Equal(t, []byte(answerKey[i-1]), k, "Expected key to be present in database")
		assert.Equal(t, []byte(answerValue[i-1]), v, "Expected value for 'key' in database")
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
	assert.Equal(t, []byte("hot1"), getValue, "Expected value 'hot1' for 'key1' in snapshot")
	s.Release()

	// Test NewBatchWithSize
	b = db.NewBatchWithSize(100)
	assert.NotNil(t, b, "Failed to create batch with size")

	err = b.Delete([]byte("key1"))
	assert.NoError(t, err, "Failed to delete key in batch")

	b.Reset()
	b.Replay(db)

	for i := 0; i < numRecords; i++ {
		// Generate a random key and value
		key := generateRandomBytes(keySize)
		value := generateRandomBytes(valueSize)

		// Insert the key-value pair into the batch
		err = db.Put(key, value)
		if err != nil {
			t.Fatalf("Failed to put key in database: %v", err)
		}
	}

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
	err = db.Put([]byte("key1"), []byte("value1"))
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
