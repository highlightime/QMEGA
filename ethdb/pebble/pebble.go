// Copyright 2023 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package pebble implements the key-value database layer based on pebble.
package pebble

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
)

const (
	// minCache is the minimum amount of memory in megabytes to allocate to pebble
	// read and write caching, split half and half.
	minCache = 16

	// minHandles is the minimum number of files handles to allocate to the open
	// database files.
	minHandles = 16

	// metricsGatheringInterval specifies the interval to retrieve pebble database
	// compaction, io and pause stats to report to the user.
	metricsGatheringInterval = 3 * time.Second

	// degradationWarnInterval specifies how often warning should be printed if the
	// leveldb database cannot keep up with requested writes.
	degradationWarnInterval = time.Minute
)

var (
	prefixSSD = []byte{'s'}
	prefixHDD = []byte{'h'}
)

// Database is a persistent key-value store based on the pebble storage engine.
// Apart from basic data storage functionality it also supports batch writes and
// iterating over the keyspace in binary-alphabetical order.
type Database struct {
	// fn string     // filename for reporting
	// db *pebble.DB // Underlying pebble storage engine
	hotFn  string
	coldFn string
	hotDb  *pebble.DB
	coldDb *pebble.DB

	compTimeMeter       metrics.Meter // Meter for measuring the total time spent in database compaction
	compReadMeter       metrics.Meter // Meter for measuring the data read during compaction
	compWriteMeter      metrics.Meter // Meter for measuring the data written during compaction
	writeDelayNMeter    metrics.Meter // Meter for measuring the write delay number due to database compaction
	writeDelayMeter     metrics.Meter // Meter for measuring the write delay duration due to database compaction
	diskSizeGauge       metrics.Gauge // Gauge for tracking the size of all the levels in the database
	diskReadMeter       metrics.Meter // Meter for measuring the effective amount of data read
	diskWriteMeter      metrics.Meter // Meter for measuring the effective amount of data written
	memCompGauge        metrics.Gauge // Gauge for tracking the number of memory compaction
	level0CompGauge     metrics.Gauge // Gauge for tracking the number of table compaction in level0
	nonlevel0CompGauge  metrics.Gauge // Gauge for tracking the number of table compaction in non0 level
	seekCompGauge       metrics.Gauge // Gauge for tracking the number of table compaction caused by read opt
	manualMemAllocGauge metrics.Gauge // Gauge for tracking amount of non-managed memory currently allocated

	levelsGauge []metrics.Gauge // Gauge for tracking the number of tables in levels

	quitLock     sync.RWMutex // Mutex protecting the quit channel and the closed flag
	quitColdLock sync.RWMutex
	quitChan     chan chan error // Quit channel to stop the metrics collection before closing the database
	closed       bool            // keep track of whether we're Closed

	log log.Logger // Contextual logger tracking the database path

	activeComp    int           // Current number of active compactions
	compStartTime time.Time     // The start time of the earliest currently-active compaction
	compTime      atomic.Int64  // Total time spent in compaction in ns
	level0Comp    atomic.Uint32 // Total number of level-zero compactions
	nonLevel0Comp atomic.Uint32 // Total number of non level-zero compactions

	writeStalled        atomic.Bool  // Flag whether the write is stalled
	writeDelayStartTime time.Time    // The start time of the latest write stall
	writeDelayCount     atomic.Int64 // Total number of write stall counts
	writeDelayTime      atomic.Int64 // Total time spent in write stalls

	writeOptions *pebble.WriteOptions
}

func (d *Database) onCompactionBegin(info pebble.CompactionInfo) {
	if d.activeComp == 0 {
		d.compStartTime = time.Now()
	}
	l0 := info.Input[0]
	if l0.Level == 0 {
		d.level0Comp.Add(1)
	} else {
		d.nonLevel0Comp.Add(1)
	}
	d.activeComp++
}

func (d *Database) onCompactionEnd(info pebble.CompactionInfo) {
	if d.activeComp == 1 {
		d.compTime.Add(int64(time.Since(d.compStartTime)))
	} else if d.activeComp == 0 {
		panic("should not happen")
	}
	d.activeComp--
}

func (d *Database) onWriteStallBegin(b pebble.WriteStallBeginInfo) {
	d.writeDelayStartTime = time.Now()
	d.writeDelayCount.Add(1)
	d.writeStalled.Store(true)
}

func (d *Database) onWriteStallEnd() {
	d.writeDelayTime.Add(int64(time.Since(d.writeDelayStartTime)))
	d.writeStalled.Store(false)
}

// panicLogger is just a noop logger to disable Pebble's internal logger.
//
// TODO(karalabe): Remove when Pebble sets this as the default.
type panicLogger struct{}

func (l panicLogger) Infof(format string, args ...interface{}) {
}

func (l panicLogger) Errorf(format string, args ...interface{}) {
}

func (l panicLogger) Fatalf(format string, args ...interface{}) {
	panic(fmt.Errorf("fatal: "+format, args...))
}

// New returns a wrapped pebble DB object. The namespace is the prefix that the
// metrics reporting should use for surfacing internal stats.
func New(file1, file2 string, cache int, handles int, namespace string, readonly bool, ephemeral bool) (*Database, error) {
	// Ensure we have some minimal caching and file guarantees
	if cache < minCache {
		cache = minCache
	}
	if handles < minHandles {
		handles = minHandles
	}
	logger := log.New("database", file1)
	logger.Info("Allocated cache and file handles", "cache", common.StorageSize(cache*1024*1024), "handles", handles)

	// The max memtable size is limited by the uint32 offsets stored in
	// internal/arenaskl.node, DeferredBatchOp, and flushableBatchEntry.
	//
	// - MaxUint32 on 64-bit platforms;
	// - MaxInt on 32-bit platforms.
	//
	// It is used when slices are limited to Uint32 on 64-bit platforms (the
	// length limit for slices is naturally MaxInt on 32-bit platforms).
	//
	// Taken from https://github.com/cockroachdb/pebble/blob/master/internal/constants/constants.go
	maxMemTableSize := (1<<31)<<(^uint(0)>>63) - 1

	// Two memory tables is configured which is identical to leveldb,
	// including a frozen memory table and another live one.
	memTableLimit := 2
	memTableSize := cache * 1024 * 1024 / 2 / memTableLimit

	// The memory table size is currently capped at maxMemTableSize-1 due to a
	// known bug in the pebble where maxMemTableSize is not recognized as a
	// valid size.
	//
	// TODO use the maxMemTableSize as the maximum table size once the issue
	// in pebble is fixed.
	if memTableSize >= maxMemTableSize {
		memTableSize = maxMemTableSize - 1
	}
	db := &Database{
		hotFn:        file1,
		coldFn:       file2,
		log:          logger,
		quitChan:     make(chan chan error),
		writeOptions: &pebble.WriteOptions{Sync: !ephemeral},
	}
	opt := &pebble.Options{
		// Pebble has a single combined cache area and the write
		// buffers are taken from this too. Assign all available
		// memory allowance for cache.
		Cache:        pebble.NewCache(int64(cache * 1024 * 1024)),
		MaxOpenFiles: handles,

		// The size of memory table(as well as the write buffer).
		// Note, there may have more than two memory tables in the system.
		MemTableSize: uint64(memTableSize),

		// MemTableStopWritesThreshold places a hard limit on the size
		// of the existent MemTables(including the frozen one).
		// Note, this must be the number of tables not the size of all memtables
		// according to https://github.com/cockroachdb/pebble/blob/master/options.go#L738-L742
		// and to https://github.com/cockroachdb/pebble/blob/master/db.go#L1892-L1903.
		MemTableStopWritesThreshold: memTableLimit,

		// The default compaction concurrency(1 thread),
		// Here use all available CPUs for faster compaction.
		MaxConcurrentCompactions: runtime.NumCPU,

		// Per-level options. Options for at least one level must be specified. The
		// options for the last level are used for all subsequent levels.
		Levels: []pebble.LevelOptions{
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
		},
		ReadOnly: readonly,
		EventListener: &pebble.EventListener{
			CompactionBegin: db.onCompactionBegin,
			CompactionEnd:   db.onCompactionEnd,
			WriteStallBegin: db.onWriteStallBegin,
			WriteStallEnd:   db.onWriteStallEnd,
		},
		Logger: panicLogger{}, // TODO(karalabe): Delete when this is upstreamed in Pebble
	}
	// Disable seek compaction explicitly. Check https://github.com/ethereum/go-ethereum/pull/20130
	// for more details.
	opt.Experimental.ReadSamplingMultiplier = -1

	// Open the db and recover any potential corruptions
	innerDB1, err := pebble.Open(file1, opt)
	if err != nil {
		return nil, err
	}
	db.hotDb = innerDB1
	innerDB2, err := pebble.Open(file2, opt)
	if err != nil {
		return nil, err
	}
	db.coldDb = innerDB2

	db.compTimeMeter = metrics.GetOrRegisterMeter(namespace+"compact/time", nil)
	db.compReadMeter = metrics.GetOrRegisterMeter(namespace+"compact/input", nil)
	db.compWriteMeter = metrics.GetOrRegisterMeter(namespace+"compact/output", nil)
	db.diskSizeGauge = metrics.GetOrRegisterGauge(namespace+"disk/size", nil)
	db.diskReadMeter = metrics.GetOrRegisterMeter(namespace+"disk/read", nil)
	db.diskWriteMeter = metrics.GetOrRegisterMeter(namespace+"disk/write", nil)
	db.writeDelayMeter = metrics.GetOrRegisterMeter(namespace+"compact/writedelay/duration", nil)
	db.writeDelayNMeter = metrics.GetOrRegisterMeter(namespace+"compact/writedelay/counter", nil)
	db.memCompGauge = metrics.GetOrRegisterGauge(namespace+"compact/memory", nil)
	db.level0CompGauge = metrics.GetOrRegisterGauge(namespace+"compact/level0", nil)
	db.nonlevel0CompGauge = metrics.GetOrRegisterGauge(namespace+"compact/nonlevel0", nil)
	db.seekCompGauge = metrics.GetOrRegisterGauge(namespace+"compact/seek", nil)
	db.manualMemAllocGauge = metrics.GetOrRegisterGauge(namespace+"memory/manualalloc", nil)

	// Start up the metrics gathering and return
	go db.meter(metricsGatheringInterval, namespace)
	go db.BackgroundMigration()
	return db, nil
}

const TIME_INTERVAL = 1

func (d *Database) BackgroundMigration() {
	var errc chan error
	ticker := time.NewTicker(TIME_INTERVAL * time.Second)
	defer ticker.Stop()

	go func() {
		for i := 1; errc == nil; i++ {
			select {
			case <-ticker.C:
				if err := d.backgroundMigration(); err != nil {
					d.log.Error("Background Migration Failed", "err", err)
				} else {
					fmt.Println("Background Migration Success")
				}
			case errc = <-d.quitChan:
			}
		}
	}()

	select {}
}

func (d *Database) backgroundMigration() error {
	iter := d.NewIteratorForMigration(prefixHDD, nil).(*pebbleIterator)
	defer iter.ReleaseHotHDD()
	batchHot := d.hotDb.NewBatch()
	defer batchHot.Close()
	batchCold := &batch{
		bHot:  d.hotDb.NewBatch(),
		bCold: d.coldDb.NewBatch(),
		db:    d,
	}
	i := 0
	for iter.NextHot() {
		key := iter.Key()
		value := iter.Value()
		keyPrefixHDD := append(prefixHDD, key...)
		batchCold.PutCold(keyPrefixHDD, value)
		if err := batchHot.Delete(keyPrefixHDD, d.writeOptions); err != nil {
			return err
		}
		i++
		if i >= 1000000 {
			break
		}
	}
	if err := batchCold.CommitCold(); err != nil {
		return err
	}
	if err := batchHot.Commit(d.writeOptions); err != nil {
		return err
	}
	fmt.Println("Eviction Iteration :", i)
	return nil
}
func (d *Database) NewIteratorForMigration(prefix []byte, start []byte) ethdb.Iterator {
	iterHotHDD, _ := d.hotDb.NewIter(&pebble.IterOptions{
		LowerBound: append(prefix, start...),
		UpperBound: upperBound(prefix),
	})
	iterHotHDD.First()
	return &pebbleIterator{
		iterHotHDD:     iterHotHDD,
		moved:          true,
		released:       false,
		movedHotSSD:    true,
		movedHotHDD:    true,
		movedCold:      true,
		releasedHotSSD: false,
		releasedHotHDD: false,
		releasedCold:   false,
		validHotSSD:    false,
		validHotHDD:    true,
		validCold:      false,
		turn:           1,
	}
}

func (iter *pebbleIterator) NextHot() bool {
	if iter.movedHotHDD {
		iter.movedHotHDD = false
		return iter.iterHotHDD.Valid()
	}
	return iter.iterHotHDD.Next()
}

func (b *batch) PutCold(key, value []byte) error {
	b.bCold.Set(key, value, nil)
	b.sizeCold += len(key) + len(value)
	return nil
}

func (b *batch) CommitCold() error {
	b.db.quitColdLock.RLock()
	defer b.db.quitColdLock.RUnlock()
	if b.db.closed {
		return pebble.ErrClosed
	}
	if err := b.bCold.Commit(b.db.writeOptions); err != nil {
		fmt.Println("dbCold Batch Write Error:", err)
		return err
	}
	return nil
}

// Close stops the metrics collection, flushes any pending data to disk and closes
// all io accesses to the underlying key-value store.
func (d *Database) Close() error {
	d.quitLock.Lock()
	defer d.quitLock.Unlock()
	// Allow double closing, simplifies things
	if d.closed {
		return nil
	}
	d.closed = true
	if d.quitChan != nil {
		errc := make(chan error)
		d.quitChan <- errc
		if err := <-errc; err != nil {
			d.log.Error("Metrics collection failed", "err", err)
		}
		d.quitChan = nil
	}
	err1 := d.hotDb.Close()
	err2 := d.coldDb.Close()
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}

	return nil
}

// Has retrieves if a key is present in the key-value store.
func (d *Database) Has(key []byte) (bool, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return false, pebble.ErrClosed
	}
	keyPrefixSSD := append(prefixSSD, key...)
	keyPrefixHDD := append(prefixHDD, key...)
	_, closer, err := d.hotDb.Get(keyPrefixSSD)
	if err != nil {
		_, closer, err = d.hotDb.Get(keyPrefixHDD)
		// check cold db if key is not found in hot db
		if err == pebble.ErrNotFound {
			_, err = d.GetCold(keyPrefixHDD)
			if err == pebble.ErrNotFound {
				return false, nil
			} else if err != nil {
				return false, err
			}
			return true, nil
		} else if err != nil {
			return false, err
		}
	}
	defer closer.Close()
	return true, nil
}

// Get retrieves the given key if it's present in the key-value store.
func (d *Database) Get(key []byte) ([]byte, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return nil, pebble.ErrClosed
	}
	keyPrefixSSD := append(prefixSSD, key...)
	keyPrefixHDD := append(prefixHDD, key...)
	dat, closer, err := d.hotDb.Get(keyPrefixSSD)
	if err != nil {
		dat, closer, err = d.hotDb.Get(keyPrefixHDD)
		// fmt.Printf("ghk: %d\n", keyPrefixHDD[0])
		// check cold db if key is not found in hot db
		if err != nil {
			dat, err := d.GetCold(keyPrefixHDD)
			if err != nil {
				return nil, err
			}
			ret := make([]byte, len(dat))
			copy(ret, dat)
			return ret, nil
		}
	}
	ret := make([]byte, len(dat))
	copy(ret, dat)
	closer.Close()
	return ret, nil
}

func (d *Database) GetCold(key []byte) ([]byte, error) {
	d.quitColdLock.RLock()
	defer d.quitColdLock.RUnlock()
	if d.closed {
		return nil, pebble.ErrClosed
	}
	dat, closer, err := d.coldDb.Get(key)
	if err != nil {
		return nil, err
	}
	ret := make([]byte, len(dat))
	copy(ret, dat)
	closer.Close()
	return ret, nil
}

// Put inserts the given value into the key-value store.
func (d *Database) Put(idx int, key []byte, value []byte) error {
	batch := &batch{
		bHot:  d.hotDb.NewBatch(),
		bCold: d.coldDb.NewBatch(),
		db:    d,
	}
	batch.Put(idx, key, value)
	return batch.Write(0)
}

// Delete removes the key from the key-value store.
func (d *Database) Delete(idx int, key []byte) error {
	batch := &batch{
		bHot:  d.hotDb.NewBatch(),
		bCold: d.coldDb.NewBatch(),
		db:    d,
	}
	batch.Delete(idx, key)
	return batch.Write(0)
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (d *Database) NewBatch() ethdb.Batch {
	return &batch{
		bHot:  d.hotDb.NewBatch(),
		bCold: d.coldDb.NewBatch(),
		db:    d,
	}
}

// NewBatchWithSize creates a write-only database batch with pre-allocated buffer.
func (d *Database) NewBatchWithSize(size int) ethdb.Batch {
	return &batch{
		bHot:  d.hotDb.NewBatchWithSize(size),
		bCold: d.coldDb.NewBatchWithSize(size),
		db:    d,
	}
}

// snapshot wraps a pebble snapshot for implementing the Snapshot interface.
type snapshot struct {
	dbHot  *pebble.Snapshot
	dbCold *pebble.Snapshot
}

// NewSnapshot creates a database snapshot based on the current state.
// The created snapshot will not be affected by all following mutations
// happened on the database.
// Note don't forget to release the snapshot once it's used up, otherwise
// the stale data will never be cleaned up by the underlying compactor.
func (d *Database) NewSnapshot() (ethdb.Snapshot, error) {
	snapHot := d.hotDb.NewSnapshot()
	snapCold := d.coldDb.NewSnapshot()
	return &snapshot{dbHot: snapHot, dbCold: snapCold}, nil
}

// Has checks if the given key is present in either the hot or cold snapshot.
func (snap *snapshot) Has(key []byte) (bool, error) {
	keyPrefixSSD := append(prefixSSD, key...)
	if has, err := snap.hasInSnapshot(snap.dbHot, keyPrefixSSD); err != nil || has {
		return has, err
	}

	keyPrefixHDD := append(prefixHDD, key...)
	if has, err := snap.hasInSnapshot(snap.dbHot, keyPrefixHDD); err != nil || has {
		return has, err
	}
	return snap.hasInSnapshot(snap.dbCold, keyPrefixHDD)
}

func (snap *snapshot) hasInSnapshot(snapDb *pebble.Snapshot, key []byte) (bool, error) {
	_, closer, err := snapDb.Get(key)
	if err != nil {
		if err != pebble.ErrNotFound {
			return false, err
		}
		return false, nil
	}
	closer.Close()
	return true, nil
}

// Get retrieves the given key if it's present in either the hot or cold snapshot.
func (snap *snapshot) Get(key []byte) ([]byte, error) {
	keyPrefixSSD := append(prefixSSD, key...)
	if data, err := snap.getFromSnapshot(snap.dbHot, keyPrefixSSD); err == nil {
		return data, nil
	} else if err != pebble.ErrNotFound {
		return nil, err
	}

	keyPrefixHDD := append(prefixHDD, key...)
	if data, err := snap.getFromSnapshot(snap.dbHot, keyPrefixHDD); err == nil {
		return data, nil
	} else if err != pebble.ErrNotFound {
		return nil, err
	}
	return snap.getFromSnapshot(snap.dbCold, keyPrefixHDD)
}

// getFromSnapshot is a helper function to get a key from a given snapshot.
func (snap *snapshot) getFromSnapshot(snapDb *pebble.Snapshot, key []byte) ([]byte, error) {
	dat, closer, err := snapDb.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	ret := make([]byte, len(dat))
	copy(ret, dat)
	return ret, nil
}

// Release releases associated resources. Release should always succeed and can
// be called multiple times without causing error.
func (snap *snapshot) Release() {
	snap.dbHot.Close()
	snap.dbCold.Close()
}

// upperBound returns the upper bound for the given prefix
func upperBound(prefix []byte) (limit []byte) {
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c == 0xff {
			continue
		}
		limit = make([]byte, i+1)
		copy(limit, prefix)
		limit[i] = c + 1
		break
	}
	return limit
}

// Stat returns the internal metrics of Pebble in a text format. It's a developer
// method to read everything there is to read, independent of Pebble version.
func (d *Database) Stat() (string, error) {
	return d.hotDb.Metrics().String(), nil
}

// Compact flattens the underlying data store for the given key range. In essence,
// deleted and overwritten versions are discarded, and the data is rearranged to
// reduce the cost of operations needed to access them.
//
// A nil start is treated as a key before all keys in the data store; a nil limit
// is treated as a key after all keys in the data store. If both is nil then it
// will compact entire data store.
func (d *Database) Compact(start []byte, limit []byte) error {
	// There is no special flag to represent the end of key range
	// in pebble(nil in leveldb). Use an ugly hack to construct a
	// large key to represent it.
	// Note any prefixed database entry will be smaller than this
	// flag, as for trie nodes we need the 32 byte 0xff because
	// there might be a shared prefix starting with a number of
	// 0xff-s, so 32 ensures than only a hash collision could touch it.
	// https://github.com/cockroachdb/pebble/issues/2359#issuecomment-1443995833

	// TODO: Parallelization Compaction?
	var wg sync.WaitGroup
	var mu sync.Mutex

	var err error

	compactDb := func(db *pebble.DB) {
		defer wg.Done()
		if e := db.Compact(start, limit, true); e != nil {
			mu.Lock()
			if err == nil {
				err = e
			}
			mu.Unlock()
		}
	}

	wg.Add(1)
	go compactDb(d.hotDb)

	wg.Add(1)
	go compactDb(d.coldDb)

	wg.Wait()

	if err != nil {
		return err
	}

	return nil
}

// Path returns the path to the database directory.
func (d *Database) Path() string {
	return d.hotFn
}

// meter periodically retrieves internal pebble counters and reports them to
// the metrics subsystem.
func (d *Database) meter(refresh time.Duration, namespace string) {
	var errc chan error
	timer := time.NewTimer(refresh)
	defer timer.Stop()

	// Create storage and warning log tracer for write delay.
	var (
		compTimes  [2]int64
		compWrites [2]int64
		compReads  [2]int64

		nWrites [2]int64

		writeDelayTimes      [2]int64
		writeDelayCounts     [2]int64
		lastWriteStallReport time.Time
	)

	// Iterate ad infinitum and collect the stats
	for i := 1; errc == nil; i++ {
		var (
			compWrite int64
			compRead  int64
			nWrite    int64

			stats              = d.hotDb.Metrics()
			compTime           = d.compTime.Load()
			writeDelayCount    = d.writeDelayCount.Load()
			writeDelayTime     = d.writeDelayTime.Load()
			nonLevel0CompCount = int64(d.nonLevel0Comp.Load())
			level0CompCount    = int64(d.level0Comp.Load())
		)
		writeDelayTimes[i%2] = writeDelayTime
		writeDelayCounts[i%2] = writeDelayCount
		compTimes[i%2] = compTime

		for _, levelMetrics := range stats.Levels {
			nWrite += int64(levelMetrics.BytesCompacted)
			nWrite += int64(levelMetrics.BytesFlushed)
			compWrite += int64(levelMetrics.BytesCompacted)
			compRead += int64(levelMetrics.BytesRead)
		}

		nWrite += int64(stats.WAL.BytesWritten)

		compWrites[i%2] = compWrite
		compReads[i%2] = compRead
		nWrites[i%2] = nWrite

		if d.writeDelayNMeter != nil {
			d.writeDelayNMeter.Mark(writeDelayCounts[i%2] - writeDelayCounts[(i-1)%2])
		}
		if d.writeDelayMeter != nil {
			d.writeDelayMeter.Mark(writeDelayTimes[i%2] - writeDelayTimes[(i-1)%2])
		}
		// Print a warning log if writing has been stalled for a while. The log will
		// be printed per minute to avoid overwhelming users.
		if d.writeStalled.Load() && writeDelayCounts[i%2] == writeDelayCounts[(i-1)%2] &&
			time.Now().After(lastWriteStallReport.Add(degradationWarnInterval)) {
			d.log.Warn("Database compacting, degraded performance")
			lastWriteStallReport = time.Now()
		}
		if d.compTimeMeter != nil {
			d.compTimeMeter.Mark(compTimes[i%2] - compTimes[(i-1)%2])
		}
		if d.compReadMeter != nil {
			d.compReadMeter.Mark(compReads[i%2] - compReads[(i-1)%2])
		}
		if d.compWriteMeter != nil {
			d.compWriteMeter.Mark(compWrites[i%2] - compWrites[(i-1)%2])
		}
		if d.diskSizeGauge != nil {
			d.diskSizeGauge.Update(int64(stats.DiskSpaceUsage()))
		}
		if d.diskReadMeter != nil {
			d.diskReadMeter.Mark(0) // pebble doesn't track non-compaction reads
		}
		if d.diskWriteMeter != nil {
			d.diskWriteMeter.Mark(nWrites[i%2] - nWrites[(i-1)%2])
		}
		// See https://github.com/cockroachdb/pebble/pull/1628#pullrequestreview-1026664054
		manuallyAllocated := stats.BlockCache.Size + int64(stats.MemTable.Size) + int64(stats.MemTable.ZombieSize)
		d.manualMemAllocGauge.Update(manuallyAllocated)
		d.memCompGauge.Update(stats.Flush.Count)
		d.nonlevel0CompGauge.Update(nonLevel0CompCount)
		d.level0CompGauge.Update(level0CompCount)
		d.seekCompGauge.Update(stats.Compact.ReadCount)

		for i, level := range stats.Levels {
			// Append metrics for additional layers
			if i >= len(d.levelsGauge) {
				d.levelsGauge = append(d.levelsGauge, metrics.GetOrRegisterGauge(namespace+fmt.Sprintf("tables/level%v", i), nil))
			}
			d.levelsGauge[i].Update(level.NumFiles)
		}

		// Sleep a bit, then repeat the stats collection
		select {
		case errc = <-d.quitChan:
			// Quit requesting, stop hammering the database
		case <-timer.C:
			timer.Reset(refresh)
			// Timeout, gather a new set of stats
		}
	}
	errc <- nil
}

// type LOGGING struct {
// 	key  string
// 	size int
// 	idx  int
// }

// batch is a write-only batch that commits changes to its host database
// when Write is called. A batch cannot be used concurrently.
type batch struct {
	// b    *pebble.Batch
	bHot     *pebble.Batch
	bCold    *pebble.Batch
	db       *Database
	sizeHot  int
	sizeCold int
	// keys []LOGGING
	// putkeys []LOGGING
	// delkeys []LOGGING
}

func isPutHDD(idx int) bool {
	var trieIdx = []int{471, 481, 491}
	var snapshotIdx = []int{291, 301, 311, 331, 341, 351}
	for _, i := range trieIdx {
		if i == idx {
			return true
		}
	}
	for _, i := range snapshotIdx {
		if i == idx {
			return true
		}
	}
	return false
}

// Put inserts the given value into the batch for later committing.
func (b *batch) Put(idx int, key, value []byte) error {
	if isPutHDD(idx) {
		key = append(prefixHDD, key...)
	} else {
		key = append(prefixSSD, key...)
	}
	// fmt.Printf("pk: %d\n", key[0])
	b.bHot.Set(key, value, nil)
	b.sizeHot += len(key) + len(value)
	// b.putkeys = append(b.putkeys, LOGGING{key: base64.StdEncoding.EncodeToString(key), size: len(key) + len(value), idx: idx})

	return nil
}

// Delete inserts the key removal into the batch for later committing.
func (b *batch) Delete(idx int, key []byte) error {
	keyCold := append(prefixHDD, key...)
	b.bCold.Delete(keyCold, nil)
	b.sizeCold += len(keyCold)

	keyHot := append(prefixSSD, key...)
	b.bHot.Delete(keyHot, nil)
	b.sizeHot += len(keyHot)

	// b.delkeys = append(b.delkeys, LOGGING{key: base64.StdEncoding.EncodeToString(key), idx: idx})

	return nil
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *batch) ValueSize() int {
	return b.sizeHot + b.sizeCold
}

func (b *batch) Write(idx int) error {
	b.db.quitLock.RLock()
	if b.db.closed {
		b.db.quitLock.RUnlock()
		return pebble.ErrClosed
	}
	if err := b.bHot.Commit(b.db.writeOptions); err != nil {
		fmt.Println("dbHot Batch Write Error:", err)
		return err
	}
	b.db.quitLock.RUnlock()
	b.db.quitColdLock.RLock()
	defer b.db.quitColdLock.RUnlock()
	if err := b.bCold.Commit(b.db.writeOptions); err != nil {
		fmt.Println("dbCold Batch Write Error:", err)
		return err
	}
	return nil
}

// Reset resets the batch for reuse.
func (b *batch) Reset() {
	b.bHot.Reset()
	b.bCold.Reset()
	b.sizeHot = 0
	b.sizeCold = 0
}

// Replay replays the batch contents.
func (b *batch) Replay(w ethdb.KeyValueWriter) error {
	if err := replayBatch(b.bHot, w); err != nil {
		fmt.Println("dbHot Batch Replay Error:", err)
		return err
	}
	if err := replayBatch(b.bCold, w); err != nil {
		fmt.Println("dbCold Batch Replay Error:", err)
		return err
	}
	return nil
}

// replayBatch replays the operations in the given batch to the KeyValueWriter.
func replayBatch(batch *pebble.Batch, w ethdb.KeyValueWriter) error {
	reader := batch.Reader()
	for {
		kind, k, v, ok, err := reader.Next()
		if !ok || err != nil {
			break
		}
		// The (k,v) slices might be overwritten if the batch is reset/reused,
		// and the receiver should copy them if they are to be retained long-term.
		if kind == pebble.InternalKeyKindSet {
			w.Put(0, k, v)
		} else if kind == pebble.InternalKeyKindDelete {
			w.Delete(0, k)
		} else {
			return fmt.Errorf("unhandled operation, keytype: %v", kind)
		}
	}
	return nil
}

// pebbleIterator is a wrapper of underlying iterator in storage engine.
// The purpose of this structure is to implement the missing APIs.
//
// The pebble iterator is not thread-safe.
type pebbleIterator struct {
	iterHotSSD     *pebble.Iterator
	iterHotHDD     *pebble.Iterator
	iterCold       *pebble.Iterator
	movedHotSSD    bool
	movedHotHDD    bool
	movedCold      bool
	releasedHotSSD bool
	releasedHotHDD bool
	releasedCold   bool
	validHotSSD    bool
	validHotHDD    bool
	validCold      bool
	moved          bool
	released       bool
	turn           int
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (d *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	prefixHot := append(prefixSSD, prefix...)
	prefixCold := append(prefixHDD, prefix...)
	iterHotSSD, _ := d.hotDb.NewIter(&pebble.IterOptions{
		LowerBound: append(prefixHot, start...),
		UpperBound: upperBound(prefixHot),
	})
	iterHotHDD, _ := d.hotDb.NewIter(&pebble.IterOptions{
		LowerBound: append(prefixCold, start...),
		UpperBound: upperBound(prefixCold),
	})
	iterCold, _ := d.coldDb.NewIter(&pebble.IterOptions{
		LowerBound: append(prefixCold, start...),
		UpperBound: upperBound(prefixCold),
	})
	iterHotSSD.First()
	iterHotHDD.First()
	iterCold.First()
	return &pebbleIterator{
		iterHotSSD:     iterHotSSD,
		iterHotHDD:     iterHotHDD,
		iterCold:       iterCold,
		moved:          true,
		released:       false,
		movedHotSSD:    true,
		movedHotHDD:    true,
		movedCold:      true,
		releasedHotSSD: false,
		releasedHotHDD: false,
		releasedCold:   false,
		validHotSSD:    true,
		validHotHDD:    true,
		validCold:      true,
		turn:           0,
	}
}

// Next moves the iterator to the next key/value pair. It returns whether the
// iterator is exhausted.
func (iter *pebbleIterator) Next() bool {
	if iter.movedHotSSD && iter.movedHotHDD && iter.movedCold {
		iter.movedHotSSD = false
		iter.movedHotHDD = false
		iter.movedCold = false
		iter.validHotSSD = iter.iterHotSSD.Valid()
		iter.validHotHDD = iter.iterHotHDD.Valid()
		iter.validCold = iter.iterCold.Valid()
		if iter.validHotSSD && iter.validHotHDD {
			result := bytes.Compare(iter.KeyHotSSD(), iter.KeyHotHDD())
			if result < 0 {
				iter.turn = 0
			} else if result > 0 {
				iter.turn = 1
			} else {
				iter.turn = 0
				iter.validHotHDD = iter.iterHotHDD.Next()
			}
		} else if iter.validHotSSD && iter.validCold {
			result := bytes.Compare(iter.KeyHotSSD(), iter.KeyCold())
			if result < 0 {
				iter.turn = 0
			} else if result > 0 {
				iter.turn = 2
			} else {
				iter.turn = 0
				iter.validCold = iter.iterCold.Next()
			}
		} else if iter.validHotHDD && iter.validCold {
			result := bytes.Compare(iter.KeyHotHDD(), iter.KeyCold())
			if result < 0 {
				iter.turn = 1
			} else if result > 0 {
				iter.turn = 2
			} else {
				iter.turn = 1
				iter.validCold = iter.iterCold.Next()
			}
		} else if iter.validHotSSD {
			iter.turn = 0
		} else if iter.validHotHDD {
			iter.turn = 1
		} else if iter.validCold {
			iter.turn = 2
		}
		return iter.validHotSSD || iter.validHotHDD || iter.validCold
	}

	if iter.turn == 0 {
		iter.validHotSSD = iter.iterHotSSD.Next()
	} else if iter.turn == 1 {
		iter.validHotHDD = iter.iterHotHDD.Next()
	} else if iter.turn == 2 {
		iter.validCold = iter.iterCold.Next()
	}

	if iter.validHotSSD && iter.validHotHDD {
		result := bytes.Compare(iter.KeyHotSSD(), iter.KeyHotHDD())
		if result < 0 {
			iter.turn = 0
		} else if result > 0 {
			iter.turn = 1
		} else {
			if iter.turn == 0 {
				iter.validHotHDD = iter.iterHotHDD.Next()
			} else {
				iter.validHotSSD = iter.iterHotSSD.Next()
			}
			if iter.validHotSSD {
				iter.turn = 0
			} else {
				iter.turn = 1
			}
		}
	} else if iter.validHotSSD && iter.validCold {
		result := bytes.Compare(iter.KeyHotSSD(), iter.KeyCold())
		if result < 0 {
			iter.turn = 0
		} else if result > 0 {
			iter.turn = 2
		} else {
			if iter.turn == 0 {
				iter.validCold = iter.iterCold.Next()
			} else {
				iter.validHotSSD = iter.iterHotSSD.Next()
			}
			if iter.validHotSSD {
				iter.turn = 0
			} else {
				iter.turn = 2
			}
		}
	} else if iter.validHotHDD && iter.validCold {
		result := bytes.Compare(iter.KeyHotHDD(), iter.KeyCold())
		if result < 0 {
			iter.turn = 1
		} else if result > 0 {
			iter.turn = 2
		} else {
			if iter.turn == 1 {
				iter.validCold = iter.iterCold.Next()
			} else {
				iter.validHotHDD = iter.iterHotHDD.Next()
			}
			if iter.validHotHDD {
				iter.turn = 1
			} else {
				iter.turn = 2
			}
		}
	} else if iter.validHotSSD {
		iter.turn = 0
	} else if iter.validHotHDD {
		iter.turn = 1
	} else if iter.validCold {
		iter.turn = 2
	}

	return iter.validHotSSD || iter.validHotHDD || iter.validCold
}

func (iter *pebbleIterator) KeyHotSSD() []byte {
	if iter.validHotSSD {
		return iter.iterHotSSD.Key()[1:]
	}
	return nil
}

func (iter *pebbleIterator) KeyHotHDD() []byte {
	if iter.validHotHDD {
		return iter.iterHotHDD.Key()[1:]
	}
	return nil
}

func (iter *pebbleIterator) KeyCold() []byte {
	if iter.validCold {
		return iter.iterCold.Key()[1:]
	}
	return nil
}

// Error returns any accumulated error. Exhausting all the key/value pairs
// is not considered to be an error.
func (iter *pebbleIterator) Error() error {
	if err := iter.iterHotSSD.Error(); err != nil {
		return err
	}
	return iter.iterCold.Error()
}

// Key returns the key of the current key/value pair, or nil if done. The caller
// should not modify the contents of the returned slice, and its contents may
// change on the next call to Next.
func (iter *pebbleIterator) Key() []byte {
	// fmt.Printf("ik: %X\n", base64.StdEncoding.EncodeToString(iter.iter.Key()))
	if iter.turn == 0 {
		key := iter.iterHotSSD.Key()
		// fmt.Printf("ik0: %d\n", key[0])
		return key[1:]
	} else if iter.turn == 1 {
		key := iter.iterHotHDD.Key()
		// fmt.Printf("ik1: %d\n", key[0])
		return key[1:]
	} else {
		key := iter.iterCold.Key()
		// fmt.Printf("ik2: %d\n", key[0])
		return key[1:]
	}
}

// Value returns the value of the current key/value pair, or nil if done. The
// caller should not modify the contents of the returned slice, and its contents
// may change on the next call to Next.
func (iter *pebbleIterator) Value() []byte {
	if iter.turn == 0 {
		val := iter.iterHotSSD.Value()
		return val
	} else if iter.turn == 1 {
		val := iter.iterHotHDD.Value()
		return val
	} else {
		return iter.iterCold.Value()
	}
}

func (iter *pebbleIterator) ValueHotSSD() []byte {
	if iter.validHotSSD {
		return iter.iterHotSSD.Value()
	}
	return nil
}

func (iter *pebbleIterator) ReleaseHotSSD() {
	if !iter.releasedHotSSD {
		iter.iterHotSSD.Close()
		iter.releasedHotSSD = true
	}
}

func (iter *pebbleIterator) ReleaseHotHDD() {
	if !iter.releasedHotSSD {
		iter.iterHotHDD.Close()
		iter.releasedHotSSD = true
	}
}

// Release releases associated resources. Release should always succeed and can
// be called multiple times without causing error.
func (iter *pebbleIterator) Release() {
	if !iter.releasedHotSSD {
		iter.iterHotSSD.Close()
		iter.releasedHotSSD = true
	}
	if !iter.releasedHotHDD {
		iter.iterHotHDD.Close()
		iter.releasedHotHDD = true
	}
	if !iter.releasedCold {
		iter.iterCold.Close()
		iter.releasedCold = true
	}
}
