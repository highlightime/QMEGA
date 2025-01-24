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
	"encoding/binary"
	"fmt"
	"os"
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

	fileNameForSkeleton = "/home/yhseo/nvme/ethereum/execution/data/geth/chaindata/ancient/chain/skeleton.txt"
	// fileNameForSkeleton = "/home/yhseo/nvme/ethereum/execution/data/geth/chaindata/ancient/skeleton.txt"
	// prefetchNum = 1
	// getMemBatchSize = 8192

	hddPath                    = "/home/yhseo/nvme/ethereum/execution/data/geth/chaindata/ancient/chain"
	skeletonMaxLen             = 1016
	onDiskSkeletonSize         = skeletonMaxLen + 8
	migrationSkeletonBatchSize = 64 * 1024
	migrationBatchSize         = 128 * 1024
	// deleteBatchSize            = 1
)

// Database is a persistent key-value store based on the pebble storage engine.
// Apart from basic data storage functionality it also supports batch writes and
// iterating over the keyspace in binary-alphabetical order.
type Database struct {
	fn    string     // filename for reporting
	dbHot *pebble.DB // Underlying pebble storage engine
	// dbCold *leveldb.Database // Underlying pebble storage engine
	dbCold *pebble.DB

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

	quitLock      sync.RWMutex    // Mutex protecting the quit channel and the closed flag
	quitChan      chan chan error // Quit channel to stop the metrics collection before closing the database
	migrationChan chan *[]*ARYFORMIG
	closed        bool // keep track of whether we're Closed
	kvCnt         atomic.Int64

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

	writeOptions     *pebble.WriteOptions
	writeOptionsCold *pebble.WriteOptions

	skeletonFinalized atomic.Int64
	// prefetchChan        chan int
	// skeletonMem         map[int][]byte
	skeletonMigChan chan int
	// skeletonMemLock     sync.RWMutex
	migrationStopped int
	fileForSkeleton  *os.File
	// deleteSkeletonChan  chan int
	isMigratingSkeleton atomic.Bool
	// isStartedPrefetch   atomic.Bool
}

type onDiskSkeletonHeader struct {
	DataLen uint64
	Data    [skeletonMaxLen]byte
}

type ARYFORFILE struct {
	index int64
	value []byte
}

// func (d *Database) deleteSkeletonHeaderInMem() {
// 	for {
// 		select {
// 		case skeletonIdx := <-d.deleteSkeletonChan:
// 			if skeletonIdx%deleteBatchSize == 0 {
// 				d.deleteBatch(skeletonIdx)
// 			}
// 		case <-d.quitChan:
// 			return
// 		}
// 	}
// }

// func (d *Database) deleteBatch(skeletonIdx int) {
// 	// fmt.Println("delete start ", skeletonIdx-deleteBatchSize, "end ", skeletonIdx)
// 	d.skeletonMemLock.Lock()
// 	defer d.skeletonMemLock.Unlock()

// 	for i := skeletonIdx; i > skeletonIdx-deleteBatchSize; i-- {
// 		delete(d.skeletonMem, skeletonIdx)
// 	}
// }

func (d *Database) WriteSkeletonHeaderToFile(ary *[]*ARYFORFILE) {
	for _, putkey := range *ary {
		var mystruct onDiskSkeletonHeader
		mystruct.DataLen = uint64(len(putkey.value))
		if len(putkey.value) > 1016 {
			fmt.Println("dataLen: ", len(putkey.value))
		}
		copy(mystruct.Data[:], putkey.value)
		_, err := d.fileForSkeleton.Seek(onDiskSkeletonSize*putkey.index, 0)
		if err != nil {
			fmt.Printf("Failed to seek: %v\n", err)
			return
		}
		err = binary.Write(d.fileForSkeleton, binary.LittleEndian, &mystruct)
		if err != nil {
			fmt.Printf("Failed to write to file: %v\n", err)
			return
		}
	}
}

func (d *Database) processKV(idx int) (*ARYFORFILE, error) {
	key := skeletonHeaderKey(uint64(idx))
	data, closer, err := d.dbHot.Get(key)
	if err != nil {
		return nil, err
	}
	ret := new(ARYFORFILE)
	ret.value = make([]byte, len(data))
	copy(ret.value, data)
	ret.index = int64(idx)
	closer.Close()
	return ret, nil
}

func (d *Database) skeletonWriteAndDelete(ary *[]*ARYFORFILE) {
	d.WriteSkeletonHeaderToFile(ary)
	d.fileForSkeleton.Sync()
	d.DeleteSkeletonHeaderInSSD(ary)
	// fmt.Println("mig start ", (*ary)[0].index, "end ", (*ary)[len(*ary)-1].index)
}

func (d *Database) skeletonMigration() {
	ticker := time.NewTicker(3 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case oldFinalized := <-d.skeletonMigChan:
			fmt.Printf("mig old_finalized: %d\n", oldFinalized)
			d.handleFullMigration(oldFinalized)
		case <-ticker.C:
			fmt.Println("Running periodic migration")
			if !d.isMigratingSkeleton.Load() {
				d.handlePeriodicMigration()
			}

		case <-d.quitChan:
			fmt.Println("Stopping migration")
			return
		}
	}
}

func (d *Database) handleFullMigration(oldFinalized int) {
	d.isMigratingSkeleton.Store(true)
	defer d.isMigratingSkeleton.Store(false)
	first := d.migrationStopped == -1
	putkeys := new([]*ARYFORFILE)

	for i := oldFinalized; i > 1; i-- {
		entry, err := d.processKV(i)
		if err != nil {
			if err == pebble.ErrNotFound && first {
				d.migrationStopped = i
			}
			break
		}
		*putkeys = append(*putkeys, entry)
		if len(*putkeys) > migrationSkeletonBatchSize {
			d.skeletonWriteAndDelete(putkeys)
			putkeys = new([]*ARYFORFILE)
		}
	}
	d.runMigrationSkeletonLoop(putkeys)
}

func (d *Database) handlePeriodicMigration() {
	d.isMigratingSkeleton.Store(true)
	defer d.isMigratingSkeleton.Store(false)
	putkeys := new([]*ARYFORFILE)
	d.runMigrationSkeletonLoop(putkeys)
}

func (d *Database) runMigrationSkeletonLoop(putkeys *[]*ARYFORFILE) {
	for i := d.migrationStopped; i > 1; i-- {
		entry, err := d.processKV(i)
		if err != nil {
			if err == pebble.ErrNotFound {
				d.migrationStopped = i
			}
			break
		}
		*putkeys = append(*putkeys, entry)
		if len(*putkeys) > migrationSkeletonBatchSize {
			d.skeletonWriteAndDelete(putkeys)
			putkeys = new([]*ARYFORFILE)
		}
	}
	if len(*putkeys) > 0 {
		d.skeletonWriteAndDelete(putkeys)
	}
}

func (d *Database) DeleteSkeletonHeaderInSSD(ary *[]*ARYFORFILE) {
	batchHot := d.dbHot.NewBatch()
	for _, putkey := range *ary {
		key := skeletonHeaderKey(uint64(putkey.index))
		batchHot.Delete(key, d.writeOptions)
	}
	if err := batchHot.Commit(d.writeOptions); err != nil {
		d.log.Error("HotDB Delete Commit failed", "err", err)
	}
}

// New returns a wrapped pebble DB object. The namespace is the prefix that the
// metrics reporting should use for surfacing internal stats.
func New(file string, cache int, handles int, namespace string, readonly bool, ephemeral bool) (*Database, error) {
	// Ensure we have some minimal caching and file guarantees
	if cache < minCache {
		cache = minCache
	}
	if handles < minHandles {
		handles = minHandles
	}
	logger := log.New("database", file)
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
	memTableSize := cache * 1024 * 1024 / 2 / memTableLimit // 2MB

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
		fn:            file,
		log:           logger,
		quitChan:      make(chan chan error),
		migrationChan: make(chan *[]*ARYFORMIG, 100000),
		// prefetchChan:       make(chan int, 100000),
		skeletonMigChan: make(chan int, 100000),
		// deleteSkeletonChan: make(chan int, 100000),
		writeOptions:     &pebble.WriteOptions{Sync: !ephemeral},
		writeOptionsCold: &pebble.WriteOptions{Sync: false},
		migrationStopped: -1,
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
	innerDB1, err := pebble.Open(file, opt)
	if err != nil {
		return nil, err
	}
	db.dbHot = innerDB1
	opt2 := opt
	opt2.DisableWAL = true
	var fileSize int64 = 64 * 1024 * 1024
	opt2.MemTableSize = uint64(fileSize)

	opt2.Levels[0] = pebble.LevelOptions{TargetFileSize: fileSize, FilterPolicy: bloom.FilterPolicy(10)}
	opt2.Levels[1] = pebble.LevelOptions{TargetFileSize: fileSize, FilterPolicy: bloom.FilterPolicy(10)}
	opt2.Levels[2] = pebble.LevelOptions{TargetFileSize: fileSize, FilterPolicy: bloom.FilterPolicy(10)}
	opt2.Levels[3] = pebble.LevelOptions{TargetFileSize: fileSize, FilterPolicy: bloom.FilterPolicy(10)}
	opt2.Levels[4] = pebble.LevelOptions{TargetFileSize: fileSize, FilterPolicy: bloom.FilterPolicy(10)}
	opt2.Levels[5] = pebble.LevelOptions{TargetFileSize: fileSize, FilterPolicy: bloom.FilterPolicy(10)}
	opt2.Levels[6] = pebble.LevelOptions{TargetFileSize: fileSize, FilterPolicy: bloom.FilterPolicy(10)}
	fmt.Println("wal: ", opt.DisableWAL)
	opt2.BytesPerSync = 1024 * 1024 * 1024
	opt2.WALBytesPerSync = 1024 * 1024 * 1024
	// innerDB2, err := leveldb.New(hddPath, cache, handles, namespace, readonly)
	innerDB2, err := pebble.Open(hddPath, opt2)
	if err != nil {
		return nil, err
	}
	db.dbCold = innerDB2

	fileForSkeleton, err := os.Create(fileNameForSkeleton)
	if err != nil {
		fmt.Printf("Failed to create file: %v\n", err)
	}
	db.fileForSkeleton = fileForSkeleton

	// db.skeletonMem = make(map[int][]byte)

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

	fmt.Println("migrationSkeletonBatchSize: ", migrationSkeletonBatchSize, "migrationBatchSize: ", migrationBatchSize)

	// Print CPU max in setting
	fmt.Printf("procs %d\n", runtime.GOMAXPROCS(0))
	// Start up the metrics gathering and return
	go db.meter(metricsGatheringInterval, namespace)
	go db.migration()
	// go db.prefetchSkeletonHeader()
	go db.skeletonMigration()
	// go db.deleteSkeletonHeaderInMem()
	return db, nil
}

func (d *Database) migration() error {
	batchCold := &coldBatch{
		b:  d.dbCold.NewBatch(),
		db: d,
	}
	// batchCold := d.dbCold.NewBatch()
	batchHot := d.dbHot.NewBatch()
	defer batchHot.Close()
	for {
		select {
		case putkeys := <-d.migrationChan:
			for _, putkey := range *putkeys {
				batchCold.PutCold(*putkey.key, *putkey.value)
				if err := batchHot.Delete(*putkey.key, d.writeOptions); err != nil {
					panic(err)
				}
			}
			d.kvCnt.Add(int64(len(*putkeys)))

			if d.kvCnt.Load() >= migrationBatchSize {
				d.kvCnt.Store(0)
				if err := batchCold.b.Commit(d.writeOptionsCold); err != nil {
					d.log.Error("ColdDB Put Commit failed", "err", err)
				}
				err := d.dbCold.Flush()
				if err != nil {
					d.log.Error("ColdDB Flush failed", "err", err)
				}
				if err := batchHot.Commit(d.writeOptions); err != nil {
					d.log.Error("HotDB Delete Commit failed", "err", err)
				}
				batchCold = &coldBatch{
					b:  d.dbCold.NewBatch(),
					db: d,
				}
				// batchCold = d.dbCold.NewBatch()
				batchHot = d.dbHot.NewBatch()
			}
		case <-d.quitChan:
			return nil
		}
	}
}

func (b *coldBatch) PutCold(key, value []byte) error {
	b.b.Set(key, value, nil)
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
	return d.dbHot.Close()
}

// Has retrieves if a key is present in the key-value store.
func (d *Database) Has(idx int, key []byte) (bool, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return false, pebble.ErrClosed
	}
	_, closer, err := d.dbHot.Get(key)
	if err == pebble.ErrNotFound && isHasHDD(idx) {
		_, closer, err = d.dbCold.Get(key)
		if err == nil {
			defer closer.Close()
			return true, nil
		}
		return false, nil
	} else if err != nil {
		return false, err
	}
	defer closer.Close()
	return true, nil
}

// func (d *Database) prefetchSkeletonHeader() {
// 	for {
// 		select {
// 		case skeletonIdx := <-d.prefetchChan:
// 			d.isStartedPrefetch.Store(true)

// 			localSkeleton := make(map[int][]byte)
// 			End := prefetchNum + skeletonIdx
// 			skeletonFinalized := int(d.skeletonFinalized.Load())
// 			if End > skeletonFinalized {
// 				End = skeletonFinalized
// 			}
// 			// fmt.Println("prefetch start: ", skeletonIdx, "end: ", End)
// 			j := 0
// 			for i := skeletonIdx; i < End; i++ {
// 				key := skeletonHeaderKey(uint64(i))
// 				dat, closer, err := d.dbHot.Get(key)

// 				if err == pebble.ErrNotFound {
// 					dat, err := d.ReadSkeletonFromFile(i)
// 					if err != nil {
// 						fmt.Printf("Failed to read from file: %v\n", err)
// 					} else {
// 						ret := make([]byte, len(dat))
// 						copy(ret, dat)
// 						localSkeleton[i] = ret
// 					}
// 				} else if err != nil {
// 					fmt.Println("pebble db prefetch error")
// 				} else {
// 					ret := make([]byte, len(dat))
// 					copy(ret, dat)
// 					localSkeleton[i] = ret
// 				}

// 				if closer != nil {
// 					defer closer.Close()
// 				}
// 				if len(localSkeleton) >= prefetchNum {
// 					// fmt.Println("prefetch start: ", skeletonIdx+j*getMemBatchSize, "end: ", skeletonIdx+(j+1)*getMemBatchSize)
// 					j += 1
// 					d.skeletonMemLock.Lock()
// 					for key, value := range localSkeleton {
// 						// fmt.Println("prefetch local: ", key)
// 						d.skeletonMem[key] = value
// 					}
// 					d.skeletonMemLock.Unlock()
// 					localSkeleton = make(map[int][]byte)
// 				}
// 			}

// 			d.isStartedPrefetch.Store(false) // defer여서 호출이 되지
// 		case <-d.quitChan:
// 			d.isStartedPrefetch.Store(false)
// 			return
// 		}
// 	}
// }

func (d *Database) ReadSkeletonFromFile(i int) ([]byte, error) {
	file, err := os.Open(fileNameForSkeleton)
	if err != nil {
		fmt.Printf("Failed to open file: %v\n", err)
		return nil, err
	}
	offset := onDiskSkeletonSize * int64(i)
	_, err = file.Seek(offset, 0)
	if err != nil {
		fmt.Printf("Failed to seek: %v\n", err)
		return nil, err
	}
	var mystruct onDiskSkeletonHeader
	err = binary.Read(file, binary.LittleEndian, &mystruct)
	if err != nil {
		fmt.Printf("Failed to read from file: %v\n", err)
		return nil, err
	}
	if mystruct.DataLen == 0 {
		fmt.Println("dataLen is 0")
		temp := make([]byte, 0)
		return temp, nil
	}
	ret := make([]byte, mystruct.DataLen)
	copy(ret, mystruct.Data[:mystruct.DataLen])
	return ret, nil
}

// func (d *Database) getFromSkeletonMem(skeletonIdx int) ([]byte, bool, error) {
// 	d.skeletonMemLock.RLock()
// 	defer d.skeletonMemLock.RUnlock()

// 	// key := skeletonHeaderKey(uint64(skeletonIdx))
// 	if data, ok := d.skeletonMem[skeletonIdx]; ok {
// 		ret := make([]byte, len(data))
// 		copy(ret, data)

// 		// fmt.Printf("idx: %d, ret: %d\n", skeletonIdx, len(ret))
// 		if skeletonIdx <= int(d.skeletonFinalized.Load()) && skeletonIdx%deleteBatchSize == 0 {
// 			d.deleteSkeletonChan <- skeletonIdx
// 		}
// 		return ret, true, nil
// 	}

// 	return nil, false, nil
// }

// Get retrieves the given key if it's present in the key-value store.
func (d *Database) Get(idx int, key []byte) ([]byte, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return nil, pebble.ErrClosed
	}
	skeletonIdx := idx / 100

	// if isGetFile(idx) {
	// 	fmt.Println("get skeleton: ", skeletonIdx)
	// 	if (skeletonIdx)%prefetchNum == 0 {
	// 		if !d.isStartedPrefetch.Load() {
	// 			d.prefetchChan <- skeletonIdx + prefetchNum
	// 		}
	// 	}
	// 	data, found, _ := d.getFromSkeletonMem(skeletonIdx)
	// 	if found { // this
	// 		// fmt.Println("mem hit: ", skeletonIdx)
	// 		return data, nil
	// 	}
	// }

	dat, closer, err := d.dbHot.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			if isGetFile(idx) {
				// fmt.Println("key miss in hotdb & mem: ", skeletonIdx)
				dat, err = d.ReadSkeletonFromFile(skeletonIdx)
				if err == nil && dat != nil {
					// fmt.Println("file hit: ", skeletonIdx)
					ret := make([]byte, len(dat))
					copy(ret, dat)
					return ret, nil
				}
				return nil, err
			}
			if isGetColdDB(idx) {
				// dat, err = d.dbCold.Get(idx, key)
				dat, closer, err = d.dbCold.Get(key)
				if err == nil && dat != nil {
					ret := make([]byte, len(dat))
					copy(ret, dat)
					defer closer.Close()
					return ret, nil
				}
				return nil, err
			}
		}
		return nil, err
	}
	ret := make([]byte, len(dat))
	copy(ret, dat)
	defer closer.Close()
	return ret, nil
}

// Put inserts the given value into the key-value store.
func (d *Database) Put(idx int, key []byte, value []byte) error {
	batch := d.NewBatch()
	batch.Put(idx, key, value)
	err := batch.Write()
	return err
}

// Delete removes the key from the key-value store.
func (d *Database) Delete(key []byte) error {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return pebble.ErrClosed
	}
	return d.dbHot.Delete(key, nil)
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (d *Database) NewBatch() ethdb.Batch {
	return &batch{
		b:  d.dbHot.NewBatch(),
		db: d,
	}
}

// NewBatchWithSize creates a write-only database batch with pre-allocated buffer.
func (d *Database) NewBatchWithSize(size int) ethdb.Batch {
	return &batch{
		b:  d.dbHot.NewBatchWithSize(size),
		db: d,
	}
}

// snapshot wraps a pebble snapshot for implementing the Snapshot interface.
type snapshot struct {
	dbHot  *pebble.Snapshot
	dbCold *pebble.Snapshot
	// dbCold ethdb.Snapshot
}

// NewSnapshot creates a database snapshot based on the current state.
// The created snapshot will not be affected by all following mutations
// happened on the database.
// Note don't forget to release the snapshot once it's used up, otherwise
// the stale data will never be cleaned up by the underlying compactor.
func (d *Database) NewSnapshot() (ethdb.Snapshot, error) {
	snapHot := d.dbHot.NewSnapshot()
	snapCold := d.dbCold.NewSnapshot()
	return &snapshot{dbHot: snapHot, dbCold: snapCold}, nil
}

// Has retrieves if a key is present in the snapshot backing by a key-value
// data store.
func (snap *snapshot) Has(idx int, key []byte) (bool, error) {
	_, closer, err := snap.dbHot.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound && isHasHDD(idx) {
			_, closer, err = snap.dbCold.Get(key)
			if err == nil {
				defer closer.Close()
				return true, nil
			}
			return false, err
		} else {
			return false, nil
		}
	}
	defer closer.Close()
	return true, nil
}

// Get retrieves the given key if it's present in the snapshot backing by
// key-value data store.
func (snap *snapshot) Get(idx int, key []byte) ([]byte, error) {
	dat, closer, err := snap.dbHot.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound && isGetColdDB(idx) {
			dat, closer, err = snap.dbCold.Get(key)
			if err == nil {
				ret := make([]byte, len(dat))
				copy(ret, dat)
				defer closer.Close()
				return ret, nil
			}
		}
		return nil, err
	}
	ret := make([]byte, len(dat))
	copy(ret, dat)
	defer closer.Close()
	return ret, nil
}

// batch is a write-only batch that commits changes to its host database
// when Write is called. A batch cannot be used concurrently.
type batch struct {
	b       *pebble.Batch
	db      *Database
	putkeys []*ARYFORMIG
	skkeys  []*ARYFORSK
	size    int
}

type ARYFORSK struct {
	key   *[]byte
	value *[]byte
	idx   int
}

type coldBatch struct {
	b  *pebble.Batch
	db *Database
}

type ARYFORMIG struct {
	key   *[]byte
	value *[]byte
}

// Put inserts the given value into the batch for later committing.
func (b *batch) Put(idx int, key, value []byte) error {
	if isPutColdDB(idx) {
		b.putkeys = append(b.putkeys, &ARYFORMIG{key: &key, value: &value})
	}

	if isPutFile(idx) {
		// fmt.Println("bput skeleton: ", len(value), idx/100)
		b.skkeys = append(b.skkeys, &ARYFORSK{key: &key, value: &value, idx: idx})
	}
	b.b.Set(key, value, nil)
	b.size += len(key) + len(value)
	return nil
}

// Write flushes any accumulated data to disk.
func (b *batch) Write() error {
	b.db.quitLock.RLock()
	defer b.db.quitLock.RUnlock()
	if b.db.closed {
		return pebble.ErrClosed
	}
	res := b.b.Commit(b.db.writeOptions)
	if len(b.putkeys) > 0 {
		b.db.migrationChan <- &b.putkeys
		// go b.db.migrationNew(&b.putkeys)
	}

	if len(b.skkeys) > 0 {
		for _, putkey := range b.skkeys {
			idx := putkey.idx
			skeletonIdx := idx / 100
			if idx%100 == 37 {
				old_finalized := b.db.skeletonFinalized.Load()
				if old_finalized < int64(skeletonIdx) && b.db.skeletonFinalized.CompareAndSwap(old_finalized, int64(skeletonIdx)) {
					b.db.skeletonMigChan <- int(skeletonIdx) - 1000 // forked 되는 경우 finalized 바뀔수도..?
				}
			}
			// if idx%100 == 38 && skeletonIdx <= prefetchNum {
			// 	b.db.skeletonMemLock.Lock()
			// 	b.db.skeletonMem[skeletonIdx] = *putkey.value
			// 	b.db.skeletonMemLock.Unlock()
			// }
		}
	}

	return res
}

// Delete inserts the key removal into the batch for later committing.
func (b *batch) Delete(key []byte) error {
	b.b.Delete(key, nil)
	b.size += len(key)
	return nil
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *batch) ValueSize() int {
	return b.size
}

// Reset resets the batch for reuse.
func (b *batch) Reset() {
	b.b.Reset()
	b.size = 0
}

// Replay replays the batch contents.
func (b *batch) Replay(w ethdb.KeyValueWriter) error {
	reader := b.b.Reader()
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
			w.Delete(k)
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
	iterHot *pebble.Iterator
	// iterCold  ethdb.Iterator
	iterCold  *pebble.Iterator
	validHot  bool
	validCold bool
	moved     bool
	released  bool
	turn      bool
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (d *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	iterHot, _ := d.dbHot.NewIter(&pebble.IterOptions{
		LowerBound: append(prefix, start...),
		UpperBound: upperBound(prefix),
	})
	// iterCold := d.dbCold.NewIterator(prefix, start)
	iterCold, _ := d.dbCold.NewIter(&pebble.IterOptions{
		LowerBound: append(prefix, start...),
		UpperBound: upperBound(prefix),
	})
	iterHot.First()
	return &pebbleIterator{iterHot: iterHot, iterCold: iterCold, moved: true, released: false}
}

// Next moves the iterator to the next key/value pair. It returns whether the
// iterator is exhausted.
func (iter *pebbleIterator) Next() bool {
	if iter.moved {
		iter.moved = false
		iter.validHot = iter.iterHot.Valid()
		iter.validCold = iter.iterCold.Next()
		if iter.validHot && iter.validCold {
			res := bytes.Compare(iter.iterHot.Key(), iter.iterCold.Key())
			if res < 0 {
				iter.turn = true
			} else if res > 0 {
				iter.turn = false
			} else {
				iter.turn = true
				iter.validCold = iter.iterCold.Next()
			}
		} else if iter.validHot {
			iter.turn = true
		} else if iter.validCold {
			iter.turn = false
		}
		return iter.validHot || iter.validCold
	}
	if iter.turn {
		iter.validHot = iter.iterHot.Next()
	} else {
		iter.validCold = iter.iterCold.Next()
	}

	if iter.validHot && iter.validCold {
		res := bytes.Compare(iter.iterHot.Key(), iter.iterCold.Key())
		if res > 0 {
			iter.turn = false
		} else if res < 0 {
			iter.turn = true
		} else {
			if iter.turn {
				iter.validCold = iter.iterCold.Next()
			} else {
				iter.validHot = iter.iterHot.Next()
			}
			if iter.validHot {
				iter.turn = true
			} else {
				iter.turn = false
			}
		}
	} else if iter.validHot {
		iter.turn = true
	} else if iter.validCold {
		iter.turn = false
	}

	return iter.validHot || iter.validCold
}

// Error returns any accumulated error. Exhausting all the key/value pairs
// is not considered to be an error.
func (iter *pebbleIterator) Error() error {
	return iter.iterHot.Error()
}

// Key returns the key of the current key/value pair, or nil if done. The caller
// should not modify the contents of the returned slice, and its contents may
// change on the next call to Next.
func (iter *pebbleIterator) Key() []byte {
	if iter.turn {
		return iter.iterHot.Key()
	}
	return iter.iterCold.Key()
}

// Value returns the value of the current key/value pair, or nil if done. The
// caller should not modify the contents of the returned slice, and its contents
// may change on the next call to Next.
func (iter *pebbleIterator) Value() []byte {
	if iter.turn {
		return iter.iterHot.Value()
	}
	return iter.iterCold.Value()
}

// Release releases associated resources. Release should always succeed and can
// be called multiple times without causing error.
func (iter *pebbleIterator) Release() {
	if !iter.released {
		iter.iterHot.Close()
		// iter.iterCold.Release()
		iter.iterCold.Close()
		iter.released = true
	}
}

// Release releases associated resources. Release should always succeed and can
// be called multiple times without causing error.
func (snap *snapshot) Release() {
	snap.dbHot.Close()
	// snap.dbCold.Release()
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
// method to read everything there is to read independent of Pebble version.
//
// The property is unused in Pebble as there's only one thing to retrieve.
func (d *Database) Stat(property string) (string, error) {
	return d.dbHot.Metrics().String(), nil
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
	if limit == nil {
		limit = bytes.Repeat([]byte{0xff}, 32)
	}
	return d.dbHot.Compact(start, limit, true) // Parallelization is preferred
}

// Path returns the path to the database directory.
func (d *Database) Path() string {
	return d.fn
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

			stats              = d.dbHot.Metrics()
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

func isPutFile(idx int) bool {
	var skeletonIdx = []int{37, 38}
	for _, i := range skeletonIdx {
		if i == idx%100 {
			return true
		}
	}
	return false
}

func isPutColdDB(idx int) bool {
	var trieIdx = []int{40, 41, 42}
	var snapshotIdx = []int{27, 28, 29, 30, 31, 32}
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

func isGetColdDB(idx int) bool {
	var trieIdx = []int{54, 55, 56}
	var snapshotIdx = []int{39, 40, 41, 42, 43, 44}
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

func isGetFile(idx int) bool {
	var skeletonIdx = []int{52}
	for _, i := range skeletonIdx {
		if i == idx%100 && idx/100 != 1 {
			return true
		}
	}
	return false
}

func isHasHDD(idx int) bool {
	var trieIdx = []int{7, 8, 9}
	for _, i := range trieIdx {
		if i == idx {
			return true
		}
	}
	return false
}

func encodeBlockNumber(number uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, number)
	return enc
}

func decodeBlockNumber(enc []byte) uint64 {
	return binary.BigEndian.Uint64(enc)
}

// func keySkeletonHeader(key []byte) uint64 {
// 	return decodeBlockNumber(key[1:])
// }

func skeletonHeaderKey(number uint64) []byte {
	return append([]byte("S"), encodeBlockNumber(number)...)
}
