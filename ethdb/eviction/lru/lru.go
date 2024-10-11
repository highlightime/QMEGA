package lru

import (
	"container/list"
	"sync"
)

type LRUEntry struct {
	list_elem *list.Element
	size      int
}

type Eviction struct {
	lruList list.List
	elemmap map[string]LRUEntry
	mu      sync.Mutex
}

// Constructor
func New() *Eviction {
	ret := Eviction{}
	ret.elemmap = make(map[string]LRUEntry)
	return &ret
}

// Return evition victim (least recently used one) and true
// Return nil and false if there is no element in policy
func (e *Eviction) SelectVictim() ([]byte, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	elem := e.lruList.Front()
	if elem == nil {
		return nil, false
	}

	return []byte(elem.Value.(string)), true
}

// Update access information of key and return true if key exists
// Otherwise, return false
func (e *Eviction) Access(key []byte) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	strKey := string(key)
	entry, success := e.elemmap[strKey]
	if !success {
		return false
	}

	e.lruList.MoveToBack(entry.list_elem)
	return true
}

// Return eviction victim, true and the size of kv pair, and remove it from policy
// Return nil, false, and 0, if there is no element in policy
func (e *Eviction) Pop() ([]byte, bool, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	entry := e.lruList.Front()
	if entry == nil {
		return nil, false, 0
	}

	strKey := entry.Value.(string)
	e.lruList.Remove(entry)
	size := e.elemmap[strKey].size
	delete(e.elemmap, strKey)
	return []byte(strKey), true, size
}

// Number of keys in policy
func (e *Eviction) Len() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.elemmap)
}

// Add new key to policy and return true
// return false if key already exists
func (e *Eviction) Push(key []byte, size int) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	strKey := string(key)
	entry, success := e.elemmap[strKey]

	if success {
		entry.size = size
		e.elemmap[strKey] = entry
		e.lruList.MoveToBack(entry.list_elem)
		return false
	}
	list_elem := e.lruList.PushBack(strKey)
	e.elemmap[strKey] = LRUEntry{list_elem: list_elem, size: size}
	return true

}

// Remove key from policy and return true
// Return false if key does not exist
func (e *Eviction) Delete(key []byte) (bool, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	strKey := string(key)
	entry, success := e.elemmap[strKey]
	if !success {
		return false, 0
	}
	size := entry.size
	e.lruList.Remove(entry.list_elem)
	delete(e.elemmap, strKey)
	return true, size
}
