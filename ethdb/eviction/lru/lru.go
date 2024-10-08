package lru

import (
	"container/list"
)

type Eviction struct {
	lruList list.List
	elemmap map[string]*list.Element
}

// Constructor
func New() *Eviction {
	ret := Eviction{}
	ret.elemmap = make(map[string]*list.Element)
	return &ret
}

// Return evition victim (least recently used one) and true
// Return nil and false if there is no element in policy
func (e *Eviction) SelectVictim() ([]byte, bool) {
	elem := e.lruList.Front()
	if elem == nil {
		return nil, false
	}

	return []byte(elem.Value.(string)), true
}

// Update access information of key and return true if key exists
// Otherwise, return false
func (e *Eviction) Access(key []byte) bool {
	strKey := string(key)
	elem := e.elemmap[strKey]
	if elem == nil {
		return false
	}

	e.lruList.MoveToBack(elem)
	return true
}

// Return eviction victim  and true, and remove it from policy
// Return nil and false if there is no element in policy
func (e *Eviction) Pop() ([]byte, bool) {
	elem := e.lruList.Front()
	if elem == nil {
		return nil, false
	}

	strKey := elem.Value.(string)
	e.lruList.Remove(elem)
	delete(e.elemmap, strKey)
	return []byte(strKey), true
}

// Add new key to policy and return true
// return false if key already exists
func (e *Eviction) Push(key []byte) bool {
	strKey := string(key)
	val := e.elemmap[strKey]
	if val != nil {
		return false
	}
	elem := e.lruList.PushBack(strKey)
	e.elemmap[strKey] = elem
	return true

}

// Remove key from policy and return true
// Return false if key does not exist
func (e *Eviction) Delete(key []byte) bool {
	strKey := string(key)
	elem := e.elemmap[strKey]
	if elem == nil {
		return false
	}

	e.lruList.Remove(elem)
	delete(e.elemmap, strKey)
	return true
}
