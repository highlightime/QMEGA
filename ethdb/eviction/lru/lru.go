package lru

import (
	"container/list"
	"fmt"
)

type Eviction struct {
	lruList list.List
	elemmap map[string]*list.Element
}

func New() *Eviction {
	ret := Eviction{}
	ret.elemmap = make(map[string]*list.Element)
	return &ret
}

func (e *Eviction) SelectVictim() ([]byte, bool) {
	elem := e.lruList.Back()
	if elem == nil {
		return nil, false
	}

	return []byte(elem.Value.(string)), true
}
func (e *Eviction) Access(key []byte) bool {
	strKey := string(key)
	elem := e.elemmap[strKey]
	if elem == nil {
		return false
	}

	e.lruList.MoveToBack(elem)
	return true

}
func (e *Eviction) Pop() ([]byte, bool) {
	elem := e.lruList.Back()
	if elem == nil {
		return nil, false
	}

	strKey := elem.Value.(string)
	e.lruList.Remove(elem)
	delete(e.elemmap, strKey)
	return []byte(strKey), true
}
func (e *Eviction) Push(key []byte) bool {
	strKey := string(key)
	elem := e.lruList.PushBack(strKey)
	fmt.Print(strKey, " ", elem)
	e.elemmap[strKey] = elem
	return true

}
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
