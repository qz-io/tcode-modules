package model

import (
	"container/list"
	"sync"
)

type SafeList struct {
	m    sync.Mutex
	list *list.List
}

func NewSafeList() *SafeList {
	return &SafeList{
		list: list.New(),
	}
}

func (sl *SafeList) PushBack(value interface{}) {
	sl.m.Lock()
	defer sl.m.Unlock()
	sl.list.PushBack(value)
}

func (sl *SafeList) Len() int {
	sl.m.Lock()
	defer sl.m.Unlock()
	return sl.list.Len()
}

func (sl *SafeList) Front() *list.Element {
	sl.m.Lock()
	defer sl.m.Unlock()
	return sl.list.Front()
}

func (sl *SafeList) Back() *list.Element {
	sl.m.Lock()
	defer sl.m.Unlock()
	return sl.list.Back()
}

func (sl *SafeList) Remove(e *list.Element) any {
	sl.m.Lock()
	defer sl.m.Unlock()
	return sl.list.Remove(e)
}

func (sl *SafeList) Contains(value interface{}) bool {
	if sl == nil {
		return false
	}

	if sl.list.Len() == 0 {
		return false
	}

	for e := sl.Front(); e != nil; e = e.Next() {
		if e.Value == value {
			return true
		}
	}
	return false
}
