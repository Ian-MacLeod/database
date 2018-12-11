package btree

import (
	"sync"
)

type TransactionManager interface {
	Add(*sync.RWMutex)
	AddLocked(*sync.RWMutex)
	RLock(*sync.RWMutex)
	RUnlock(*sync.RWMutex)
	Lock(*sync.RWMutex)
	Unlock(*sync.RWMutex)
}

type ReadTransactionManager struct {
	Locks map[*sync.RWMutex]struct{}
	Tree  *BTree
}

func (manager *ReadTransactionManager) has(mux *sync.RWMutex) bool {
	_, ok := manager.Locks[mux]
	return ok
}

func (manager *ReadTransactionManager) RLock(mux *sync.RWMutex) {
	if !manager.has(mux) {
		mux.RLock()
	}
}

func (manager *ReadTransactionManager) RUnlock(mux *sync.RWMutex) {
	if !manager.has(mux) {
		mux.RUnlock()
	}
}

func (manager *ReadTransactionManager) Lock(mux *sync.RWMutex) {
	if !manager.has(mux) {
		mux.Lock()
	}
}

func (manager *ReadTransactionManager) Unlock(mux *sync.RWMutex) {
	if !manager.has(mux) {
		mux.Unlock()
	}
}

func (manager *ReadTransactionManager) Add(mux *sync.RWMutex) {
	if !manager.has(mux) {
		mux.RLock()
		manager.Locks[mux] = struct{}{}
	}
}

func (manager *ReadTransactionManager) AddLocked(mux *sync.RWMutex) {
	if !manager.has(mux) {
		manager.Locks[mux] = struct{}{}
	}
}

func (manager *ReadTransactionManager) Find(key string) (value string, ok bool) {
	return manager.Tree.TransactionFind(manager, key)
}

func (manager *ReadTransactionManager) End() {
	for mux := range manager.Locks {
		mux.RUnlock()
	}
	manager.Locks = make(map[*sync.RWMutex]struct{})
}
