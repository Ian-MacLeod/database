package btree

import (
	"sync"
)

type TransactionManager struct {
	Locks map[*sync.RWMutex]struct{}
}

func NewTransactionManager() TransactionManager {
	return TransactionManager{
		Locks: make(map[*sync.RWMutex]struct{}),
	}
}

func (manager *TransactionManager) has(mux *sync.RWMutex) bool {
	_, ok := manager.Locks[mux]
	return ok
}

func (manager *TransactionManager) RLock(mux *sync.RWMutex) {
	if !manager.has(mux) {
		mux.RLock()
	}
}

func (manager *TransactionManager) RUnlock(mux *sync.RWMutex) {
	if !manager.has(mux) {
		mux.RUnlock()
	}
}

func (manager *TransactionManager) Add(mux *sync.RWMutex) {
	manager.RLock(mux)
	manager.Locks[mux] = struct{}{}
}

func (manager *TransactionManager) RRelease(mux *sync.RWMutex) {
	for mux := range manager.Locks {
		mux.RUnlock()
	}
}
