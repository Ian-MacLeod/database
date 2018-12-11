package btree

import (
	"sync"
)

type WriteTransactionManager struct {
	Locks map[*sync.RWMutex]struct{}
	Tree  *BTree
}

func NewWriteTransactionManager(tree *BTree) WriteTransactionManager {
	return WriteTransactionManager{
		Locks: make(map[*sync.RWMutex]struct{}),
		Tree:  tree,
	}
}

func (manager *WriteTransactionManager) has(mux *sync.RWMutex) bool {
	_, ok := manager.Locks[mux]
	return ok
}

func (manager *WriteTransactionManager) RLock(mux *sync.RWMutex) {
	if !manager.has(mux) {
		mux.RLock()
	}
}

func (manager *WriteTransactionManager) RUnlock(mux *sync.RWMutex) {
	if !manager.has(mux) {
		mux.RUnlock()
	}
}

func (manager *WriteTransactionManager) Lock(mux *sync.RWMutex) {
	if !manager.has(mux) {
		mux.Lock()
	}
}

func (manager *WriteTransactionManager) Unlock(mux *sync.RWMutex) {
	if !manager.has(mux) {
		mux.Unlock()
	}
}

func (manager *WriteTransactionManager) Add(mux *sync.RWMutex) {
	if !manager.has(mux) {
		mux.Lock()
		manager.Locks[mux] = struct{}{}
	}
}

func (manager *WriteTransactionManager) AddLocked(mux *sync.RWMutex) {
	if !manager.has(mux) {
		manager.Locks[mux] = struct{}{}
	}
}

func (manager *WriteTransactionManager) Find(key string) (value string, ok bool) {
	return manager.Tree.TransactionFind(manager, key)
}

func (manager *WriteTransactionManager) Upsert(key, value string) (created bool) {
	return manager.Tree.TransactionUpsert(manager, key, value)
}

func (manager *WriteTransactionManager) End() {
	for mux := range manager.Locks {
		mux.Unlock()
	}
	manager.Locks = make(map[*sync.RWMutex]struct{})
}
