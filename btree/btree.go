package btree

import (
	"sync"
)

type BTree struct {
	Root    Node
	maxKeys int
	mux     sync.RWMutex
}

func New(maxKeys int) BTree {
	return BTree{
		Root:    &LeafNode{MaxKeys: maxKeys},
		maxKeys: maxKeys,
	}
}

func (tree *BTree) Find(key string) (string, bool) {
	manager := tree.GetReadTransactionManager()
	value, ok := tree.TransactionFind(manager, key)
	manager.End()
	return value, ok
}

func (tree *BTree) GetStableAncestor(manager TransactionManager, key string) (Node, *sync.RWMutex) {
	lockContext := NewLockContext(tree, manager)
	tree.Root.AcquireLockContext(key, &lockContext)
	return lockContext.Resolve()
}

func (tree *BTree) Upsert(key, value string) bool {
	manager := tree.GetWriteTransactionManager()
	created := tree.TransactionUpsert(manager, key, value)
	manager.End()
	return created
}

func (tree *BTree) GetReadTransactionManager() *ReadTransactionManager {
	return &ReadTransactionManager{
		Locks: make(map[*sync.RWMutex]struct{}),
		Tree:  tree,
	}
}

func (tree *BTree) GetWriteTransactionManager() *WriteTransactionManager {
	return &WriteTransactionManager{
		Locks: make(map[*sync.RWMutex]struct{}),
		Tree:  tree,
	}
}

func (tree *BTree) TransactionFind(manager TransactionManager, key string) (value string, ok bool) {
	manager.RLock(&tree.mux)
	return tree.Root.Find(manager, key, &tree.mux)
}

func (tree *BTree) TransactionUpsert(manager TransactionManager, key, value string) (created bool) {
	stableAncestor, parentMux := tree.GetStableAncestor(manager, key)

	if stableAncestor == nil {
		manager.RUnlock(parentMux)
		manager.Lock(&tree.mux)
		defer manager.Unlock(&tree.mux)
		stableAncestor = tree.Root
		manager.Add(stableAncestor.GetMux())
	} else {
		manager.Lock(stableAncestor.GetMux())
		if !stableAncestor.IsStable() {
			manager.RUnlock(parentMux)
			manager.Unlock(stableAncestor.GetMux())
			return tree.TransactionUpsert(manager, key, value)
		}
		manager.AddLocked(stableAncestor.GetMux())
		defer manager.RUnlock(parentMux)
	}

	result := stableAncestor.SafeUpsert(manager, key, value)
	if result.DidSplit() {
		tree.Root = &IntermediateNode{
			Keys:     []string{result.SplitKey},
			Children: []Node{result.Left, result.Right},
			MaxKeys:  tree.maxKeys,
			Mux:      sync.RWMutex{},
		}
	}

	return result.Created
}
