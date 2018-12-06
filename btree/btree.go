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

func (tree *BTree) Find(key string) (value string, ok bool) {
	tree.mux.RLock()
	return tree.Root.Find(key, &tree.mux)
}

func (tree *BTree) GetStableAncestor(key string) (Node, *sync.RWMutex) {
	lockContext := NewLockContext(tree)
	tree.Root.AcquireLockContext(key, &lockContext)
	return lockContext.Resolve()
}

func (tree *BTree) Upsert(key, value string) (created bool) {
	stableAncestor, parentMux := tree.GetStableAncestor(key)

	if stableAncestor == nil {
		parentMux.RUnlock()
		tree.mux.Lock()
		defer tree.mux.Unlock()
		stableAncestor = tree.Root
		stableAncestor.GetMux().Lock()
		defer stableAncestor.GetMux().Unlock()
	} else {
		stableAncestor.GetMux().Lock()
		if !stableAncestor.IsStable() {
			parentMux.RUnlock()
			stableAncestor.GetMux().Unlock()
			return tree.Upsert(key, value)
		}
		defer stableAncestor.GetMux().Unlock()
		defer parentMux.RUnlock()
	}

	result := stableAncestor.SafeUpsert(key, value)
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

func (tree *BTree) PrepareFind(manager *TransactionManager, key string) {
	manager.RLock(&tree.mux)
	tree.Root.PrepareFind(manager, key, &tree.mux)
}
