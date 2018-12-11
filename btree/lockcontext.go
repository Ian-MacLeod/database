package btree

import "sync"

type LockContext struct {
	Muxes          []*sync.RWMutex
	StableAncestor Node
	Manager        TransactionManager
}

func NewLockContext(tree *BTree, manager TransactionManager) LockContext {
	tree.mux.RLock()
	return LockContext{
		Muxes:   []*sync.RWMutex{&tree.mux},
		Manager: manager,
	}
}

func (ctx *LockContext) UpdateStableAncestor(ancestor Node) {
	for _, mux := range ctx.Muxes[:len(ctx.Muxes)-2] {
		ctx.Manager.RUnlock(mux)
	}
	ctx.Muxes = ctx.Muxes[len(ctx.Muxes)-2:]

	ctx.StableAncestor = ancestor
}

func (ctx *LockContext) Add(node Node) {
	ctx.Manager.RLock(node.GetMux())
	ctx.Muxes = append(ctx.Muxes, node.GetMux())

	if node.IsStable() {
		ctx.UpdateStableAncestor(node)
	}
}

func (ctx *LockContext) Resolve() (Node, *sync.RWMutex) {
	for _, mux := range ctx.Muxes[1:] {
		ctx.Manager.RUnlock(mux)
	}
	return ctx.StableAncestor, ctx.Muxes[0]
}
