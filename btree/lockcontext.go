package btree

import "sync"

type LockContext struct {
	Muxes          []*sync.RWMutex
	StableAncestor Node
}

func NewLockContext(tree *BTree) LockContext {
	tree.mux.RLock()
	return LockContext{
		Muxes: []*sync.RWMutex{&tree.mux},
	}
}

func (ctx *LockContext) UpdateStableAncestor(ancestor Node) {
	for _, mux := range ctx.Muxes[:len(ctx.Muxes)-2] {
		mux.RUnlock()
	}
	ctx.Muxes = ctx.Muxes[len(ctx.Muxes)-2:]

	ctx.StableAncestor = ancestor
}

func (ctx *LockContext) Add(node Node) {
	node.GetMux().RLock()
	ctx.Muxes = append(ctx.Muxes, node.GetMux())

	if node.IsStable() {
		ctx.UpdateStableAncestor(node)
	}
}

func (ctx *LockContext) Resolve() (Node, *sync.RWMutex) {
	for _, mux := range ctx.Muxes[1:] {
		mux.RUnlock()
	}
	return ctx.StableAncestor, ctx.Muxes[0]
}
