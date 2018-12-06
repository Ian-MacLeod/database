package btree

import (
	"sync"
)

type LeafNode struct {
	Keys    []string
	Values  []string
	Next    *LeafNode
	MaxKeys int
	Mux     sync.RWMutex
}

type IntermediateNode struct {
	Keys     []string
	Children []Node
	MaxKeys  int
	Mux      sync.RWMutex
}

type InsertionResult struct {
	Left     Node
	Right    Node
	SplitKey string
	Created  bool
}

type Node interface {
	Find(findKey string, parentMux *sync.RWMutex) (string, bool)
	SafeUpsert(updateKey, value string) InsertionResult
	Split() (Node, Node, string)
	GetMux() *sync.RWMutex
	AcquireLockContext(updateKey string, lockContext *LockContext)
	IsStable() bool
}

func (result *InsertionResult) DidSplit() bool {
	return result.Left != nil
}

func (node *IntermediateNode) GetMux() *sync.RWMutex {
	return &node.Mux
}

func (node *LeafNode) GetMux() *sync.RWMutex {
	return &node.Mux
}

func (node *IntermediateNode) Find(findKey string, parentMux *sync.RWMutex) (value string, ok bool) {
	node.Mux.RLock()
	parentMux.RUnlock()
	idx := node.indexContaining(findKey)
	return node.Children[idx].Find(findKey, &node.Mux)
}

func (node *LeafNode) Find(findKey string, parentMux *sync.RWMutex) (value string, ok bool) {
	node.Mux.RLock()
	defer node.Mux.RUnlock()
	parentMux.RUnlock()
	for idx, key := range node.Keys {
		if findKey == key {
			return node.Values[idx], true
		}
	}
	return "", false
}

func (node *IntermediateNode) IsStable() bool {
	return len(node.Keys) < node.MaxKeys
}

func (node *LeafNode) IsStable() bool {
	return len(node.Keys) < node.MaxKeys
}

func (node *IntermediateNode) AcquireLockContext(updateKey string, lockContext *LockContext) {
	lockContext.Add(node)

	idx := node.indexContaining(updateKey)
	node.Children[idx].AcquireLockContext(updateKey, lockContext)
}

func (node *LeafNode) AcquireLockContext(updateKey string, lockContext *LockContext) {
	lockContext.Add(node)
}

/*
SafeUpsert assumes that the caller holds write locks on the node itself, and all its
ancestors until a stable node is reached.
*/
func (node *IntermediateNode) SafeUpsert(updateKey, value string) InsertionResult {
	idx := node.indexContaining(updateKey)
	child := node.Children[idx]
	child.GetMux().Lock()
	defer child.GetMux().Unlock()
	result := child.SafeUpsert(updateKey, value)
	if !result.DidSplit() {
		return result
	}
	node.Keys = insert(node.Keys, idx, result.SplitKey)
	node.Children[idx] = result.Left
	node.Children = insertNode(node.Children, idx+1, result.Right)
	if len(node.Keys) > node.MaxKeys {
		left, right, splitKey := node.Split()
		return InsertionResult{left, right, splitKey, result.Created}
	}
	return InsertionResult{Created: result.Created}
}

/*
SafeUpsert assumes that the caller holds write locks on the node itself, and all its
ancestors until a stable node is reached.
*/
func (node *LeafNode) SafeUpsert(updateKey, value string) InsertionResult {
	idx := 0
	for idx < len(node.Keys) && updateKey > node.Keys[idx] {
		idx++
	}
	if idx != len(node.Keys) && updateKey == node.Keys[idx] {
		node.Values[idx] = value
		return InsertionResult{Created: false}
	}
	node.Keys = insert(node.Keys, idx, updateKey)
	node.Values = insert(node.Values, idx, value)
	if len(node.Keys) > node.MaxKeys {
		left, right, splitKey := node.Split()
		return InsertionResult{left, right, splitKey, true}
	}
	return InsertionResult{Created: true}
}

func (node *IntermediateNode) indexContaining(findKey string) int {
	idx := 0
	for idx < len(node.Keys) && findKey >= node.Keys[idx] {
		idx++
	}
	return idx
}

func (node *LeafNode) Split() (Node, Node, string) {
	rightKeys := make([]string, len(node.Keys)-len(node.Keys)/2, node.MaxKeys)
	copy(rightKeys, node.Keys[len(node.Keys)/2:])
	rightValues := make([]string, len(node.Values)-len(node.Values)/2, node.MaxKeys+1)
	copy(rightValues, node.Values[len(node.Values)/2:])
	right := LeafNode{
		Keys:    rightKeys,
		Values:  rightValues,
		Next:    node.Next,
		MaxKeys: node.MaxKeys,
	}
	left := LeafNode{
		Keys:    node.Keys[:len(node.Keys)/2],
		Values:  node.Values[:len(node.Values)/2],
		Next:    &right,
		MaxKeys: node.MaxKeys,
	}
	return &left, &right, right.Keys[0]
}

func (node *IntermediateNode) Split() (Node, Node, string) {
	medianIndex := len(node.Keys) / 2
	splitKey := node.Keys[medianIndex]
	rightKeys := make([]string, len(node.Keys)-medianIndex-1, node.MaxKeys)
	copy(rightKeys, node.Keys[medianIndex+1:])
	rightChildren := make([]Node, len(node.Children)-medianIndex-1, node.MaxKeys+1)
	copy(rightChildren, node.Children[medianIndex+1:])
	right := IntermediateNode{
		Keys:     rightKeys,
		Children: rightChildren,
		MaxKeys:  node.MaxKeys,
	}
	left := IntermediateNode{
		Keys:     node.Keys[:medianIndex],
		Children: node.Children[:medianIndex+1],
		MaxKeys:  node.MaxKeys,
	}
	return &left, &right, splitKey
}
