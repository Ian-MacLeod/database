package btree

func insert(slice []string, idx int, value string) []string {
	slice = append(slice, "")
	copy(slice[idx+1:], slice[idx:])
	slice[idx] = value
	return slice
}

func insertNode(slice []Node, idx int, node Node) []Node {
	slice = append(slice, nil)
	copy(slice[idx+1:], slice[idx:])
	slice[idx] = node
	return slice
}
