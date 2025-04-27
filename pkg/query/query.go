package query


type Node struct {
	Parent   *Node
	Children []*Node
	Token
}

type AST struct {
	root Node
	size uint64
}

// Walk an ast depth first
func (T AST) dfWalk() func() (*Node, bool) {
	stack := nodeStack{make([]*Node, 0, T.size)}
	stack.Push(&T.root)

	return func() (*Node, bool) {
		n := stack.Pop()
		for _, child := range n.Children {
			stack.Push(child)
		}

		if stack.IsEmpty() {
			return n, false
		}
		return n, true
	}
}

// Walk an ast breadth first
func (T AST) bfWalk() func() (*Node, bool) {
	queue := nodeQueue{}
	queue.buf = make([]*Node, 0, T.size)
	queue.Enqueue(&T.root)

	return func() (*Node, bool) {
		n, err := queue.Dequeue()
		if err != nil {
			return nil, false
		}

		for _, child := range n.Children {
			queue.Enqueue(child)
		}
		return n, true
	}
}
