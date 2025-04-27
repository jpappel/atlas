package query

import "errors"

// not threadsafe implementation of stack
type nodeStack struct {
	buf []*Node
}

func (s nodeStack) Push(n *Node) {
	s.buf = append(s.buf, n)
}
func (s nodeStack) Pop() *Node {
	last_index := len(s.buf) - 1
	n := s.buf[last_index]
	s.buf = s.buf[:last_index]
	return n
}
func (s nodeStack) Peek() *Node {
	return s.buf[len(s.buf)-1]
}
func (s nodeStack) IsEmpty() bool {
	return len(s.buf) == 0
}

type nodeQueue struct {
	buf  []*Node
	head int
	tail int
}

func makeNodeQueue(initial *Node, cap int) nodeQueue {
	if cap < 1 {
		panic("Invalid nodeQueue Capacity")
	}

	q := nodeQueue{
		buf:  make([]*Node, 0, cap),
		head: 0,
		tail: 1,
	}
	q.buf[0] = initial

	return q
}

func (q nodeQueue) Enqueue(n *Node) error {

	q.buf[q.tail] = n
	new_tail := (q.tail + 1) % len(q.buf)
	if new_tail == q.head {
		return errors.New("Queue out of capacity")
	}

	q.tail = new_tail
	return nil
}
func (q nodeQueue) Dequeue() (*Node, error) {
	if q.head == q.tail {
		return nil, errors.New("Empty Queue")
	}

	n := q.buf[q.head]
	q.head = (q.head + 1) % len(q.buf)
	return n, nil
}
func (q nodeQueue) PeekHead() (*Node, error) {
	if q.head == q.tail {
		return nil, errors.New("Empty queue")
	}
	return q.buf[q.head], nil
}
func (q nodeQueue) PeekTail() (*Node, error) {
	if q.head == q.tail {
		return nil, errors.New("Empty Queue")
	}
	return q.buf[q.tail-1], nil
}
func (q nodeQueue) IsEmpty() bool {
	return q.head == q.tail
}
