package heaputils

import "container/heap"

type SliceHeap[T SliceHeapItem[T]] []T

type SliceHeapItem[T any] interface {
	Less(another T) bool
}

func (h SliceHeap[T]) Len() int           { return len(h) }
func (h SliceHeap[T]) Less(i, j int) bool { return h[i].Less(h[j]) }
func (h SliceHeap[T]) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *SliceHeap[T]) Push(x any) {
	*h = append(*h, x.(T))
}

func (h *SliceHeap[T]) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type LimitHeap[T SliceHeapItem[T]] SliceHeap[T]

func MakeLimitHeap[T SliceHeapItem[T]](n int) LimitHeap[T] {
	h := make(LimitHeap[T], 0, n)
	heap.Init((*SliceHeap[T])(&h))
	return h
}

func (h *LimitHeap[T]) Push(item T) {
	if len(*h) < cap(*h) {
		heap.Push((*SliceHeap[T])(h), item)
	} else {
		(*h)[0] = item
		heap.Fix((*SliceHeap[T])(h), 0)
	}
}
