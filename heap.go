package vectors

import "github.com/umk/vectors/internal/heaputils"

// maxDistanceHeap implements a heap interface that compares vector
// records by the order of decreasing their similarity to another vector.
type maxDistanceHeap = heaputils.LimitHeap[*maxDistanceHeapItem]

type maxDistanceHeapItem struct {
	record     *chunkRecord
	similarity float64
}

func (i *maxDistanceHeapItem) Less(another *maxDistanceHeapItem) bool {
	return i.similarity > another.similarity
}
