package vectors

import (
	"slices"
	"sync"

	"github.com/umk/vectors/internal/heaputils"
)

type ID int64

type Vector []float32

type Vectors struct {
	chunkSize int

	chunks       []*vectorsChunk
	currentChunk *vectorsChunk
}

func NewVectors(chunkSize int) *Vectors {
	vectors := &Vectors{
		chunkSize: chunkSize,
		chunks:    make([]*vectorsChunk, 1, 32),
	}

	currentChunk := newChunk(ID(0), chunkSize)

	vectors.chunks[0] = currentChunk
	vectors.currentChunk = currentChunk

	return vectors
}

func (v *Vectors) Add(vector Vector) ID {
	currentChunk := v.currentChunk

	for {
		id := currentChunk.add(vector)
		if id >= 0 {
			return id
		}

		if v.currentChunk == currentChunk {
			baseID := ID(len(v.chunks) * v.chunkSize)

			currentChunk = newChunk(baseID, v.chunkSize)

			id := v.currentChunk.add(vector)

			v.chunks = append(v.chunks, v.currentChunk)
			v.currentChunk = currentChunk

			return id
		}

		currentChunk = v.currentChunk
	}
}

func (v *Vectors) Delete(id ID) bool {
	i, _ := slices.BinarySearchFunc(v.chunks, id, searchChunk)
	return v.chunks[i].delete(id)
}

func (v *Vectors) Get(vectors []Vector, n int) []ID {
	h := v.getHeaps(vectors, n)
	r := reduceHeaps(h, n)

	ids := make([]ID, len(r))
	for i, hr := range r {
		ids[i] = hr.record.id
	}

	return ids
}

func (v *Vectors) Compact() {
	var destIndex, destRecordIndex int
	destChunk := v.chunks[destIndex]

	// Compact records by iterating over all chunks and their records.
	for _, srcChunk := range v.chunks {
		for _, record := range srcChunk.records {
			if record == nil {
				continue
			}

			// When destination chunk is full, move to next and reset index.
			if destRecordIndex == cap(destChunk.records) {
				destIndex++
				destChunk = v.chunks[destIndex]
				destRecordIndex = 0
			}

			// Write the valid record to the destination.
			destChunk.records[destRecordIndex] = record
			// Set new baseID for the chunk when writing its first record.
			if destRecordIndex == 0 {
				destChunk.baseID = record.id
			}
			destRecordIndex++
		}
	}

	v.currentChunk = destChunk

	// Nil out chunks that are no longer used.
	for i := destIndex + 1; i < len(v.chunks); i++ {
		v.chunks[i] = nil
	}
	v.chunks = v.chunks[:destIndex+1]

	// Clear trailing nil records from the destination chunk.
	for i := destRecordIndex; i < len(destChunk.records); i++ {
		destChunk.records[i] = nil
	}
	destChunk.records = destChunk.records[:destRecordIndex]
}

func (v *Vectors) Repack() *Vectors {
	// Create a new Vectors instance to hold compacted records.
	vectors := &Vectors{
		chunkSize: v.chunkSize,
		chunks:    make([]*vectorsChunk, 1, 32),
	}

	// Initialize the first destination chunk.
	destChunk := &vectorsChunk{
		records: make([]*chunkRecord, 0, v.chunkSize),
	}
	vectors.chunks[0] = destChunk

	// Iterate over all existing chunks.
	for _, srcChunk := range v.chunks {
		// Iterate over all records in the current source chunk.
		for _, record := range srcChunk.records {
			if record == nil {
				continue
			}

			// If the current destination chunk is full, allocate a new one.
			if len(destChunk.records) == cap(destChunk.records) {
				destChunk = &vectorsChunk{
					records: make([]*chunkRecord, 0, v.chunkSize),
				}
				vectors.chunks = append(vectors.chunks, destChunk)
			}
			// Set the baseID for a chunk when inserting its first record.
			if len(destChunk.records) == 0 {
				destChunk.baseID = record.id
			}

			// Append the valid record.
			destChunk.records = append(destChunk.records, record)
		}
	}

	// Update the current chunk pointer.
	vectors.currentChunk = destChunk

	// Optionally: review memory usage / add logging if needed.
	return vectors
}

func (v *Vectors) getHeaps(vectors []Vector, n int) <-chan maxDistanceHeap {
	out := make(chan maxDistanceHeap)

	go func() {
		defer close(out)

		var wg sync.WaitGroup

		for _, vector := range vectors {
			tmp := vectorsPool.Get(len(vector))
			norm := vectorNorm(vector, *tmp)
			vectorsPool.Put(tmp)

			for i := range v.chunks {
				wg.Add(1)
				go func(chunk *vectorsChunk) {
					defer wg.Done()

					out <- v.getByChunk(chunk, vector, n, norm)
				}(v.chunks[i])
			}
		}

		wg.Wait()
	}()

	return out
}

func (v *Vectors) getByChunk(
	chunk *vectorsChunk, vector Vector, n int, norm float64,
) maxDistanceHeap {
	dh := heaputils.MakeLimitHeap[*maxDistanceHeapItem](n)

	tmp := vectorsPool.Get(len(vector))

	count := len(chunk.records)
	for i := range count {
		r := chunk.records[i]

		if r == nil {
			continue
		}

		s := cosineSimilarity(vector, r.vector, norm, r.norm, *tmp)
		dh.Push(&maxDistanceHeapItem{record: r, similarity: s})
	}

	vectorsPool.Put(tmp)

	return dh
}

func reduceHeaps(in <-chan maxDistanceHeap, n int) maxDistanceHeap {
	out := make(chan maxDistanceHeap, 1)

	go func() {
		defer close(out)

		h := make(maxDistanceHeap, 0, n)
		for cur := range in {
			for _, r := range cur {
				h.Push(r)
			}
		}

		out <- h
	}()

	return <-out
}

func searchChunk(c *vectorsChunk, id ID) int {
	return int(id - c.baseID)
}
