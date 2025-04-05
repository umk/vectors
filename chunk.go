package vectors

import "slices"

type vectorsChunk struct {
	baseID  ID
	records []*chunkRecord
}

type chunkRecord struct {
	id     ID
	vector Vector
	norm   float64
}

func newChunk(baseID ID, chunkSize int) *vectorsChunk {
	return &vectorsChunk{
		baseID:  baseID,
		records: make([]*chunkRecord, 0, chunkSize),
	}
}

func (vc *vectorsChunk) add(vector []float32) ID {
	if len(vc.records) == cap(vc.records) {
		return -1
	}

	id := vc.baseID + ID(len(vc.records))

	tmp := vectorsPool.Get(len(vector))

	vc.records = append(vc.records, &chunkRecord{
		id:     id,
		vector: vector,
		norm:   vectorNorm(vector, *tmp),
	})

	vectorsPool.Put(tmp)

	return id
}

func (vc *vectorsChunk) delete(id ID) bool {
	if i, ok := slices.BinarySearchFunc(vc.records, id, searchRecord); ok {
		vc.records[i] = nil
		return true
	}
	return false
}

func searchRecord(r *chunkRecord, id ID) int {
	return int(id - r.id)
}
