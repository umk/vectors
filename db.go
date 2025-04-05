package vectors

import (
	"sync"
)

type Database[V any] struct {
	mu sync.RWMutex

	vectors *Vectors
	Data    map[ID]V

	vectorLength  int
	repackDivisor int

	itemsCount   int
	deletesCount int

	repacking bool
}

type Record[V any] struct {
	ID     ID
	Vector Vector
	Data   V
}

func NewDatabase[V any](vectorLength int, options ...Option[V]) *Database[V] {
	db := &Database[V]{
		vectors:       NewVectors(128),
		Data:          make(map[ID]V),
		vectorLength:  vectorLength,
		repackDivisor: 10,
	}
	for _, option := range options {
		option(db)
	}
	return db
}

func (db *Database[V]) Add(record Record[V]) Record[V] {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(record.Vector) != db.vectorLength {
		panic("vector length mismatch")
	}

	record = db.addRecord(record)

	db.itemsCount++
	return record
}

func (db *Database[V]) AddBatch(records []Record[V]) []Record[V] {
	db.mu.Lock()
	defer db.mu.Unlock()

	for _, record := range records {
		if len(record.Vector) != db.vectorLength {
			panic("vector length mismatch")
		}
	}

	result := make([]Record[V], len(records))
	for i, record := range records {
		result[i] = db.addRecord(record)
	}

	db.itemsCount += len(records)
	return result
}

func (db *Database[V]) Delete(id ID) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.deleteRecord(id) {
		db.increaseDeleteCount(1)
	}
}

func (db *Database[V]) DeleteBatch(ids []ID) {
	db.mu.Lock()
	defer db.mu.Unlock()

	deletedCount := 0

	for _, id := range ids {
		if db.deleteRecord(id) {
			deletedCount++
		}
	}

	db.increaseDeleteCount(deletedCount)
}

func (db *Database[V]) Get(vectors []Vector, n int) []Record[V] {
	db.mu.RLock()
	defer db.mu.RUnlock()

	ids := db.vectors.Get(vectors, n)

	r := make([]Record[V], len(ids))
	for i, id := range ids {
		r[i] = Record[V]{
			ID:   id,
			Data: db.Data[id],
		}
	}

	return r
}

func (db *Database[V]) increaseDeleteCount(count int) {
	db.deletesCount += count

	if !db.repacking && db.deletesCount > (db.itemsCount/db.repackDivisor) {
		db.repacking = true
		go func(vectors *Vectors) {
			db.mu.RLock()

			db.vectors = vectors.Repack()

			db.itemsCount -= db.deletesCount
			db.deletesCount = 0

			db.repacking = false
			db.mu.RUnlock()
		}(db.vectors)
	}
}

func (db *Database[V]) addRecord(record Record[V]) Record[V] {
	record.ID = db.vectors.Add(record.Vector)
	record.Vector = nil

	db.Data[record.ID] = record.Data

	return record
}

func (db *Database[V]) deleteRecord(id ID) bool {
	if db.vectors.Delete(id) {
		delete(db.Data, id)
		return true
	}
	return false
}
