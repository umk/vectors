package vectors

type Option[V any] func(*Database[V])

func WithRepackDivisor[V any](divisor int) Option[V] {
	return func(db *Database[V]) {
		db.repackDivisor = divisor
	}
}
