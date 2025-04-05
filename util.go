package vectors

func Similarity(v1, v2 Vector) float32 {
	if len(v1) != len(v2) {
		panic("vectors have different lengths")
	}

	tmp := vectorsPool.Get(len(v1))

	norm1 := vectorNorm(v1, *tmp)
	norm2 := vectorNorm(v2, *tmp)

	s := cosineSimilarity(v1, v2, norm1, norm2, *tmp)

	vectorsPool.Put(tmp)

	return float32(s)
}
