package pipeline

// Vectorized evaluation functions for batch-level arithmetic.

// AddInt64Columns adds two int64 columns element-wise.
func AddInt64Columns(a, b []int64) []int64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	result := make([]int64, n)
	for i := 0; i < n; i++ {
		result[i] = a[i] + b[i]
	}

	return result
}

// AddFloat64Columns adds two float64 columns element-wise.
func AddFloat64Columns(a, b []float64) []float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	result := make([]float64, n)
	for i := 0; i < n; i++ {
		result[i] = a[i] + b[i]
	}

	return result
}
