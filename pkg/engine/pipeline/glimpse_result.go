package pipeline

// GlimpseFieldInfo holds per-field statistics collected by glimpse.
type GlimpseFieldInfo struct {
	Name        string           `json:"name"`
	Type        string           `json:"type"`
	CoveragePct float64          `json:"coverage_pct"`
	NullPct     float64          `json:"null_pct"`
	Cardinality int              `json:"cardinality,omitempty"`
	TopValues   []GlimpseValue   `json:"top_values,omitempty"`
	NumStats    *GlimpseNumStats `json:"num_stats,omitempty"`
}

// GlimpseValue represents a top value with its count and percentage.
type GlimpseValue struct {
	Value string  `json:"value"`
	Count int     `json:"count"`
	Pct   float64 `json:"pct"`
}

// GlimpseNumStats holds numeric field statistics.
type GlimpseNumStats struct {
	Min float64 `json:"min"`
	P50 float64 `json:"p50"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

// GlimpseResult is the structured output of the glimpse operator.
type GlimpseResult struct {
	Fields    []GlimpseFieldInfo `json:"fields"`
	Sampled   int                `json:"sampled"`
	ElapsedMs int64              `json:"elapsed_ms"`
}
