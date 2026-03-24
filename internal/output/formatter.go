// Package output provides formatters for query results.
package output

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/lynxbase/lynxdb/internal/ui"
)

// Format represents an output format.
type Format string

const (
	FormatAuto     Format = "auto"
	FormatTable    Format = "table"
	FormatJSON     Format = "json"
	FormatNDJSON   Format = "ndjson" // Alias for JSON — JSONFormatter already outputs NDJSON.
	FormatCSV      Format = "csv"
	FormatTSV      Format = "tsv"
	FormatRaw      Format = "raw"
	FormatVertical Format = "vertical"
)

// Formatter writes query results to an output writer.
type Formatter interface {
	Format(w io.Writer, rows []map[string]interface{}) error
}

// isTTY returns true if f is a terminal.
func isTTY(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}

	return (fi.Mode() & os.ModeCharDevice) != 0
}

// DetectFormat chooses the best format based on context.
// When theme is non-nil, styled headers and separators are used in table output.
func DetectFormat(format Format, rows []map[string]interface{}, theme ...*ui.Theme) Formatter {
	var t *ui.Theme
	if len(theme) > 0 {
		t = theme[0]
	}

	// Detect glimpse output (has __glimpse_result column).
	if isGlimpseResult(rows) {
		switch format {
		case FormatJSON, FormatNDJSON:
			return &GlimpseJSONFormatter{}
		case FormatCSV:
			return &GlimpseCSVFormatter{}
		default:
			if !isTTY(os.Stdout) {
				return &GlimpseJSONFormatter{}
			}

			return &GlimpseTTYFormatter{}
		}
	}

	switch format {
	case FormatTable:
		if len(rows) == 1 && len(rows[0]) == 1 {
			if _, ok := rows[0]["_raw"]; ok {
				return &SingleValueFormatter{}
			}
		}

		return &TableFormatter{Theme: t}
	case FormatJSON, FormatNDJSON:
		return &JSONFormatter{}
	case FormatCSV:
		return &CSVFormatter{}
	case FormatTSV:
		return &TSVFormatter{}
	case FormatRaw:
		return &RawFormatter{}
	case FormatVertical:
		return &VerticalFormatter{}
	default: // auto
		if !isTTY(os.Stdout) {
			return &JSONFormatter{}
		}
		if len(rows) == 1 && len(rows[0]) == 1 {
			return &SingleValueFormatter{}
		}
		// Auto-detect vertical format for wide tables.
		if len(rows) > 0 {
			cols := collectColumns(rows)
			estimatedWidth := 0
			for _, col := range cols {
				estimatedWidth += len(col) + 2 // column name + separator
			}
			// If estimated table width exceeds typical terminal width, use vertical.
			if estimatedWidth > 120 && len(rows) <= 10 {
				return &VerticalFormatter{}
			}
		}

		return &TableFormatter{Theme: t}
	}
}

// isGlimpseResult checks if rows contain glimpse output.
func isGlimpseResult(rows []map[string]interface{}) bool {
	if len(rows) != 1 {
		return false
	}
	_, ok := rows[0]["__glimpse_result"]

	return ok
}

// TableFormatter outputs aligned table format using lipgloss/table.
// When Theme is set, headers are styled bold and separators use "─" characters.
type TableFormatter struct {
	Theme *ui.Theme
}

func (f *TableFormatter) Format(w io.Writer, rows []map[string]interface{}) error {
	if len(rows) == 0 {
		fmt.Fprintln(w, "No results.")

		return nil
	}

	// Collect columns in deterministic order.
	cols := collectColumns(rows)

	// Build a themed table. Use a plain theme when none provided.
	theme := f.Theme
	if theme == nil {
		theme = ui.NewTheme(w, true)
	}

	tbl := ui.NewTable(theme).SetColumns(cols...)

	for _, row := range rows {
		vals := make([]string, len(cols))
		for i, col := range cols {
			vals[i] = formatValue(row[col])
		}
		tbl.AddRow(vals...)
	}

	_, err := fmt.Fprint(w, tbl.String())

	return err
}

// JSONFormatter outputs newline-delimited JSON.
type JSONFormatter struct{}

func (f *JSONFormatter) Format(w io.Writer, rows []map[string]interface{}) error {
	enc := json.NewEncoder(w)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return err
		}
	}

	return nil
}

// CSVFormatter outputs RFC 4180 CSV.
type CSVFormatter struct{}

func (f *CSVFormatter) Format(w io.Writer, rows []map[string]interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	cols := collectColumns(rows)
	cw := csv.NewWriter(w)

	// Header.
	if err := cw.Write(cols); err != nil {
		return err
	}

	// Rows.
	record := make([]string, len(cols))
	for _, row := range rows {
		for i, col := range cols {
			record[i] = formatValue(row[col])
		}
		if err := cw.Write(record); err != nil {
			return err
		}
	}
	cw.Flush()

	return cw.Error()
}

// TSVFormatter outputs tab-separated values with a header row.
type TSVFormatter struct{}

func (f *TSVFormatter) Format(w io.Writer, rows []map[string]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	cols := collectColumns(rows)

	// Header.
	if _, err := fmt.Fprintln(w, strings.Join(cols, "\t")); err != nil {
		return err
	}

	// Rows.
	vals := make([]string, len(cols))
	for _, row := range rows {
		for i, col := range cols {
			vals[i] = formatValue(row[col])
		}

		if _, err := fmt.Fprintln(w, strings.Join(vals, "\t")); err != nil {
			return err
		}
	}

	return nil
}

// RawFormatter outputs raw text (one line per row, tab-separated).
type RawFormatter struct{}

func (f *RawFormatter) Format(w io.Writer, rows []map[string]interface{}) error {
	for _, row := range rows {
		if raw, ok := row["_raw"]; ok {
			s := formatValue(raw)
			if s != "" {
				fmt.Fprintln(w, s)
			} else {
				// Fallback for empty _raw (e.g., column pruning missed it
				// or segment reader returned an empty string for the field).
				writeFieldValueLine(w, row)
			}
		} else {
			writeFieldValueLine(w, row)
		}
	}

	return nil
}

// writeFieldValueLine writes a tab-separated field=value line for a row,
// sorted alphabetically for deterministic output.
func writeFieldValueLine(w io.Writer, row map[string]interface{}) {
	parts := make([]string, 0, len(row))
	for k, v := range row {
		parts = append(parts, k+"="+formatValue(v))
	}
	sort.Strings(parts)
	fmt.Fprintln(w, strings.Join(parts, "\t"))
}

// SingleValueFormatter outputs a single value with formatting.
type SingleValueFormatter struct{}

func (f *SingleValueFormatter) Format(w io.Writer, rows []map[string]interface{}) error {
	if len(rows) != 1 || len(rows[0]) != 1 {
		return (&TableFormatter{}).Format(w, rows)
	}
	for _, v := range rows[0] {
		fmt.Fprintln(w, formatValue(v))
	}

	return nil
}

// VerticalFormatter outputs results in vertical format (one field per line).
// Best for wide tables with few rows.
type VerticalFormatter struct{}

func (f *VerticalFormatter) Format(w io.Writer, rows []map[string]interface{}) error {
	if len(rows) == 0 {
		fmt.Fprintln(w, "No results.")

		return nil
	}

	allKeys := collectColumns(rows)
	maxLen := 0
	for _, k := range allKeys {
		if len(k) > maxLen {
			maxLen = len(k)
		}
	}

	for i, row := range rows {
		fmt.Fprintf(w, "  ─── Row %d ───\n", i+1)
		keys := collectColumns([]map[string]interface{}{row})
		for _, k := range keys {
			fmt.Fprintf(w, "  %*s: %s\n", maxLen, k, formatValue(row[k]))
		}
		if i < len(rows)-1 {
			fmt.Fprintln(w)
		}
	}

	return nil
}

// builtinFieldOrder defines the canonical display order for LynxDB internal
// fields. These always appear first (in this order) when present, followed
// by user-defined fields in alphabetical order. This guarantees deterministic
// column ordering across runs — Go map iteration is randomized, so we must
// never rely on insertion order.
var builtinFieldOrder = [...]string{
	"_time",
	"_raw",
	"index",
	"source",
	"_source",
	"sourcetype",
	"_sourcetype",
	"host",
}

// builtinFieldRank maps builtin field names to their sort priority.
// Lower rank = appears first. Populated once at package init time would be
// the obvious choice, but we avoid init() per project convention; a package-
// level var with a helper is equivalent and testable.
var builtinFieldRank = func() map[string]int {
	m := make(map[string]int, len(builtinFieldOrder))
	for i, name := range builtinFieldOrder {
		m[name] = i
	}

	return m
}()

// collectColumns extracts column names in a deterministic order:
//  1. LynxDB built-in fields in canonical order (_time, _raw, index, source, …)
//  2. User-defined (schema-on-read) fields in alphabetical order
func collectColumns(rows []map[string]interface{}) []string {
	seen := make(map[string]struct{})
	for _, row := range rows {
		for k := range row {
			seen[k] = struct{}{}
		}
	}

	// Partition into builtin (ordered) and user (alphabetical).
	builtins := make([]string, 0, len(builtinFieldOrder))
	user := make([]string, 0, len(seen))

	for col := range seen {
		if _, ok := builtinFieldRank[col]; ok {
			builtins = append(builtins, col)
		} else {
			user = append(user, col)
		}
	}

	// Sort builtins by their canonical rank.
	sort.Slice(builtins, func(i, j int) bool {
		return builtinFieldRank[builtins[i]] < builtinFieldRank[builtins[j]]
	})
	// Sort user fields alphabetically for determinism.
	sort.Strings(user)

	return append(builtins, user...)
}

func formatValue(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}

		return fmt.Sprintf("%.4g", val)
	default:
		return fmt.Sprint(v)
	}
}

// GlimpseTTYFormatter prints the pre-rendered ASCII table from _raw for TTY output.
type GlimpseTTYFormatter struct{}

func (f *GlimpseTTYFormatter) Format(w io.Writer, rows []map[string]interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	raw, ok := rows[0]["_raw"]
	if !ok {
		return nil
	}
	fmt.Fprintln(w, formatValue(raw))

	return nil
}

// GlimpseJSONFormatter outputs the structured glimpse result as JSON.
type GlimpseJSONFormatter struct{}

func (f *GlimpseJSONFormatter) Format(w io.Writer, rows []map[string]interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	raw, ok := rows[0]["__glimpse_result"]
	if !ok {
		// Fallback to raw.
		raw, ok = rows[0]["_raw"]
		if !ok {
			return nil
		}
		_, err := fmt.Fprintln(w, formatValue(raw))

		return err
	}

	// Pretty-print the structured JSON.
	var parsed interface{}
	s := fmt.Sprintf("%v", raw)
	if err := json.Unmarshal([]byte(s), &parsed); err != nil {
		_, err := fmt.Fprintln(w, s)

		return err
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	return enc.Encode(parsed)
}

// GlimpseCSVFormatter outputs the structured glimpse fields as CSV rows.
type GlimpseCSVFormatter struct{}

func (f *GlimpseCSVFormatter) Format(w io.Writer, rows []map[string]interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	raw, ok := rows[0]["__glimpse_result"]
	if !ok {
		raw = rows[0]["_raw"]
		_, err := fmt.Fprintln(w, formatValue(raw))

		return err
	}

	s := fmt.Sprintf("%v", raw)
	var result struct {
		Fields []struct {
			Name        string  `json:"name"`
			Type        string  `json:"type"`
			CoveragePct float64 `json:"coverage_pct"`
			NullPct     float64 `json:"null_pct"`
			Cardinality int     `json:"cardinality"`
		} `json:"fields"`
		Sampled   int   `json:"sampled"`
		ElapsedMs int64 `json:"elapsed_ms"`
	}
	if err := json.Unmarshal([]byte(s), &result); err != nil {
		_, err := fmt.Fprintln(w, formatValue(raw))

		return err
	}

	cw := csv.NewWriter(w)
	defer cw.Flush()
	_ = cw.Write([]string{"field", "type", "coverage_pct", "null_pct", "cardinality"})
	for _, f := range result.Fields {
		card := ""
		if f.Cardinality > 0 {
			card = fmt.Sprintf("%d", f.Cardinality)
		}
		_ = cw.Write([]string{
			f.Name,
			f.Type,
			fmt.Sprintf("%.1f", f.CoveragePct),
			fmt.Sprintf("%.1f", f.NullPct),
			card,
		})
	}

	return cw.Error()
}
