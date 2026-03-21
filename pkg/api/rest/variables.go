package rest

import (
	"sort"
	"strconv"
	"strings"
)

// substituteVariables replaces $key tokens in the query with quoted, escaped
// values from the provided variables map. Keys are sorted by length descending
// so that $source_type is replaced before $source. Values are escaped using
// strconv.Quote for correct handling of all special characters (backslashes,
// newlines, tabs, control characters, Unicode).
func substituteVariables(query string, vars map[string]string) string {
	if len(vars) == 0 {
		return query
	}

	// Collect and sort keys by length descending to prevent partial matches.
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return len(keys[i]) > len(keys[j])
	})

	for _, k := range keys {
		// Use strconv.Quote for proper escaping, then extract the inner
		// content (without outer quotes) and wrap in our own quotes.
		quoted := strconv.Quote(vars[k])
		escaped := quoted[1 : len(quoted)-1]
		query = strings.ReplaceAll(query, "$"+k, `"`+escaped+`"`)
	}

	return query
}
