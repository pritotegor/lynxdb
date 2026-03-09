package query

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func TestPlanDistributedQuery_NilProgram(t *testing.T) {
	plan, err := PlanDistributedQuery(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Strategy != MergeConcat {
		t.Errorf("expected MergeConcat, got %v", plan.Strategy)
	}
}

func TestPlanDistributedQuery_EmptyProgram(t *testing.T) {
	prog := &spl2.Program{Main: &spl2.Query{}}
	plan, err := PlanDistributedQuery(prog)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Strategy != MergeConcat {
		t.Errorf("expected MergeConcat, got %v", plan.Strategy)
	}
}

func TestPlanDistributedQuery_SearchOnly(t *testing.T) {
	prog, err := spl2.ParseProgram(`search "error"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	plan, err := PlanDistributedQuery(prog)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Search is pushable — all commands go to shard, no coord commands.
	if plan.Strategy != MergeConcat {
		t.Errorf("expected MergeConcat, got %v", plan.Strategy)
	}
	if len(plan.ShardCommands) != 1 {
		t.Errorf("expected 1 shard command, got %d", len(plan.ShardCommands))
	}
	if len(plan.CoordCommands) != 0 {
		t.Errorf("expected 0 coord commands, got %d", len(plan.CoordCommands))
	}
}

func TestPlanDistributedQuery_StatsCount(t *testing.T) {
	prog, err := spl2.ParseProgram(`search "error" | stats count`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	plan, err := PlanDistributedQuery(prog)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.Strategy != MergePartialAgg {
		t.Errorf("expected MergePartialAgg, got %v", plan.Strategy)
	}
	if plan.PartialAggSpec == nil {
		t.Fatal("expected non-nil PartialAggSpec")
	}
	if len(plan.PartialAggSpec.Funcs) != 1 {
		t.Fatalf("expected 1 agg func, got %d", len(plan.PartialAggSpec.Funcs))
	}
	if plan.PartialAggSpec.Funcs[0].Name != "count" {
		t.Errorf("expected count, got %s", plan.PartialAggSpec.Funcs[0].Name)
	}
}

func TestPlanDistributedQuery_StatsCountByField(t *testing.T) {
	prog, err := spl2.ParseProgram(`search "error" | stats count by source`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	plan, err := PlanDistributedQuery(prog)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.Strategy != MergePartialAgg {
		t.Errorf("expected MergePartialAgg, got %v", plan.Strategy)
	}
	if len(plan.PartialAggSpec.GroupBy) == 0 {
		t.Error("expected non-empty group by")
	}
	if plan.PartialAggSpec.GroupBy[0] != "source" {
		t.Errorf("expected group by source, got %v", plan.PartialAggSpec.GroupBy)
	}
}

func TestPlanDistributedQuery_TopK(t *testing.T) {
	prog, err := spl2.ParseProgram(`search "error" | stats count by source | sort -count | head 10`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	plan, err := PlanDistributedQuery(prog)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.Strategy != MergeTopK {
		t.Errorf("expected MergeTopK, got %v", plan.Strategy)
	}
	if plan.TopK != 10 {
		t.Errorf("expected TopK=10, got %d", plan.TopK)
	}
	if len(plan.TopKSortFields) == 0 {
		t.Fatal("expected non-empty sort fields")
	}
	if plan.TopKSortFields[0].Name != "count" || !plan.TopKSortFields[0].Desc {
		t.Errorf("expected sort by count desc, got %+v", plan.TopKSortFields[0])
	}
	// sort+head should be consumed from coord commands.
	if len(plan.CoordCommands) != 0 {
		t.Errorf("expected 0 coord commands after TopK, got %d", len(plan.CoordCommands))
	}
}

func TestPlanDistributedQuery_SortIsCoordOnly(t *testing.T) {
	// Sort without stats should be coordinator-only.
	prog, err := spl2.ParseProgram(`search "error" | sort -_time`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	plan, err := PlanDistributedQuery(prog)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.Strategy != MergeConcat {
		t.Errorf("expected MergeConcat, got %v", plan.Strategy)
	}
	// Search is pushable but sort is not.
	if len(plan.ShardCommands) != 1 {
		t.Errorf("expected 1 shard commands, got %d", len(plan.ShardCommands))
	}
	if len(plan.CoordCommands) != 1 {
		t.Errorf("expected 1 coord commands, got %d", len(plan.CoordCommands))
	}
}

func TestPlanDistributedQuery_WhereEvalStats(t *testing.T) {
	prog, err := spl2.ParseProgram(`| where status >= 500 | eval svc=source | stats count by svc`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	plan, err := PlanDistributedQuery(prog)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.Strategy != MergePartialAgg {
		t.Errorf("expected MergePartialAgg, got %v", plan.Strategy)
	}
	// where + eval + stats = 3 pushable commands.
	if len(plan.ShardCommands) != 3 {
		t.Errorf("expected 3 shard commands, got %d", len(plan.ShardCommands))
	}
}

func TestPlanDistributedQuery_DedupIsCoordOnly(t *testing.T) {
	prog, err := spl2.ParseProgram(`search "error" | dedup host`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	plan, err := PlanDistributedQuery(prog)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// dedup is not pushable.
	if len(plan.ShardCommands) != 1 {
		t.Errorf("expected 1 shard command (search), got %d", len(plan.ShardCommands))
	}
	if len(plan.CoordCommands) != 1 {
		t.Errorf("expected 1 coord command (dedup), got %d", len(plan.CoordCommands))
	}
}

func TestIsPushable(t *testing.T) {
	tests := []struct {
		name     string
		cmd      spl2.Command
		pushable bool
	}{
		{"search", &spl2.SearchCommand{}, true},
		{"where", &spl2.WhereCommand{}, true},
		{"eval", &spl2.EvalCommand{}, true},
		{"rex", &spl2.RexCommand{}, true},
		{"bin", &spl2.BinCommand{}, true},
		{"fillnull", &spl2.FillnullCommand{}, true},
		{"rename", &spl2.RenameCommand{}, true},
		{"stats", &spl2.StatsCommand{}, true},
		{"top", &spl2.TopCommand{}, true},
		{"rare", &spl2.RareCommand{}, true},
		{"fields_keep", &spl2.FieldsCommand{Remove: false}, true},
		{"fields_remove", &spl2.FieldsCommand{Remove: true}, false},
		{"sort", &spl2.SortCommand{}, false},
		{"head", &spl2.HeadCommand{}, false},
		{"dedup", &spl2.DedupCommand{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPushable(tt.cmd); got != tt.pushable {
				t.Errorf("isPushable(%s) = %v, want %v", tt.name, got, tt.pushable)
			}
		})
	}
}

func TestMergeStrategy_String(t *testing.T) {
	if MergeConcat.String() != "concat" {
		t.Errorf("unexpected: %s", MergeConcat.String())
	}
	if MergePartialAgg.String() != "partial_agg" {
		t.Errorf("unexpected: %s", MergePartialAgg.String())
	}
	if MergeTopK.String() != "topk" {
		t.Errorf("unexpected: %s", MergeTopK.String())
	}
}
