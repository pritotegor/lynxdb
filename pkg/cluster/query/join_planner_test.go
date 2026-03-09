package query

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func TestPlanDistributedJoin_NilCommand(t *testing.T) {
	got := PlanDistributedJoin(nil, nil)
	if got != JoinCoordinator {
		t.Errorf("expected JoinCoordinator for nil, got %v", got)
	}
}

func TestPlanDistributedJoin_NilSubquery(t *testing.T) {
	got := PlanDistributedJoin(&spl2.JoinCommand{}, nil)
	if got != JoinCoordinator {
		t.Errorf("expected JoinCoordinator for nil subquery, got %v", got)
	}
}

func TestPlanDistributedJoin_SmallHead(t *testing.T) {
	cmd := &spl2.JoinCommand{
		Subquery: &spl2.Query{
			Commands: []spl2.Command{
				&spl2.SearchCommand{},
				&spl2.HeadCommand{Count: 100},
			},
		},
	}

	got := PlanDistributedJoin(cmd, nil)
	if got != JoinBroadcast {
		t.Errorf("expected JoinBroadcast for small head, got %v", got)
	}
}

func TestPlanDistributedJoin_LargeHead(t *testing.T) {
	cmd := &spl2.JoinCommand{
		Subquery: &spl2.Query{
			Commands: []spl2.Command{
				&spl2.SearchCommand{},
				&spl2.HeadCommand{Count: 20000},
			},
		},
	}

	got := PlanDistributedJoin(cmd, nil)
	if got != JoinCoordinator {
		t.Errorf("expected JoinCoordinator for large head, got %v", got)
	}
}

func TestPlanDistributedJoin_NoHead(t *testing.T) {
	cmd := &spl2.JoinCommand{
		Subquery: &spl2.Query{
			Commands: []spl2.Command{
				&spl2.SearchCommand{},
				&spl2.WhereCommand{},
			},
		},
	}

	got := PlanDistributedJoin(cmd, nil)
	if got != JoinCoordinator {
		t.Errorf("expected JoinCoordinator for no head, got %v", got)
	}
}

func TestJoinStrategy_String(t *testing.T) {
	tests := []struct {
		s    JoinStrategy
		want string
	}{
		{JoinBroadcast, "broadcast"},
		{JoinColocated, "colocated"},
		{JoinCoordinator, "coordinator"},
	}
	for _, tt := range tests {
		if got := tt.s.String(); got != tt.want {
			t.Errorf("JoinStrategy(%d).String() = %q, want %q", tt.s, got, tt.want)
		}
	}
}
