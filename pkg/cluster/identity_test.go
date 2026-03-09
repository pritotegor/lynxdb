package cluster

import (
	"testing"
)

func TestRoleString(t *testing.T) {
	tests := []struct {
		role Role
		want string
	}{
		{RoleMeta, "meta"},
		{RoleIngest, "ingest"},
		{RoleQuery, "query"},
		{Role(99), "unknown(99)"},
	}

	for _, tt := range tests {
		if got := tt.role.String(); got != tt.want {
			t.Errorf("Role(%d).String() = %q, want %q", int(tt.role), got, tt.want)
		}
	}
}

func TestRoleSetHas(t *testing.T) {
	rs := RoleSet{RoleMeta, RoleQuery}

	if !rs.Has(RoleMeta) {
		t.Error("expected RoleSet to contain RoleMeta")
	}
	if !rs.Has(RoleQuery) {
		t.Error("expected RoleSet to contain RoleQuery")
	}
	if rs.Has(RoleIngest) {
		t.Error("expected RoleSet to NOT contain RoleIngest")
	}
}

func TestRoleSetString(t *testing.T) {
	rs := RoleSet{RoleMeta, RoleIngest, RoleQuery}
	want := "meta,ingest,query"
	if got := rs.String(); got != want {
		t.Errorf("RoleSet.String() = %q, want %q", got, want)
	}
}

func TestParseRoles(t *testing.T) {
	tests := []struct {
		input   []string
		want    RoleSet
		wantErr bool
	}{
		{[]string{"meta", "ingest", "query"}, RoleSet{RoleMeta, RoleIngest, RoleQuery}, false},
		{[]string{"META", "Query"}, RoleSet{RoleMeta, RoleQuery}, false},
		{[]string{"meta", "meta"}, RoleSet{RoleMeta}, false}, // dedup
		{[]string{"unknown"}, nil, true},
		{nil, nil, true},
	}

	for _, tt := range tests {
		got, err := ParseRoles(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseRoles(%v) error = %v, wantErr %v", tt.input, err, tt.wantErr)

			continue
		}
		if err != nil {
			continue
		}
		if len(got) != len(tt.want) {
			t.Errorf("ParseRoles(%v) = %v, want %v", tt.input, got, tt.want)

			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("ParseRoles(%v)[%d] = %v, want %v", tt.input, i, got[i], tt.want[i])
			}
		}
	}
}
