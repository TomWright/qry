package qry_test

import (
	"github.com/TomWright/qry"
	"testing"
)

func TestEqual(t *testing.T) {
	type def struct {
		name    string
		field   qry.Field
		value   any
		expStmt string
		expArgs []any
	}
	tests := []def{
		{
			name:    "Basic string",
			field:   "name",
			value:   "abc",
			expStmt: "name = ?",
			expArgs: []any{"abc"},
		},
		{
			name:    "Basic int",
			field:   "age",
			value:   123,
			expStmt: "age = ?",
			expArgs: []any{123},
		},
		{
			name:    "Negative int",
			field:   "age",
			value:   -123,
			expStmt: "age = ?",
			expArgs: []any{-123},
		},
		{
			name:    "Basic bool true",
			field:   "enabled",
			value:   true,
			expStmt: "enabled = ?",
			expArgs: []any{true},
		},
		{
			name:    "Basic bool false",
			field:   "enabled",
			value:   false,
			expStmt: "enabled = ?",
			expArgs: []any{false},
		},
		{
			name:    "NULL",
			field:   "deleted_at",
			value:   nil,
			expStmt: "deleted_at IS NULL",
			expArgs: []any{},
		},
	}

	for _, test := range tests {
		tc := test
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotStmt, gotArgs := qry.Equal(tc.field, tc.value).Build()

			if !checkDiffMsg(t, tc.expStmt, gotStmt, "invalid statement") {
				return
			}

			if !checkDiffMsg(t, tc.expArgs, gotArgs, "invalid args") {
				return
			}
		})
	}
}

func TestNotEqual(t *testing.T) {
	type def struct {
		name    string
		field   qry.Field
		value   any
		expStmt string
		expArgs []any
	}
	tests := []def{
		{
			name:    "Basic string",
			field:   "name",
			value:   "abc",
			expStmt: "name != ?",
			expArgs: []any{"abc"},
		},
		{
			name:    "Basic int",
			field:   "age",
			value:   123,
			expStmt: "age != ?",
			expArgs: []any{123},
		},
		{
			name:    "Negative int",
			field:   "age",
			value:   -123,
			expStmt: "age != ?",
			expArgs: []any{-123},
		},
		{
			name:    "Basic bool true",
			field:   "enabled",
			value:   true,
			expStmt: "enabled != ?",
			expArgs: []any{true},
		},
		{
			name:    "Basic bool false",
			field:   "enabled",
			value:   false,
			expStmt: "enabled != ?",
			expArgs: []any{false},
		},
		{
			name:    "NULL",
			field:   "deleted_at",
			value:   nil,
			expStmt: "deleted_at IS NOT NULL",
			expArgs: []any{},
		},
	}

	for _, test := range tests {
		tc := test
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotStmt, gotArgs := qry.NotEqual(tc.field, tc.value).Build()

			if !checkDiffMsg(t, tc.expStmt, gotStmt, "invalid statement") {
				return
			}

			if !checkDiffMsg(t, tc.expArgs, gotArgs, "invalid args") {
				return
			}
		})
	}
}

func TestJsonArrayContains(t *testing.T) {
	type def struct {
		name    string
		field   qry.Field
		value   any
		expStmt string
		expArgs []any
	}
	tests := []def{
		{
			name:    "Basic string",
			field:   "name",
			value:   "abc",
			expStmt: "JSON_CONTAINS(name, ?, '$') = 1",
			expArgs: []any{"abc"},
		},
		{
			name:    "Basic int",
			field:   "age",
			value:   123,
			expStmt: "JSON_CONTAINS(age, ?, '$') = 1",
			expArgs: []any{123},
		},
		{
			name:    "Negative int",
			field:   "age",
			value:   -123,
			expStmt: "JSON_CONTAINS(age, ?, '$') = 1",
			expArgs: []any{-123},
		},
		{
			name:    "Basic bool true",
			field:   "enabled",
			value:   true,
			expStmt: "JSON_CONTAINS(enabled, ?, '$') = 1",
			expArgs: []any{true},
		},
		{
			name:    "Basic bool false",
			field:   "enabled",
			value:   false,
			expStmt: "JSON_CONTAINS(enabled, ?, '$') = 1",
			expArgs: []any{false},
		},
	}

	for _, test := range tests {
		tc := test
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotStmt, gotArgs := qry.JsonArrayContains(tc.field, tc.value).Build()

			if !checkDiffMsg(t, tc.expStmt, gotStmt, "invalid statement") {
				return
			}

			if !checkDiffMsg(t, tc.expArgs, gotArgs, "invalid args") {
				return
			}
		})
	}
}

func TestNotJsonArrayContains(t *testing.T) {
	type def struct {
		name    string
		field   qry.Field
		value   any
		expStmt string
		expArgs []any
	}
	tests := []def{
		{
			name:    "Basic string",
			field:   "name",
			value:   "abc",
			expStmt: "JSON_CONTAINS(name, ?, '$') = 0",
			expArgs: []any{"abc"},
		},
		{
			name:    "Basic int",
			field:   "age",
			value:   123,
			expStmt: "JSON_CONTAINS(age, ?, '$') = 0",
			expArgs: []any{123},
		},
		{
			name:    "Negative int",
			field:   "age",
			value:   -123,
			expStmt: "JSON_CONTAINS(age, ?, '$') = 0",
			expArgs: []any{-123},
		},
		{
			name:    "Basic bool true",
			field:   "enabled",
			value:   true,
			expStmt: "JSON_CONTAINS(enabled, ?, '$') = 0",
			expArgs: []any{true},
		},
		{
			name:    "Basic bool false",
			field:   "enabled",
			value:   false,
			expStmt: "JSON_CONTAINS(enabled, ?, '$') = 0",
			expArgs: []any{false},
		},
	}

	for _, test := range tests {
		tc := test
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotStmt, gotArgs := qry.NotJsonArrayContains(tc.field, tc.value).Build()

			if !checkDiffMsg(t, tc.expStmt, gotStmt, "invalid statement") {
				return
			}

			if !checkDiffMsg(t, tc.expArgs, gotArgs, "invalid args") {
				return
			}
		})
	}
}

func TestAnd(t *testing.T) {
	type def struct {
		name       string
		conditions []qry.Condition
		expStmt    string
		expArgs    []any
	}
	tests := []def{
		{
			name:       "One Condition",
			conditions: []qry.Condition{qry.Equal("name", "Tom")},
			expStmt:    "(name = ?)",
			expArgs:    []any{"Tom"},
		},
		{
			name: "Two Condition",
			conditions: []qry.Condition{
				qry.Equal("first_name", "Tom"),
				qry.Equal("last_name", "Wright"),
			},
			expStmt: "(first_name = ? AND last_name = ?)",
			expArgs: []any{"Tom", "Wright"},
		},
		{
			name: "Nested Or Condition",
			conditions: []qry.Condition{
				qry.Equal("first_name", "Tom"),
				qry.Equal("last_name", "Wright"),
				qry.Or(qry.Equal("active", true), qry.Equal("banned", false)),
			},
			expStmt: "(first_name = ? AND last_name = ? AND (active = ? OR banned = ?))",
			expArgs: []any{"Tom", "Wright", true, false},
		},
	}

	for _, test := range tests {
		tc := test
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotStmt, gotArgs := qry.And(tc.conditions...).Build()

			if !checkDiffMsg(t, tc.expStmt, gotStmt, "invalid statement") {
				return
			}

			if !checkDiffMsg(t, tc.expArgs, gotArgs, "invalid args") {
				return
			}
		})
	}
}

func TestOr(t *testing.T) {
	type def struct {
		name       string
		conditions []qry.Condition
		expStmt    string
		expArgs    []any
	}
	tests := []def{
		{
			name:       "One Condition",
			conditions: []qry.Condition{qry.Equal("name", "Tom")},
			expStmt:    "(name = ?)",
			expArgs:    []any{"Tom"},
		},
		{
			name: "Two Condition",
			conditions: []qry.Condition{
				qry.Equal("first_name", "Tom"),
				qry.Equal("last_name", "Wright"),
			},
			expStmt: "(first_name = ? OR last_name = ?)",
			expArgs: []any{"Tom", "Wright"},
		},
		{
			name: "Nested Or Condition",
			conditions: []qry.Condition{
				qry.Or(qry.Equal("first_name", "Tom"), qry.Equal("last_name", "Wright")),
				qry.Or(qry.Equal("active", true), qry.Equal("banned", false)),
			},
			expStmt: "((first_name = ? OR last_name = ?) OR (active = ? OR banned = ?))",
			expArgs: []any{"Tom", "Wright", true, false},
		},
	}

	for _, test := range tests {
		tc := test
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotStmt, gotArgs := qry.Or(tc.conditions...).Build()

			if !checkDiffMsg(t, tc.expStmt, gotStmt, "invalid statement") {
				return
			}

			if !checkDiffMsg(t, tc.expArgs, gotArgs, "invalid args") {
				return
			}
		})
	}
}
