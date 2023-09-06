package qry

import (
	"fmt"
	"strings"
)

// Condition is a condition that can be used in a where clause.
type Condition interface {
	// Build returns an SQL statement and the related args.
	Build() (string, []any)
}

// ConditionGroup is a Condition made up of many Conditions separated by an AND or an OR.
type ConditionGroup struct {
	Conditions []Condition
	Or         bool
}

// Build returns an SQL statement and the related args.
// The statement is already wrapped in brackets.
func (group *ConditionGroup) Build() (string, []any) {
	if group == nil {
		return "", make([]any, 0)
	}
	parts := make([]string, 0)
	args := make([]any, 0)

	if len(group.Conditions) == 0 {
		return "", args
	}

	if len(group.Conditions) > 0 {
		for _, cs := range group.Conditions {
			part, partArgs := cs.Build()
			parts = append(parts, part)
			args = append(args, partArgs...)
		}
	}

	sep := " AND "
	if group.Or {
		sep = " OR "
	}

	return fmt.Sprintf("(%s)", strings.Join(parts, sep)), args
}

// SimpleCondition is a Condition that can be used to make a basic comparison.
// E.g. user_id = "123"
type SimpleCondition struct {
	Field      Field
	Value      any
	Comparison string
}

// Build returns an SQL statement and the related args.
func (query *SimpleCondition) Build() (string, []any) {
	stmt := fmt.Sprintf("%s %s ?", query.Field, query.Comparison)
	args := []any{query.Value}
	return stmt, args
}

// RawCondition is a Condition that can be used to make more complex comparisons.
type RawCondition struct {
	SQL  string
	Args []any
}

// Build returns an SQL statement and the related args.
func (query *RawCondition) Build() (string, []any) {
	return query.SQL, query.Args
}
