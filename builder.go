package qry

import (
	"fmt"
	"strings"
)

// Query is an SQL query.
type Query interface {
	Build() (string, []any)
}

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
func (query *ConditionGroup) Build() (string, []any) {
	if query == nil {
		return "", make([]any, 0)
	}
	parts := make([]string, 0)
	args := make([]any, 0)

	if len(query.Conditions) > 0 {
		for _, cs := range query.Conditions {
			part, partArgs := cs.Build()
			parts = append(parts, part)
			args = append(args, partArgs...)
		}
	}

	sep := " AND "
	if query.Or {
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

// And returns a ConditionGroup made up of many Conditions separated an AND.
func And(conditions ...Condition) Condition {
	return &ConditionGroup{
		Conditions: conditions,
		Or:         false,
	}
}

// Or returns a ConditionGroup made up of many Conditions separated an OR.
func Or(conditions ...Condition) Condition {
	return &ConditionGroup{
		Conditions: conditions,
		Or:         true,
	}
}

// Equal returns a SimpleCondition that will check that the given field has the given value.
func Equal(field Field, value any) Condition {
	if value == nil {
		return &RawCondition{
			SQL:  fmt.Sprintf("%s IS NULL", field),
			Args: []any{},
		}
	}
	return &SimpleCondition{
		Field:      field,
		Comparison: "=",
		Value:      value,
	}
}

// NotEqual returns a SimpleCondition that will check that the given field does not have the given value.
func NotEqual(field Field, value any) Condition {
	if value == nil {
		return &RawCondition{
			SQL:  fmt.Sprintf("%s IS NOT NULL", field),
			Args: []any{},
		}
	}
	return &SimpleCondition{
		Field:      field,
		Comparison: "!=",
		Value:      value,
	}
}

// JsonArrayContains returns a Condition that will check if the given value exists in a JSON array stored under
// the given field.
func JsonArrayContains(field Field, value any) Condition {
	return &RawCondition{
		SQL:  fmt.Sprintf("JSON_CONTAINS(%s, ?, '$') = 1", field),
		Args: []any{value},
	}
}

// NotJsonArrayContains returns a Condition that will check that the given value does not exist in a JSON array stored under
// the given field.
func NotJsonArrayContains(field Field, value any) Condition {
	return &RawCondition{
		SQL:  fmt.Sprintf("JSON_CONTAINS(%s, ?, '$') = 0", field),
		Args: []any{value},
	}
}
