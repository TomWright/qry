package qry

import (
	"fmt"
)

// Query is an SQL query.
type Query interface {
	Build() (string, []any)
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
