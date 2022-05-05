package qry

import (
	"fmt"
)

func Select() SelectQuery {
	return SelectQuery{}
}

// SelectQuery is a Query.
type SelectQuery struct {
	Fields    []Field
	Table     string
	Condition Condition
	Limit     int64
	Offset    int64
}

func (query SelectQuery) Build() (string, []any) {
	stmt := fmt.Sprintf(
		"SELECT %s FROM %s",
		genericJoin(query.Fields, ", "),
		query.Table,
	)

	args := make([]any, 0)

	if query.Condition != nil {
		if conditionsStmt, conditionArgs := query.Condition.Build(); len(conditionsStmt) > 0 {
			stmt += fmt.Sprintf(" WHERE %s", conditionsStmt)
			args = append(args, conditionArgs...)
		}
	}

	if query.Limit > 0 {
		stmt += fmt.Sprintf(" LIMIT %d", query.Limit)
	}

	if query.Offset > 0 {
		stmt += fmt.Sprintf(" OFFSET %d", query.Offset)
	}

	return stmt, args
}

type TypedSelectQuery[T any] struct {
	SelectQuery
	FieldReferences func(target *T) []any
}

func (query TypedSelectQuery[T]) Prepare() SelectQuery {
	return query.SelectQuery
}

func (query TypedSelectQuery[T]) Build() (string, []any) {
	return query.Prepare().Build()
}
