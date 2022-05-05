package qry

import (
	"fmt"
)

func Delete() DeleteQuery {
	return DeleteQuery{}
}

type DeleteQuery struct {
	Table     string
	Condition Condition
	Limit     int64
	Offset    int64
}

func (query DeleteQuery) Build() (string, []any) {
	stmt := fmt.Sprintf(
		"DELETE FROM %s",
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

type TypedDeleteQuery[T any] struct {
	DeleteQuery

	Condition func(target *T) Condition
	Target    *T
}

func (query TypedDeleteQuery[T]) Prepare() DeleteQuery {
	query.DeleteQuery.Condition = query.Condition(query.Target)
	return query.DeleteQuery
}

func (query TypedDeleteQuery[T]) Build() (string, []any) {
	return query.Prepare().Build()
}
