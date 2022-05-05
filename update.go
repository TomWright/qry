package qry

import (
	"fmt"
	"strings"
)

func Update() UpdateQuery {
	return UpdateQuery{}
}

type UpdateQuery struct {
	Values    map[Field]any
	Table     string
	Limit     int64
	Offset    int64
	Condition Condition
}

func (query UpdateQuery) Build() (string, []any) {
	stmt := fmt.Sprintf(
		"UPDATE %s SET ",
		query.Table,
	)

	args := make([]any, 0)

	for field, value := range query.Values {
		stmt += fmt.Sprintf("%s = ?, ", field)
		args = append(args, value)
	}
	stmt = strings.TrimRight(stmt, ", ")

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

type TypedUpdateQuery[T any] struct {
	UpdateQuery

	Values    func(target *T) map[Field]any
	Condition func(target *T) Condition
	Target    *T
}

func (query TypedUpdateQuery[T]) Prepare() UpdateQuery {
	query.UpdateQuery.Values = query.Values(query.Target)
	query.UpdateQuery.Condition = query.Condition(query.Target)
	return query.UpdateQuery
}

func (query TypedUpdateQuery[T]) Build() (string, []any) {
	return query.Prepare().Build()
}
