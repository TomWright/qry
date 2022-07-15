package qry

import (
	"fmt"
	"strings"
)

func Insert() InsertQuery {
	return InsertQuery{}
}

type InsertQuery struct {
	Fields []Field
	Values [][]any
	Table  string
	Limit  int64
	Offset int64
}

func (query InsertQuery) Build() (string, []any) {
	stmt := fmt.Sprintf(
		"INSERT INTO %s",
		query.Table,
	)

	args := make([]any, 0)

	stmt += fmt.Sprintf("(%s) ", genericJoin(query.Fields, ", "))

	valuesSeparator := ", "

	stmt += "VALUES "
	for _, rowValues := range query.Values {
		args = append(args, rowValues...)
		stmt += fmt.Sprintf("(%s) ", strings.TrimRight(strings.Repeat("?"+valuesSeparator, len(rowValues)), valuesSeparator))
	}
	stmt = strings.TrimSpace(stmt)

	if query.Limit > 0 {
		stmt += fmt.Sprintf(" LIMIT %d", query.Limit)
	}

	if query.Offset > 0 {
		stmt += fmt.Sprintf(" OFFSET %d", query.Offset)
	}

	return stmt, args
}

type TypedInsertQuery[T any] struct {
	InsertQuery

	Values  func(target *T) map[Field]any
	Targets []*T
}

func findFieldIndex(field Field, columns []Field) int {
	for i, f := range columns {
		if f == field {
			return i
		}
	}
	panic("field not found in columns")
}

func (query TypedInsertQuery[T]) Prepare() InsertQuery {
	var columns []Field = nil
	values := make([][]any, 0)

	for _, target := range query.Targets {
		targetValues := query.Values(target)

		// Extract the columns of the first target.
		// Each target should be returning the same values.
		if columns == nil {
			columns = make([]Field, len(targetValues))
			columnIndex := 0
			for k := range targetValues {
				columns[columnIndex] = k
				columnIndex++
			}
		}

		// Extract the values
		rowValues := make([]any, len(targetValues))
		for field, v := range targetValues {
			rowValueIndex := findFieldIndex(field, columns)
			rowValues[rowValueIndex] = v
		}

		values = append(values, rowValues)
	}

	query.InsertQuery.Fields = columns
	query.InsertQuery.Values = values
	return query.InsertQuery
}

func (query TypedInsertQuery[T]) Build() (string, []any) {
	return query.Prepare().Build()
}
