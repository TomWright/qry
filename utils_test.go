package qry_test

import (
	"database/sql"
	"fmt"
	"github.com/TomWright/qry"
	"github.com/go-test/deep"
	"testing"
)

func checkDiff(t *testing.T, exp any, got any) bool {
	return checkDiffMsg(t, exp, got, "")
}

func checkDiffMsg(t *testing.T, exp any, got any, message string) bool {
	if message != "" {
		message += "\n"
	}
	if diffs := deep.Equal(exp, got); len(diffs) > 0 {
		diffsMsg := ""
		for _, diff := range diffs {
			diffsMsg += fmt.Sprintf("- %s\n", diff)
		}
		t.Errorf("%sexpected:\n%v\ngot:\n%v\n%s", message, exp, got, diffsMsg)
		return false
	}
	return true
}

func scanRowsToMapInterface(fields []qry.Field, rows *sql.Rows) ([]map[string]interface{}, error) {
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	res := make([]map[string]interface{}, 0)
	for rows.Next() {
		row, err := scanRowToMapInterface(fields, rows)
		if err != nil {
			return res, err
		}
		res = append(res, row)
	}

	return res, nil
}

func scanRowToMapInterface(fields []qry.Field, scanner qry.Scanner) (map[string]interface{}, error) {
	numColumns := len(fields)
	columns := make([]string, numColumns)
	for index, column := range fields {
		columns[index] = string(column)
	}

	values := make([]any, numColumns)
	valuePointers := make([]any, numColumns)
	for k := range values {
		valuePointers[k] = &values[k]
	}

	if err := scanner.Scan(valuePointers...); err != nil {
		return nil, fmt.Errorf("could not scan row: %v", err)
	}

	got := make(map[string]any)

	for i, column := range columns {
		got[column] = values[i]
	}

	return got, nil
}
