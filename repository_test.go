package qry_test

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/TomWright/qry"
	"testing"
)

func TestRepository_QueryRow(t *testing.T) {
	type def struct {
		name         string
		table        string
		selectFields []qry.Field
		query        func() qry.SelectQuery
		exp          map[string]any
		mockFn       func(db sqlmock.Sqlmock)
	}
	tests := []def{
		{
			name:         "Fallback to defaults",
			table:        "users",
			selectFields: []qry.Field{"id", "name"},
			query: func() qry.SelectQuery {
				query := qry.Select()
				return query
			},
			exp: map[string]any{
				"id":   int64(1),
				"name": "Tom",
			},
			mockFn: func(db sqlmock.Sqlmock) {
				db.ExpectPrepare("SELECT id, name FROM users").
					ExpectQuery().
					WillReturnRows(
						sqlmock.NewRows([]string{"id", "name"}).
							AddRow(int64(1), "Tom"),
					).
					RowsWillBeClosed()
			},
		},
		{
			name:         "Basic select",
			table:        "users",
			selectFields: []qry.Field{"id", "name"},
			query: func() qry.SelectQuery {
				query := qry.Select()
				query.Table = "user"
				query.Fields = []qry.Field{"uuid", "username"}
				return query
			},
			exp: map[string]any{
				"uuid": "11112222-3333-4444-5555-666677778888",
				"name": "Tom",
			},
			mockFn: func(db sqlmock.Sqlmock) {
				db.ExpectPrepare("SELECT uuid, username FROM user").
					ExpectQuery().
					WillReturnRows(
						sqlmock.NewRows([]string{"uuid", "username"}).
							AddRow("11112222-3333-4444-5555-666677778888", "Tom"),
					).
					RowsWillBeClosed()
			},
		},
		{
			name: "Select with equal condition",
			query: func() qry.SelectQuery {
				query := qry.Select()
				query.Table = "users"
				query.Fields = []qry.Field{"id", "name"}
				query.Condition = qry.Equal("id", 1)
				return query
			},
			exp: map[string]any{
				"id":   int64(1),
				"name": "Tom",
			},

			mockFn: func(db sqlmock.Sqlmock) {
				db.ExpectPrepare("SELECT id, name FROM users WHERE id = ?").
					ExpectQuery().
					WithArgs(1).
					WillReturnRows(
						sqlmock.NewRows([]string{"id", "name"}).
							AddRow(1, "Tom"),
					).
					RowsWillBeClosed()
			},
		},
	}

	for _, test := range tests {
		tc := test
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Errorf("could not init db mock")
				return
			}

			defer db.Close()

			tc.mockFn(mock)

			repo := qry.Repository{
				DB:                   db,
				Table:                tc.table,
				StandardSelectFields: tc.selectFields,
			}

			row, err := repo.QueryRow(context.Background(), tc.query())
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			columns := make([]string, len(tc.exp))
			columnIndex := 0
			for column, _ := range tc.exp {
				columns[columnIndex] = column
				columnIndex++
			}

			values := make([]any, columnIndex)
			valuePointers := make([]any, columnIndex)
			for k, _ := range values {
				valuePointers[k] = &values[k]
			}

			if err := row.Scan(valuePointers...); err != nil {
				t.Errorf("could not scan row: %v", err)
				return
			}

			got := make(map[string]any)

			for i, column := range columns {
				got[column] = values[i]
			}

			if !checkDiff(t, tc.exp, got) {
				return
			}
		})
	}
}
