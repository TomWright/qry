package qry_test

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/TomWright/qry"
	"testing"
)

type model struct {
	ID   int64
	Name string
}

func TestTypedRepository_QueryRow(t *testing.T) {
	type def struct {
		name         string
		table        string
		selectFields []qry.Field
		query        func(*qry.TypedSelectQuery[model])
		exp          model
		mockFn       func(db sqlmock.Sqlmock)
	}
	tests := []def{
		{
			name:         "Fallback to defaults",
			table:        "users",
			selectFields: []qry.Field{"id", "name"},
			query: func(query *qry.TypedSelectQuery[model]) {
			},
			exp: model{
				ID:   int64(1),
				Name: "Tom",
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
			query: func(query *qry.TypedSelectQuery[model]) {
				query.Table = "user"
				query.Fields = []qry.Field{"user_id", "username"}
			},
			exp: model{
				ID:   1,
				Name: "Tom",
			},
			mockFn: func(db sqlmock.Sqlmock) {
				db.ExpectPrepare("SELECT user_id, username FROM user").
					ExpectQuery().
					WillReturnRows(
						sqlmock.NewRows([]string{"user_id", "username"}).
							AddRow(1, "Tom"),
					).
					RowsWillBeClosed()
			},
		},
		{
			name: "Select with equal condition",
			query: func(query *qry.TypedSelectQuery[model]) {
				query.Table = "users"
				query.Fields = []qry.Field{"id", "name"}
				query.Condition = qry.Equal("id", 1)
			},
			exp: model{
				ID:   1,
				Name: "Tom",
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
		{
			name: "Partial select",
			query: func(query *qry.TypedSelectQuery[model]) {
				query.Table = "users"
				query.Fields = []qry.Field{"name"}
				query.Condition = qry.Equal("id", 1)
				query.FieldReferences = func(target *model) []any {
					return []any{&target.Name}
				}
			},
			exp: model{
				ID:   0,
				Name: "Tom",
			},

			mockFn: func(db sqlmock.Sqlmock) {
				db.ExpectPrepare("SELECT name FROM users WHERE id = ?").
					ExpectQuery().
					WithArgs(1).
					WillReturnRows(
						sqlmock.NewRows([]string{"name"}).
							AddRow("Tom"),
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

			repo := qry.TypedRepository[model]{
				Repository: qry.Repository{
					DB:                   db,
					Table:                tc.table,
					StandardSelectFields: tc.selectFields,
				},
				StandardSelectFieldReferences: func(target *model) []any {
					return []any{
						&target.ID,
						&target.Name,
					}
				},
			}

			got, err := repo.QueryRowFn(context.Background(), tc.query)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if !checkDiff(t, &tc.exp, got) {
				return
			}
		})
	}
}
