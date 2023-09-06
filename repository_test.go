package qry_test

import (
	"context"
	"database/sql"
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
				"uuid":     "11112222-3333-4444-5555-666677778888",
				"username": "Tom",
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
		{
			name: "Select with equal condition and order by",
			query: func() qry.SelectQuery {
				query := qry.Select()
				query.Table = "users"
				query.Fields = []qry.Field{"id", "name"}
				query.Condition = qry.Equal("id", 1)
				query.OrderBy = []qry.OrderBy{
					{
						Field:     "name",
						Direction: qry.Descending,
					},
					{
						Field:     "id",
						Direction: qry.Ascending,
					},
				}
				query.Limit = 5
				query.Offset = 2
				return query
			},
			exp: map[string]any{
				"id":   int64(1),
				"name": "Tom",
			},

			mockFn: func(db sqlmock.Sqlmock) {
				db.ExpectPrepare("SELECT id, name FROM users WHERE id = ? ORDER BY name DESC, id ASC LIMIT 5 OFFSET 2").
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

			defer func(db *sql.DB) {
				_ = db.Close()
			}(db)

			tc.mockFn(mock)

			var query qry.SelectQuery
			repo := qry.Repository{
				DB:                   db,
				Table:                tc.table,
				StandardSelectFields: tc.selectFields,
				PreSelectFn: func(ctx context.Context, innerQuery qry.Query) error {
					query = innerQuery.(qry.SelectQuery)
					return nil
				},
			}

			row, err := repo.QueryRow(context.Background(), tc.query())
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			got, err := scanRowToMapInterface(query.Fields, row)
			if err != nil {
				t.Error(err)
				return
			}

			if !checkDiff(t, tc.exp, got) {
				return
			}
		})
	}
}

func TestRepository_Query(t *testing.T) {
	type def struct {
		name         string
		table        string
		selectFields []qry.Field
		query        func() qry.SelectQuery
		exp          []map[string]any
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
			exp: []map[string]any{
				{
					"id":   int64(1),
					"name": "Tom",
				},
				{
					"id":   int64(2),
					"name": "Jim",
				},
			},
			mockFn: func(db sqlmock.Sqlmock) {
				db.ExpectPrepare("SELECT id, name FROM users").
					ExpectQuery().
					WillReturnRows(
						sqlmock.NewRows([]string{"id", "name"}).
							AddRow(int64(1), "Tom").
							AddRow(int64(2), "Jim"),
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
			exp: []map[string]any{
				{
					"uuid":     "11112222-3333-4444-5555-666677778888",
					"username": "Tom",
				},
				{
					"uuid":     "88887777-6666-5555-4444-333322221111",
					"username": "Jim",
				},
			},
			mockFn: func(db sqlmock.Sqlmock) {
				db.ExpectPrepare("SELECT uuid, username FROM user").
					ExpectQuery().
					WillReturnRows(
						sqlmock.NewRows([]string{"uuid", "username"}).
							AddRow("11112222-3333-4444-5555-666677778888", "Tom").
							AddRow("88887777-6666-5555-4444-333322221111", "Jim"),
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
			exp: []map[string]any{
				{
					"id":   int64(1),
					"name": "Tom",
				},
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

			defer func(db *sql.DB) {
				_ = db.Close()
			}(db)

			tc.mockFn(mock)

			var query qry.SelectQuery
			repo := qry.Repository{
				DB:                   db,
				Table:                tc.table,
				StandardSelectFields: tc.selectFields,
				PreSelectFn: func(ctx context.Context, innerQuery qry.Query) error {
					query = innerQuery.(qry.SelectQuery)
					return nil
				},
			}

			rows, err := repo.Query(context.Background(), tc.query())
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			got, err := scanRowsToMapInterface(query.Fields, rows)
			if err != nil {
				t.Error(err)
				return
			}

			if !checkDiff(t, tc.exp, got) {
				return
			}
		})
	}
}
