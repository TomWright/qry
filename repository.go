package qry

import (
	"context"
	"database/sql"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Repository struct {
	DB                   *sql.DB
	Table                string
	LogFn                func(string, []any)
	StandardSelectFields []Field

	PreSelectFn func(ctx context.Context, query Query) error
	PreInsertFn func(ctx context.Context, query Query) error
	PreUpdateFn func(ctx context.Context, query Query) error
	PreDeleteFn func(ctx context.Context, query Query) error

	Tracer trace.Tracer
}

func (repo Repository) QueryFn(ctx context.Context, queryFn func(*SelectQuery)) (*sql.Rows, error) {
	if repo.Tracer != nil {
		spanCtx, span := repo.Tracer.Start(ctx, "QueryFn")
		ctx = spanCtx
		defer span.End()
	}

	query := Select()
	queryFn(&query)
	return repo.Query(ctx, query)
}

func (repo Repository) Query(ctx context.Context, query SelectQuery) (*sql.Rows, error) {
	if repo.Tracer != nil {
		spanCtx, span := repo.Tracer.Start(ctx, "Query")
		ctx = spanCtx
		defer span.End()
	}

	query = repo.prepareSelectQuery(query)

	if repo.PreSelectFn != nil {
		if err := repo.PreSelectFn(ctx, query); err != nil {
			return nil, fmt.Errorf("pre select hook failed: %w", err)
		}
	}

	sqlQuery, args := query.Build()

	if repo.LogFn != nil {
		repo.LogFn(sqlQuery, args)
	}

	stmt, err := repo.DB.Prepare(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("could not prepare query: %w", err)
	}

	defer stmt.Close()

	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, fmt.Errorf("could not execute query: %w", err)
	}

	return rows, nil
}

func (repo Repository) QueryRowFn(ctx context.Context, queryFn func(*SelectQuery)) (*sql.Row, error) {
	if repo.Tracer != nil {
		spanCtx, span := repo.Tracer.Start(ctx, "QueryRowFn")
		ctx = spanCtx
		defer span.End()
	}

	query := Select()
	queryFn(&query)
	return repo.QueryRow(ctx, query)
}

func (repo Repository) QueryRow(ctx context.Context, query SelectQuery) (*sql.Row, error) {
	if repo.Tracer != nil {
		spanCtx, span := repo.Tracer.Start(ctx, "QueryRow")
		ctx = spanCtx
		defer span.End()
	}

	query = repo.prepareSelectQuery(query)

	if repo.PreSelectFn != nil {
		if err := repo.PreSelectFn(ctx, query); err != nil {
			return nil, fmt.Errorf("pre select hook failed: %w", err)
		}
	}

	sqlQuery, args := query.Build()

	if repo.LogFn != nil {
		repo.LogFn(sqlQuery, args)
	}

	stmt, err := repo.DB.Prepare(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("could not prepare query: %w", err)
	}

	defer stmt.Close()

	row := stmt.QueryRow(args...)

	if err := row.Err(); err != nil {
		return nil, fmt.Errorf("could not execute query: %w", err)
	}

	return row, nil
}

func (repo Repository) UpdateFn(ctx context.Context, queryFn func(*UpdateQuery)) (sql.Result, error) {
	if repo.Tracer != nil {
		spanCtx, span := repo.Tracer.Start(ctx, "UpdateFn")
		ctx = spanCtx
		defer span.End()
	}

	query := Update()
	queryFn(&query)
	return repo.Update(ctx, query)
}

func (repo Repository) Update(ctx context.Context, query UpdateQuery) (sql.Result, error) {
	if repo.Tracer != nil {
		spanCtx, span := repo.Tracer.Start(ctx, "Update")
		ctx = spanCtx
		defer span.End()
	}

	query = repo.prepareUpdateQuery(query)

	if repo.PreUpdateFn != nil {
		if err := repo.PreUpdateFn(ctx, query); err != nil {
			return nil, fmt.Errorf("pre update hook failed: %w", err)
		}
	}

	return repo.Exec(ctx, query)
}

func (repo Repository) DeleteFn(ctx context.Context, queryFn func(*DeleteQuery)) (sql.Result, error) {
	if repo.Tracer != nil {
		spanCtx, span := repo.Tracer.Start(ctx, "DeleteFn")
		ctx = spanCtx
		defer span.End()
	}

	query := Delete()
	queryFn(&query)
	return repo.Delete(ctx, query)
}

func (repo Repository) Delete(ctx context.Context, query DeleteQuery) (sql.Result, error) {
	if repo.Tracer != nil {
		spanCtx, span := repo.Tracer.Start(ctx, "Delete")
		ctx = spanCtx
		defer span.End()
	}

	query = repo.prepareDeleteQuery(query)

	if repo.PreDeleteFn != nil {
		if err := repo.PreDeleteFn(ctx, query); err != nil {
			return nil, fmt.Errorf("pre delete hook failed: %w", err)
		}
	}

	return repo.Exec(ctx, query)
}

func (repo Repository) InsertFn(ctx context.Context, queryFn func(*InsertQuery)) (sql.Result, error) {
	if repo.Tracer != nil {
		spanCtx, span := repo.Tracer.Start(ctx, "InsertFn")
		ctx = spanCtx
		defer span.End()
	}

	query := Insert()
	queryFn(&query)
	return repo.Insert(ctx, query)
}

func (repo Repository) Insert(ctx context.Context, query InsertQuery) (sql.Result, error) {
	if repo.Tracer != nil {
		spanCtx, span := repo.Tracer.Start(ctx, "Insert")
		ctx = spanCtx
		defer span.End()
	}

	query = repo.prepareInsertQuery(query)

	if repo.PreInsertFn != nil {
		if err := repo.PreInsertFn(ctx, query); err != nil {
			return nil, fmt.Errorf("pre insert hook failed: %w", err)
		}
	}

	return repo.Exec(ctx, query)
}

func (repo Repository) prepareSelectQuery(query SelectQuery) SelectQuery {
	if query.Table == "" {
		query.Table = repo.Table
	}
	if query.Fields == nil {
		query.Fields = repo.StandardSelectFields
	}
	return query
}

func (repo Repository) prepareUpdateQuery(query UpdateQuery) UpdateQuery {
	if query.Table == "" {
		query.Table = repo.Table
	}
	return query
}

func (repo Repository) prepareInsertQuery(query InsertQuery) InsertQuery {
	if query.Table == "" {
		query.Table = repo.Table
	}
	return query
}

func (repo Repository) prepareDeleteQuery(query DeleteQuery) DeleteQuery {
	if query.Table == "" {
		query.Table = repo.Table
	}
	return query
}

func (repo Repository) Exec(ctx context.Context, query Query) (sql.Result, error) {
	var span trace.Span = nil

	if repo.Tracer != nil {
		ctx, span = repo.Tracer.Start(ctx, "Exec")
		defer span.End()
	}

	sqlQuery, args := query.Build()

	if span != nil {
		span.SetAttributes(attribute.String("query", sqlQuery))
	}

	if repo.LogFn != nil {
		repo.LogFn(sqlQuery, args)
	}

	stmt, err := repo.DB.Prepare(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("could not prepare query: %w", err)
	}

	defer stmt.Close()

	result, err := stmt.Exec(args...)
	if err != nil {
		return nil, fmt.Errorf("could not execute query: %w", err)
	}

	return result, nil
}
