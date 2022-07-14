package qry

import (
	"context"
	"database/sql"
	"fmt"
)

type TypedRepository[T any] struct {
	Repository

	StandardSelectFieldReferences func(target *T) []any
	StandardUpdateValues          func(target *T) map[Field]any
	StandardUpdateCondition       func(target *T) Condition
	StandardInsertValues          func(target *T) map[Field]any
	StandardDeleteCondition       func(target *T) Condition

	PreScanFn   func(ctx context.Context, query Query, target *T) error
	PostScanFn  func(ctx context.Context, query Query, target *T) error
	PreSelectFn func(ctx context.Context, query Query) error
	PreInsertFn func(ctx context.Context, query Query) error
	PreUpdateFn func(ctx context.Context, query Query) error
	PreDeleteFn func(ctx context.Context, query Query) error
}

func (repo TypedRepository[T]) SelectQuery() TypedSelectQuery[T] {
	return TypedSelectQuery[T]{}
}

func (repo TypedRepository[T]) InsertQuery() TypedInsertQuery[T] {
	return TypedInsertQuery[T]{}
}

func (repo TypedRepository[T]) UpdateQuery() TypedUpdateQuery[T] {
	return TypedUpdateQuery[T]{}
}

func (repo TypedRepository[T]) DeleteQuery() TypedDeleteQuery[T] {
	return TypedDeleteQuery[T]{}
}

func (repo TypedRepository[T]) ScanRow(ctx context.Context, query Query, scanner Scanner, destFn func(*T) []interface{}) (*T, error) {
	result := new(T)

	if repo.PreScanFn != nil {
		if err := repo.PreScanFn(ctx, query, result); err != nil {
			return result, fmt.Errorf("pre scan hook failed: %w", err)
		}
	}

	if err := scanner.Scan(destFn(result)...); err != nil {
		return nil, fmt.Errorf("could not scan row: %w", err)
	}

	if repo.PostScanFn != nil {
		if err := repo.PostScanFn(ctx, query, result); err != nil {
			return result, fmt.Errorf("post scan hook failed: %w", err)
		}
	}

	return result, nil
}

func (repo TypedRepository[T]) QueryRowFn(ctx context.Context, queryFn func(*TypedSelectQuery[T])) (*T, error) {
	if repo.Tracer != nil {
		spanCtx, span := repo.Tracer.Start(ctx, "QueryRowFn")
		ctx = spanCtx
		defer span.End()
	}

	query := repo.SelectQuery()
	queryFn(&query)
	return repo.QueryRow(ctx, query)
}

func (repo TypedRepository[T]) QueryRow(ctx context.Context, query TypedSelectQuery[T]) (*T, error) {
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

	row, err := repo.Repository.QueryRow(ctx, query.Prepare())
	if err != nil {
		return nil, err
	}
	return repo.ScanRow(ctx, query, row, query.FieldReferences)
}

func (repo TypedRepository[T]) UpdateFn(ctx context.Context, queryFn func(*TypedUpdateQuery[T])) (sql.Result, error) {
	if repo.Tracer != nil {
		spanCtx, span := repo.Tracer.Start(ctx, "UpdateFn")
		ctx = spanCtx
		defer span.End()
	}

	query := repo.UpdateQuery()
	queryFn(&query)
	return repo.Update(ctx, query)
}

func (repo TypedRepository[T]) Update(ctx context.Context, query TypedUpdateQuery[T]) (sql.Result, error) {
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

	return repo.Repository.Update(ctx, query.Prepare())
}

func (repo TypedRepository[T]) DeleteFn(ctx context.Context, queryFn func(*TypedDeleteQuery[T])) (sql.Result, error) {
	if repo.Tracer != nil {
		spanCtx, span := repo.Tracer.Start(ctx, "DeleteFn")
		ctx = spanCtx
		defer span.End()
	}

	query := repo.DeleteQuery()
	queryFn(&query)
	return repo.Delete(ctx, query)
}

func (repo TypedRepository[T]) Delete(ctx context.Context, query TypedDeleteQuery[T]) (sql.Result, error) {
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

	return repo.Repository.Delete(ctx, query.Prepare())
}

func (repo TypedRepository[T]) InsertFn(ctx context.Context, queryFn func(*TypedInsertQuery[T])) (sql.Result, error) {
	if repo.Tracer != nil {
		spanCtx, span := repo.Tracer.Start(ctx, "InsertFn")
		ctx = spanCtx
		defer span.End()
	}

	query := repo.InsertQuery()
	queryFn(&query)
	return repo.Insert(ctx, query)
}

func (repo TypedRepository[T]) Insert(ctx context.Context, query TypedInsertQuery[T]) (sql.Result, error) {
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

	return repo.Repository.Insert(ctx, query.Prepare())
}

func (repo TypedRepository[T]) QueryFn(ctx context.Context, queryFn func(*TypedSelectQuery[T])) ([]*T, error) {
	if repo.Tracer != nil {
		spanCtx, span := repo.Tracer.Start(ctx, "QueryFn")
		ctx = spanCtx
		defer span.End()
	}

	query := repo.SelectQuery()
	queryFn(&query)
	return repo.Query(ctx, query)
}

func (repo TypedRepository[T]) Query(ctx context.Context, query TypedSelectQuery[T]) ([]*T, error) {
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

	rows, err := repo.Repository.Query(ctx, query.Prepare())
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	results := make([]*T, 0)

	for rows.Next() {
		result, err := repo.ScanRow(ctx, query, rows, query.FieldReferences)

		if err != nil {
			return results, err
		}

		results = append(results, result)
	}

	return results, nil
}

func (repo TypedRepository[T]) prepareSelectQuery(query TypedSelectQuery[T]) TypedSelectQuery[T] {
	query.SelectQuery = repo.Repository.prepareSelectQuery(query.SelectQuery)
	if query.FieldReferences == nil {
		query.FieldReferences = repo.StandardSelectFieldReferences
	}
	return query
}

func (repo TypedRepository[T]) prepareUpdateQuery(query TypedUpdateQuery[T]) TypedUpdateQuery[T] {
	query.UpdateQuery = repo.Repository.prepareUpdateQuery(query.UpdateQuery)
	if query.Values == nil {
		query.Values = repo.StandardUpdateValues
	}
	if query.Condition == nil {
		query.Condition = repo.StandardUpdateCondition
	}
	return query
}

func (repo TypedRepository[T]) prepareInsertQuery(query TypedInsertQuery[T]) TypedInsertQuery[T] {
	query.InsertQuery = repo.Repository.prepareInsertQuery(query.InsertQuery)
	if query.Values == nil {
		query.Values = repo.StandardInsertValues
	}
	return query
}

func (repo TypedRepository[T]) prepareDeleteQuery(query TypedDeleteQuery[T]) TypedDeleteQuery[T] {
	query.DeleteQuery = repo.Repository.prepareDeleteQuery(query.DeleteQuery)
	if query.Condition == nil {
		query.Condition = repo.StandardUpdateCondition
	}
	return query
}
