package dbkit

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/Zaytsev-Dmitry/dbkit/custom_error"
	"github.com/jmoiron/sqlx"
)

const (
	Get       = "Get"
	Select    = "Exec"
	QueryRowx = "QueryRowx"
)

func ExecuteQuery[T any](needTx bool, queryType string, db *sqlx.DB, query, action string, args ...interface{}) (*T, *custom_error.CustomError) {
	var result T

	run := func(exec interface{}) error {
		switch e := exec.(type) {
		case *sqlx.DB:
			return executeByType(&result, e, queryType, query, args...)
		case *sqlx.Tx:
			return executeByType(&result, e, queryType, query, args...)
		default:
			return fmt.Errorf("unsupported executor type")
		}
	}

	if needTx {
		tx, err := db.Beginx()
		if err != nil {
			return nil, custom_error.New(action+": begin tx", err)
		}
		if err := run(tx); err != nil {
			_ = tx.Rollback()
			return nil, custom_error.New(action, err)
		}
		if err := tx.Commit(); err != nil {
			return nil, custom_error.New(action+": commit", err)
		}
	} else {
		if err := run(db); err != nil {
			return nil, custom_error.New(action, err)
		}
	}

	return &result, nil
}

func executeByType[T any](out *T, exec interface{}, queryType, query string, args ...interface{}) error {
	switch queryType {
	case Get:
		return exec.(*sqlx.DB).Get(out, query, args...)
	case Select:
		return exec.(*sqlx.DB).Select(out, query, args...)
	case QueryRowx:
		return exec.(*sqlx.DB).QueryRowx(query, args...).StructScan(out)
	default:
		return fmt.Errorf("unknown query type: %s", queryType)
	}
}

func ExecuteQueryWithOutEntityResponse(db *sqlx.DB, query, action string, args ...interface{}) *custom_error.CustomError {
	var err error
	_, err = db.Exec(query, args...)
	if err != nil {
		return custom_error.New(action, err)
	}
	return nil
}

func ExecuteQuerySlice[T any](db *sqlx.DB, query, action string, args ...interface{}) ([]*T, *custom_error.CustomError) {
	var results []T
	err := db.Select(&results, query, args...)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []*T{}, nil // Возвращаем пустой срез указателей
		}
		return nil, custom_error.New(action, err)
	}

	// Преобразуем []T → []*T
	ptrResults := make([]*T, len(results))
	for i := range results {
		ptrResults[i] = &results[i]
	}

	return ptrResults, nil
}
