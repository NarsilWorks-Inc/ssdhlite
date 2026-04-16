package ssdhlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	dhl "github.com/NarsilWorks-Inc/datahelperlite/v3"
	mssql "github.com/denisenkom/go-mssqldb"
)

// SQLServerHelper implements DataHelperLite
type SQLServerHelper struct {
	hndl             dhl.DataHelperHandle
	tx               *sql.Tx
	ctx              context.Context
	trCnt            uint16
	rw               sync.RWMutex
	finalizeMu       sync.Mutex
	err              error
	manualCnt        uint16 // manual-mode nesting
	vendorStatements []vendorStmt
	frames           []bool // LIFO stack of per-scope state. true = ARMED, false = DISARMED
	rollbackTriggered,
	committed bool
}

type vendorStmt struct {
	Key   string
	Value string
}

func init() {
	dhl.SetHelper("ssdhlite", &SQLServerHelper{})
	dhl.SetErrNoRows(sql.ErrNoRows)
}

// NewHelper instantiates new helper
func (dh *SQLServerHelper) NewHelper() dhl.DataHelperLite {
	return &SQLServerHelper{
		vendorStatements: []vendorStmt{
			{
				Key:   "NOCOUNT",
				Value: "SET NOCOUNT ON;",
			},
			{
				Key:   "XABORT",
				Value: "SET ARITHABORT ON;",
			},
		},
	}
}

// Acquire sets all queries to a new context from pool.
func (dh *SQLServerHelper) Acquire(ctx context.Context, h dhl.DataHelperHandle) error {
	dh.rw.Lock()
	defer dh.rw.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	dh.ctx = ctx
	dh.hndl = h
	return nil
}

// Begin starts a transaction. If there is an existing transaction, begin is ignored
func (dh *SQLServerHelper) Begin() (err error) {
	dh.rw.Lock()
	defer dh.rw.Unlock()
	if dh.err != nil {
		err = dh.err
		return
	}
	if dh.hndl == nil {
		err = fmt.Errorf("begin: %w", dhl.ErrHandleNotSet)
		dh.err = err
		return
	}

	db := dh.hndl.DB()
	if db == nil {
		err = fmt.Errorf("begin: %w", dhl.ErrHandleDBNotSet)
		dh.err = err
		return
	}
	if dh.manualCnt > 0 {
		err = errors.New("begin: cannot mix Begin() with BeginManually() in the same transaction")
		dh.err = err
		return
	}
	defer handlePanic(&err)
	if dh.tx == nil {
		dh.tx, err = db.BeginTx(dh.ctx, nil)
		if err != nil {
			err = fmt.Errorf("begin: %w", err)
			dh.err = err
			return
		}
	}

	// Reset committed when the transaction count is 0
	if dh.trCnt == 0 {
		dh.committed = false
	}

	// Increment transaction count
	dh.trCnt++

	// Track this scope’s deferred rollback
	dh.frames = append(dh.frames, true) // armed

	return nil
}

// BeginManually starts a transaction that does not support deferred rollback.
func (dh *SQLServerHelper) BeginManually() (err error) {
	dh.rw.Lock()
	defer dh.rw.Unlock()

	if dh.err != nil {
		err = dh.err
		return
	}
	if dh.hndl == nil {
		err = fmt.Errorf("begin-manually: %w", dhl.ErrHandleNotSet)
		dh.err = err
		return
	}
	db := dh.hndl.DB()
	if db == nil {
		err = fmt.Errorf("begin-manually: %w", dhl.ErrHandleDBNotSet)
		dh.err = err
		return
	}
	if dh.trCnt > 0 || len(dh.frames) > 0 {
		err = errors.New("begin-manually: cannot mix BeginManually() with Begin() in the same transaction")
		dh.err = err
		return
	}
	defer handlePanic(&err)
	if dh.tx == nil {
		dh.tx, err = db.BeginTx(dh.ctx, nil)
		if err != nil {
			err = fmt.Errorf("begin-manually: %w", err)
			dh.err = err
			return
		}
	}

	// Reset committed when the transaction count is 0
	if dh.trCnt == 0 {
		dh.committed = false
	}

	// Increment transaction count
	dh.trCnt++
	dh.manualCnt++

	return nil
}

func (dh *SQLServerHelper) Commit() (err error) {

	dh.rw.RLock()
	tx, trCnt,
		committed,
		rollbackTriggered,
		herr,
		hndl,
		manualCnt := dh.tx,
		dh.trCnt,
		dh.committed,
		dh.rollbackTriggered,
		dh.err,
		dh.hndl, dh.manualCnt
	dh.rw.RUnlock()

	if tx == nil || trCnt == 0 || rollbackTriggered || committed {
		return nil
	}

	if herr != nil {
		return dh.Rollback()
	}

	defer handlePanic(&err)

	// Manual mode
	if manualCnt > 0 {
		// Check variables
		if hndl == nil {
			dh.rw.Lock()
			err = fmt.Errorf("commit: %w", dhl.ErrHandleNotSet)
			dh.err = err
			dh.rw.Unlock()
			return
		}
		if db := hndl.DB(); db == nil {
			dh.rw.Lock()
			err = fmt.Errorf("commit: %w", dhl.ErrHandleDBNotSet)
			dh.err = err
			dh.rw.Unlock()
			return
		}
		// Check if manual count is more than 1
		if manualCnt > 1 {
			dh.rw.Lock()
			dh.manualCnt--
			if dh.trCnt > 0 {
				dh.trCnt--
			}
			err = nil
			dh.err = err
			dh.rw.Unlock()
			return
		}

		// Outermost manual: real commit
		dh.finalizeMu.Lock()
		err = tx.Commit()
		dh.finalizeMu.Unlock()
		if err != nil && !errors.Is(err, sql.ErrTxDone) {
			dh.rw.Lock()
			err = fmt.Errorf("commit: %w", err)
			dh.err = err
			dh.rw.Unlock()
			return
		}

		dh.rw.Lock()
		dh.manualCnt = 0
		dh.tx = nil
		dh.committed = false
		// Treat ErrTxDone as success (idempotent commit)
		if err == nil || errors.Is(err, sql.ErrTxDone) {
			dh.committed = true
			err = nil
			dh.err = err
		}

		dh.rollbackTriggered = false
		dh.frames = nil
		dh.trCnt = 0
		dh.rw.Unlock()
		return nil
	}

	// Deferred mode:
	// If the transaction is not the outermost transaction, flag the frame as disarmed
	if trCnt > 1 {
		dh.rw.Lock()
		if n := len(dh.frames); n > 0 {
			dh.frames[n-1] = false // DISARMED
		}
		dh.trCnt--
		dh.rw.Unlock()
		return nil
	}

	// Ensure DB, connection, and transaction are valid before committing
	if hndl == nil {
		dh.rw.Lock()
		err = fmt.Errorf("commit: %w", dhl.ErrHandleNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	// Serialize finalization
	// Commit the outermost transaction
	dh.finalizeMu.Lock()
	err = tx.Commit()
	dh.finalizeMu.Unlock()
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		dh.rw.Lock()
		err = fmt.Errorf("commit: %w", err)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	// Mark committed, set transaction to nil and set rollback flag to false
	dh.rw.Lock()
	dh.trCnt = 0
	dh.tx = nil
	dh.committed = false
	if err == nil || errors.Is(err, sql.ErrTxDone) {
		dh.committed = true
		err = nil
		dh.err = err
	}
	dh.rollbackTriggered = false
	dh.frames = nil
	dh.rw.Unlock()
	return nil
}

func (dh *SQLServerHelper) Rollback() (err error) {
	dh.rw.RLock()
	tx, trCnt, manualCnt, committed, herr := dh.tx, dh.trCnt, dh.manualCnt, dh.committed, dh.err
	dh.rw.RUnlock()

	if tx == nil || committed {
		return nil
	}

	// Manual mode
	if manualCnt > 0 {
		if manualCnt > 1 {
			dh.rw.Lock()
			dh.manualCnt--
			if dh.trCnt > 0 {
				dh.trCnt--
			}
			dh.rw.Unlock()
			return nil
		}
		// outermost manual
		return dh.realRollback()
	}

	// If there is a database error, rollback
	if herr != nil {
		return dh.realRollback()
	}

	dh.rw.Lock()
	// Deferred mode:
	// Note: This part will only return if the previous frame was committed earlier
	//
	// If:
	// 	- the frame count is greater than 0
	//  - the last frame is disarmed (false, if this scope was committed earlier)
	// Pop it and return (no-ops)
	if n := len(dh.frames); n > 0 && !dh.frames[n-1] {
		dh.frames = dh.frames[:n-1]
		dh.rw.Unlock()
		return nil
	}
	dh.rw.Unlock()

	// Deferred mode:
	// Note: This part will only return if the previous frame was NOT committed earlier
	//
	// If the transaction count is greater than 1
	// Reduce the transaction count and scopes (frames)
	// Pop it and return (no-ops)
	if trCnt > 1 {
		dh.rw.Lock()
		if n := len(dh.frames); n > 0 {
			dh.frames = dh.frames[:n-1] // pop armed
		}
		dh.trCnt--
		dh.rw.Unlock()
		return nil
	}

	// If this is the outermost transaction, rollback the transaction
	return dh.realRollback()
}

func (dh *SQLServerHelper) realRollback() (err error) {
	dh.rw.RLock()
	tx, hndl := dh.tx, dh.hndl
	dh.rw.RUnlock()

	if hndl == nil {
		dh.rw.Lock()
		err = fmt.Errorf("rollbk: %w", dhl.ErrHandleNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	if db := hndl.DB(); db == nil {
		dh.rw.Lock()
		err = fmt.Errorf("rollbk: %w", dhl.ErrHandleDBNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	dh.rw.Lock()
	dh.rollbackTriggered = true // 🔧 Mark rollback occurred
	dh.rw.Unlock()

	defer handlePanic(&err)

	// serialize finalization
	dh.finalizeMu.Lock()
	err = tx.Rollback()
	dh.finalizeMu.Unlock()
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		dh.rw.Lock()
		err = fmt.Errorf("rollbk: %w", err)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	// Reset all transaction state after rollback
	dh.rw.Lock()
	defer dh.rw.Unlock()
	dh.tx = nil
	dh.trCnt = 0
	dh.err = nil
	dh.committed = false         // 🔧 Reset flags
	dh.rollbackTriggered = false // 🔧 Reset flags (rollback is done)
	dh.frames = nil              // NEW: clear frames
	return nil
}

// Mark a savepoint
func (dh *SQLServerHelper) Mark(name string) (err error) {
	dh.rw.RLock()
	tx, herr, trCnt, hndl := dh.tx, dh.err, dh.trCnt, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		err = herr
		return
	}
	if tx == nil {
		dh.rw.Lock()
		err = fmt.Errorf("mark: %w", dhl.ErrNoTx)
		dh.err = err
		dh.rw.Unlock()
		return
	}
	if hndl == nil {
		dh.rw.Lock()
		err = fmt.Errorf("mark: %w", dhl.ErrHandleNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}
	if db := hndl.DB(); db == nil {
		dh.rw.Lock()
		err = fmt.Errorf("mark: %w", dhl.ErrHandleDBNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	defer handlePanic(&err)

	if trCnt > 0 {
		_, err = tx.ExecContext(dh.ctx, `SAVE TRAN sp_`+sanitizeName(name)+`;`)
		if err != nil {
			dh.rw.Lock()
			err = fmt.Errorf("mark: %w", err)
			dh.err = err
			dh.rw.Unlock()
			return
		}
	}

	return nil
}

// Discard a savepoint
func (dh *SQLServerHelper) Discard(name string) (err error) {
	dh.rw.RLock()
	tx, herr, trCnt, hndl := dh.tx, dh.err, dh.trCnt, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		err = herr
		return
	}
	if tx == nil {
		dh.rw.Lock()
		err = fmt.Errorf("discard: %w", dhl.ErrNoTx)
		dh.err = err
		dh.rw.Unlock()
		return
	}
	if hndl == nil {
		dh.rw.Lock()
		err = fmt.Errorf("discard: %w", dhl.ErrHandleNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}
	if db := hndl.DB(); db == nil {
		dh.rw.Lock()
		err = fmt.Errorf("discard: %w", dhl.ErrHandleDBNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	defer handlePanic(&err)
	if trCnt > 0 {
		_, err = tx.ExecContext(dh.ctx, `ROLLBACK TRAN sp_`+sanitizeName(name)+`;`)
		if err != nil {
			dh.rw.Lock()
			err = fmt.Errorf("discard: %w", err)
			dh.err = err
			dh.rw.Unlock()
			return
		}
	}
	return nil
}

// Save a savepoint
func (dh *SQLServerHelper) Save(name string) (err error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	dh.rw.Lock()
	defer dh.rw.Unlock()
	if herr != nil {
		err = herr
		return
	}
	if tx == nil {
		err = fmt.Errorf("save: %w", dhl.ErrNoTx)
		dh.err = err
		return
	}
	if hndl == nil {
		err = fmt.Errorf("save: %w", dhl.ErrHandleNotSet)
		dh.err = err
		return
	}
	if db := hndl.DB(); db == nil {
		err = fmt.Errorf("save: %w", dhl.ErrHandleDBNotSet)
		dh.err = err
		return
	}

	// if trCnt > 0 {
	// 	_, err := tx.ExecContext(dh.ctx, `COMMIT TRAN sp_`+name+`;`)
	// 	if err != nil {
	// 		dh.setDHErr(fmt.Errorf("save: %w", err))
	// 		return dh.err
	// 	}
	// }
	return nil
}

// Query retrieves rows from database
func (dh *SQLServerHelper) Query(querySql string, args ...any) (rows dhl.Rows, err error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		err = herr
		return
	}

	if hndl == nil {
		dh.rw.Lock()
		err = fmt.Errorf("query: %w", dhl.ErrHandleNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	db := hndl.DB()
	if db == nil {
		dh.rw.Lock()
		err = fmt.Errorf("query: %w", dhl.ErrHandleDBNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	placeholder, paraminseq, schema := dh.getParamDataInfo()

	defer handlePanic(&err)

	// Replace question mark (?) parameter with configured query parameter, if there are any
	// Replace tables meant for interpolation {table} for putting the schema
	querySql = dhl.ReplaceQueryParamMarker(querySql, paraminseq, placeholder)
	querySql = dhl.InterpolateTable(querySql, schema)
	args = refineParameters(args...)

	var sqr *sql.Rows
	if tx != nil {
		sqr, err = tx.QueryContext(dh.ctx, querySql, args...)
	} else {
		sqr, err = db.QueryContext(dh.ctx, querySql, args...)
	}
	if err != nil {
		dh.rw.Lock()
		err = fmt.Errorf("query: %w", err)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	return NewSQLServerRows(sqr), nil
}

// QueryArray puts the single column result to an output array
func (dh *SQLServerHelper) QueryArray(querySql string, out any, args ...any) (err error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		err = herr
		return
	}

	if hndl == nil {
		dh.rw.Lock()
		err = fmt.Errorf("queryarray: %w", dhl.ErrHandleNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	db := hndl.DB()
	if db == nil {
		dh.rw.Lock()
		err = fmt.Errorf("queryarray: %w", dhl.ErrHandleDBNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	placeholder, paraminseq, schema := dh.getParamDataInfo()

	switch out.(type) {
	case *[]string, *[]int, *[]int8, *[]int16, *[]int32, *[]int64, *[]bool, *[]float32, *[]float64:
	case *[]time.Time:
	default:
		dh.rw.Lock()
		err = fmt.Errorf("queryarray: %w", dhl.ErrArrayTypeNotSupported)
		dh.err = err
		dh.rw.Unlock()
		return dh.err
	}

	// Replace question mark (?) parameter with configured query parameter, if there are any
	// Replace tables meant for interpolation {table} for putting the schema
	querySql = dhl.InterpolateTable(dhl.ReplaceQueryParamMarker(querySql, paraminseq, placeholder), schema)
	args = refineParameters(args...)

	defer handlePanic(&err)

	var sqr *sql.Rows
	if tx != nil {
		sqr, err = tx.QueryContext(dh.ctx, querySql, args...)
	} else {
		sqr, err = db.QueryContext(dh.ctx, querySql, args...)
	}
	if err != nil {
		dh.rw.Lock()
		err = fmt.Errorf("queryarray: %w", err)
		dh.err = err
		dh.rw.Unlock()
		return
	}
	defer sqr.Close()

	dh.rw.Lock()
	defer dh.rw.Unlock()
	switch t := out.(type) {
	case *[]string:
		for sqr.Next() {
			var v string
			if err = sqr.Scan(&v); err != nil {
				err = fmt.Errorf("queryarray: %w", err)
				dh.err = err
				return
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			err = fmt.Errorf("queryarray: %w", err)
			dh.err = err
			return
		}
		_ = t
	case *[]int:
		for sqr.Next() {
			var v int
			if err = sqr.Scan(&v); err != nil {
				err = fmt.Errorf("queryarray: %w", err)
				dh.err = err
				return
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			err = fmt.Errorf("queryarray: %w", err)
			dh.err = err
			return
		}
		_ = t
	case *[]int8:
		for sqr.Next() {
			var v int8
			if err = sqr.Scan(&v); err != nil {
				err = fmt.Errorf("queryarray: %w", err)
				dh.err = err
				return
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			err = fmt.Errorf("queryarray: %w", err)
			dh.err = err
			return
		}
		_ = t
	case *[]int16:
		for sqr.Next() {
			var v int16
			if err = sqr.Scan(&v); err != nil {
				err = fmt.Errorf("queryarray: %w", err)
				dh.err = err
				return
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			err = fmt.Errorf("queryarray: %w", err)
			dh.err = err
			return
		}
		_ = t
	case *[]int32:
		for sqr.Next() {
			var v int32
			if err = sqr.Scan(&v); err != nil {
				err = fmt.Errorf("queryarray: %w", err)
				dh.err = err
				return
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			err = fmt.Errorf("queryarray: %w", err)
			dh.err = err
			return
		}
		_ = t
	case *[]int64:
		for sqr.Next() {
			var v int64
			if err = sqr.Scan(&v); err != nil {
				err = fmt.Errorf("queryarray: %w", err)
				dh.err = err
				return
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			err = fmt.Errorf("queryarray: %w", err)
			dh.err = err
			return
		}
		_ = t
	case *[]bool:
		for sqr.Next() {
			var v bool
			if err = sqr.Scan(&v); err != nil {
				err = fmt.Errorf("queryarray: %w", err)
				dh.err = err
				return
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			err = fmt.Errorf("queryarray: %w", err)
			dh.err = err
			return
		}
		_ = t
	case *[]float32:
		for sqr.Next() {
			var v float32
			if err = sqr.Scan(&v); err != nil {
				err = fmt.Errorf("queryarray: %w", err)
				dh.err = err
				return
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			err = fmt.Errorf("queryarray: %w", err)
			dh.err = err
			return
		}
		_ = t
	case *[]float64:
		for sqr.Next() {
			var v float64
			if err = sqr.Scan(&v); err != nil {
				err = fmt.Errorf("queryarray: %w", err)
				dh.err = err
				return
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			err = fmt.Errorf("queryarray: %w", err)
			dh.err = err
			return
		}
		_ = t
	case *[]time.Time:
		for sqr.Next() {
			var v time.Time
			if err = sqr.Scan(&v); err != nil {
				err = fmt.Errorf("queryarray: %w", err)
				dh.err = err
				return
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			err = fmt.Errorf("queryarray: %w", err)
			dh.err = err
			return
		}
		_ = t
	}

	return nil
}

// QueryRow retrieves a single row from a query
func (dh *SQLServerHelper) QueryRow(querySql string, args ...any) dhl.Row {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()

	if herr != nil {
		return NewSQLServerRow(nil)
	}

	if hndl == nil {
		dh.rw.Lock()
		dh.err = fmt.Errorf("queryrow: %w", dhl.ErrHandleNotSet)
		dh.rw.Unlock()
		return NewSQLServerRow(nil)
	}

	db := hndl.DB()
	if db == nil {
		dh.rw.Lock()
		dh.err = fmt.Errorf("queryrow: %w", dhl.ErrHandleDBNotSet)
		dh.rw.Unlock()
		return NewSQLServerRow(nil)
	}
	placeholder, paraminseq, schema := dh.getParamDataInfo()

	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.InterpolateTable(dhl.ReplaceQueryParamMarker(querySql, paraminseq, placeholder), schema)
	args = refineParameters(args...)

	defer handlePanic(nil)
	if tx != nil {
		return NewSQLServerRow(tx.QueryRowContext(dh.ctx, querySql, args...))
	}

	return NewSQLServerRow(db.QueryRowContext(dh.ctx, querySql, args...))
}

// Exec executes data manipulation command and returns the number of affected rows
func (dh *SQLServerHelper) Exec(querySql string, args ...any) (ra int64, err error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()

	if herr != nil {
		err = herr
		return
	}

	if hndl == nil {
		dh.rw.Lock()
		err = fmt.Errorf("exec: %w", dhl.ErrHandleNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	db := hndl.DB()
	if db == nil {
		dh.rw.Lock()
		err = fmt.Errorf("exec: %w", dhl.ErrHandleDBNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	placeholder, paraminseq, schema := dh.getParamDataInfo()

	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.ReplaceQueryParamMarker(querySql, paraminseq, placeholder)
	querySql = dhl.InterpolateTable(querySql, schema)
	args = refineParameters(args...)

	var sqr sql.Result

	defer handlePanic(&err)
	if tx != nil {
		sqr, err = tx.ExecContext(dh.ctx, querySql, args...)
	} else {
		sqr, err = db.ExecContext(dh.ctx, querySql, args...)
	}
	if err != nil {
		dh.rw.Lock()
		err = fmt.Errorf("exec: %w", err)
		dh.err = err
		dh.rw.Unlock()
		return
	}
	ra, _ = sqr.RowsAffected()
	return
}

// Exists checks if a record exist
func (dh *SQLServerHelper) Exists(sqlWithParams string, args ...any) (exists bool, err error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()

	if herr != nil {
		err = herr
		return
	}

	if hndl == nil {
		dh.rw.Lock()
		err = fmt.Errorf("exists: %w", dhl.ErrHandleNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	db := hndl.DB()
	if db == nil {
		dh.rw.Lock()
		err = fmt.Errorf("exists: %w", dhl.ErrHandleDBNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	placeholder, paraminseq, schema := dh.getParamDataInfo()

	// replace question mark (?) parameter with configured query parameter, if there are any
	sqlWithParams = dhl.ReplaceQueryParamMarker(sqlWithParams, paraminseq, placeholder)
	sqlWithParams = dhl.InterpolateTable(sqlWithParams, schema)
	sqlWithParams = strings.TrimSpace(sqlWithParams)
	if strings.HasSuffix(sqlWithParams, `;`) {
		dh.rw.Lock()
		err = errors.New(`semicolons are not allowed at the end of this query`)
		dh.err = err
		dh.rw.Unlock()
		return
	}
	args = refineParameters(args...)

	var b strings.Builder
	b.Grow(len(sqlWithParams) + 100)
	b.WriteString("SELECT TOP 1 1 FROM ")
	b.WriteString(sqlWithParams)
	b.WriteByte(';')
	sqlq := b.String()

	defer handlePanic(&err)

	var cnt int
	if tx != nil {
		err = tx.QueryRowContext(dh.ctx, sqlq, args...).Scan(&cnt)
		if err != nil {
			if !errors.Is(err, dhl.ErrNoRows) {
				dh.rw.Lock()
				err = fmt.Errorf("exists: %w", err)
				dh.err = err
				dh.rw.Unlock()
				return false, dh.err
			}
		}
		return cnt == 1, nil
	}
	err = db.QueryRowContext(dh.ctx, sqlq, args...).Scan(&cnt)
	if err != nil {
		if !errors.Is(err, dhl.ErrNoRows) {
			dh.rw.Lock()
			err = fmt.Errorf("exists: %w", err)
			dh.err = err
			dh.rw.Unlock()
			return false, dh.err
		}
	}
	return cnt == 1, nil
}

// ExistsExt a set of validation expression against the underlying database table
func (dh *SQLServerHelper) ExistsExt(tableName string, values []dhl.ColumnFilter) (valid bool, err error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()

	if herr != nil {
		err = herr
		return
	}
	if hndl == nil {
		dh.rw.Lock()
		err = fmt.Errorf("existsext: %w", dhl.ErrHandleNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	db := hndl.DB()
	if db == nil {
		dh.rw.Lock()
		err = fmt.Errorf("existsext: %w", dhl.ErrHandleDBNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	var (
		andstr, sqlq,
		ph string
		exists bool
		i      int
	)

	args := make([]any, 0)

	placeholder, paraminseq, schema := dh.getParamDataInfo()

	tableNameWithParameters := tableName
	if len(values) > 0 {
		tableNameWithParameters += ` WHERE `
	}
	ph = placeholder
	for _, v := range values {
		if isInterfaceNil(v.Value) {
			v.Operator = " IS NULL"
			ph = ""
		} else {
			// If there is no operator, we default to "="
			if v.Operator == "" {
				v.Operator = "="
			}
			if paraminseq {
				ph = placeholder + strconv.Itoa(i+1)
			}
			args = append(args, v.Value)
			i++
		}

		tableNameWithParameters += andstr + v.Name + v.Operator + ph
		andstr = " AND "
	}

	args = refineParameters(args...)
	tableNameWithParameters = strings.TrimSpace(tableNameWithParameters)
	if strings.HasSuffix(tableNameWithParameters, `;`) {
		tableNameWithParameters, _ = strings.CutSuffix(tableNameWithParameters, `;`)
	}

	defer handlePanic(&err)

	sqlq = dhl.InterpolateTable(`SELECT CAST(CASE WHEN (SELECT TOP(1) 1 FROM `+tableNameWithParameters+`) = 1 THEN 1 ELSE 0 END AS BIT);`, schema)
	var row *sql.Row
	if tx != nil {
		row = tx.QueryRowContext(dh.ctx, sqlq, args...)
	} else {
		row = db.QueryRowContext(dh.ctx, sqlq, args...)
	}
	if err := row.Scan(&exists); err != nil {
		if !errors.Is(err, dhl.ErrNoRows) {
			err = fmt.Errorf("existsext: %w", err)
			dh.rw.Lock()
			dh.err = err
			dh.rw.Unlock()
			return false, err
		}
		return false, nil
	}

	return exists, nil
}

// Next gets the next serial number
func (dh *SQLServerHelper) Next(serial string, next *int64) (err error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()

	if herr != nil {
		err = herr
		return
	}

	if hndl == nil {
		dh.rw.Lock()
		err = fmt.Errorf("next: %w", dhl.ErrHandleNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	db := hndl.DB()
	if db == nil {
		dh.rw.Lock()
		err = fmt.Errorf("next: %w", dhl.ErrHandleDBNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	if next == nil {
		dh.rw.Lock()
		err = fmt.Errorf("next: %w", dhl.ErrVarMustBeInit)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	di := hndl.DI()
	if di == nil {
		dh.rw.Lock()
		err = errors.New("next: database info not configured")
		dh.err = err
		dh.rw.Unlock()
		return
	}

	schema := "dbo"
	if di.Schema != nil && *di.Schema != "" {
		schema = *di.Schema
	}

	defer handlePanic(&err)

	// if the database config has set a sequence generator, this will use it
	sg := di.SequenceGenerator
	if sg != nil {
		if sg.NamePlaceHolder == "" {
			dh.rw.Lock()
			err = errors.New(`next: name place holder should be provided. ` +
				`Set name place holder in {placeholder} format. ` +
				`Place holder name should also be present in the upsert or select query`)
			dh.err = err
			dh.rw.Unlock()
			return
		}
		if sg.ResultQuery == "" {
			dh.rw.Lock()
			err = errors.New(`next: result query must be provided`)
			dh.err = err
			dh.rw.Unlock()
			return
		}
		// Upsert is usually an insert or an update, so we execute it.
		// It is optional when all queries are set in the result query.
		// affr (affected rows) must be at least 1 to proceed
		affr := int64(1)
		if sg.UpsertQuery != "" {
			var sqr sql.Result
			sqlq := dhl.InterpolateTable(strings.ReplaceAll(sg.UpsertQuery, sg.NamePlaceHolder, serial), schema)
			if tx != nil {
				sqr, err = tx.ExecContext(dh.ctx, sqlq)
			} else {
				sqr, err = db.ExecContext(dh.ctx, sqlq)
			}
			if err != nil {
				dh.rw.Lock()
				err = fmt.Errorf("next: %w", err)
				dh.err = err
				dh.rw.Unlock()
				return
			}
			affr, _ = sqr.RowsAffected()
		}
		// in the event that the upsert alters the affr variable to 0, we return an error
		if affr == 0 {
			dh.rw.Lock()
			err = errors.New(`next: upsert query did not insert or update any records`)
			dh.err = err
			dh.rw.Unlock()
			return
		}
		// result query needs a single scalar value to be returned
		sqlq := dhl.InterpolateTable(strings.ReplaceAll(sg.ResultQuery, sg.NamePlaceHolder, serial), schema)
		if tx != nil {
			err = tx.QueryRowContext(dh.ctx, sqlq).Scan(next)
		} else {
			err = db.QueryRowContext(dh.ctx, sqlq).Scan(next)
		}
		if err != nil {
			dh.rw.Lock()
			err = fmt.Errorf("next: %w", err)
			dh.err = err
			dh.rw.Unlock()
			return
		}
		return nil
	}

	// If there are no sequence configuration specified, we will create a and use a sequence (SQL Server 2012 and later).
	// The format of the sequence should be <schema>.<sequence name>.
	// Dots are not allowed in the sequence name, therefore it must be converted to
	// another character, for example an underscore. If there is a dot specified
	// in the serial, it would be parsed as the schema.
	sln := serial
	if idx := strings.Index(serial, "."); idx != -1 {
		schema = serial[:idx]
		sln = strings.ReplaceAll(serial[idx+1:], ".", "_")
	}
	sln = "seq_" + sln

	sqlq := fmt.Sprintf("SELECT NEXT VALUE FOR %s.%s;", schema, sln)
	scan := func() error {
		if tx != nil {
			return tx.QueryRowContext(dh.ctx, sqlq).Scan(next)
		}
		return db.QueryRowContext(dh.ctx, sqlq).Scan(next)
	}
	if err = scan(); err == nil {
		return
	} else {
		var sqlErr mssql.Error
		if errors.As(err, &sqlErr) && sqlErr.Number == 208 { // object not found
			ddl := fmt.Sprintf(
				"IF NOT EXISTS(SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'%s.%s') AND type = 'SO') "+
					"CREATE SEQUENCE %s.%s AS INT START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;",
				schema, sln, schema, sln,
			)
			if tx != nil {
				if _, err2 := tx.ExecContext(dh.ctx, ddl); err2 != nil {
					err = fmt.Errorf("next: %w", err2)
					return
				}
			} else {
				if _, err2 := db.ExecContext(dh.ctx, ddl); err2 != nil {
					err = fmt.Errorf("next: %w", err2)
					return
				}
			}
			return scan() // retry
		}
		err = fmt.Errorf("next: %w", err) // some other error
		return
	}
}

// Escape a field value (fv) from disruption by single quote
func (dh *SQLServerHelper) Escape(fv string) string {
	if fv == "" {
		return ""
	}
	// For SQL Server, safest default:
	return strings.ReplaceAll(fv, `'`, `''`)
}

// DatabaseVersion returns database version
func (dh *SQLServerHelper) DatabaseVersion() string {
	var version string
	err := dh.QueryRow(`SELECT @@VERSION;`).Scan(&version)
	if err != nil {
		version = err.Error()
	}
	return version
}

// Now gets the current server date
func (dh *SQLServerHelper) Now() *time.Time {
	var tm time.Time
	err := dh.QueryRow(`SELECT GETDATE();`).Scan(&tm)
	if err != nil {
		tm = time.Now()
		return &tm
	}
	return &tm
}

// NowUTC gets the current server date in UTC
func (dh *SQLServerHelper) NowUTC() *time.Time {
	var tm time.Time
	err := dh.QueryRow(`SELECT GETUTCDATE();`).Scan(&tm)
	if err != nil {
		tm = time.Now().UTC()
		return &tm
	}
	return &tm
}

// refineParameters sets the built-in type of the datahelper-specified parameter type
// to mssql parameter type. The default type for strings is nvarchar
func refineParameters(args ...any) []any {
	for i, arg := range args {
		switch t := arg.(type) {
		case dhl.VarChar:
			args[i] = mssql.VarChar(t)
		case dhl.NVarCharMax:
			args[i] = mssql.NVarCharMax(t)
		case dhl.VarCharMax:
			args[i] = mssql.VarCharMax(t)
		default:
			_ = t
		}
	}
	return args
}

// Ping sends data packets to check pool connection
func (dh *SQLServerHelper) Ping() (err error) {
	dh.rw.RLock()
	hndl, ctx := dh.hndl, dh.ctx
	dh.rw.RUnlock()

	defer handlePanic(&err)
	if hndl == nil {
		dh.rw.Lock()
		err = fmt.Errorf("ping: %w", dhl.ErrHandleNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}

	db := hndl.DB()
	if db == nil {
		dh.rw.Lock()
		err = fmt.Errorf("ping: %w", dhl.ErrHandleDBNotSet)
		dh.err = err
		dh.rw.Unlock()
		return
	}
	return db.PingContext(ctx)
}

// UpsertReturning inserts a row into the table.
// If a conflict occurs on the specified unique columns:
//
//   - If updateColumns is empty, a no-op update is issued and the existing row is returned unchanged
//   - If updateColumns is provided, the existing row is updated using the incoming source values
//
// Parameters:
//   - insertColumns - columns in the INSERT. All NOT NULL columns without defaults must be included.
//   - uniqueColumns - columns defining the conflict target. These should correspond to a unique key or unique index in the database.
//   - updateColumns -
//     1. empty or nil → do not modify existing row on conflict
//     2. non-empty → updates the specified columns using the values provided for insertColumns
//   - returnColumns - columns to return
//   - args - values for insertColumns, in order
//
// Note: The table and columns that require escaping must be set in the parameters.
//
// The method always returns the resulting row.
func (dh *SQLServerHelper) UpsertReturning(
	tableName string,
	insertColumns []string,
	uniqueColumns []string,
	updateColumns []string,
	returnColumns []string,
	args ...any,
) (dhl.Row, error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return NewSQLServerRow(nil), herr
	}
	if tableName == "" {
		return NewSQLServerRow(nil), fmt.Errorf("upsertreturning: %s", "table name not set")
	}
	if len(insertColumns) == 0 {
		return NewSQLServerRow(nil), fmt.Errorf("upsertreturning: %s", "insert columns needs to be set")
	}
	if len(uniqueColumns) == 0 {
		return NewSQLServerRow(nil), fmt.Errorf("upsertreturning: %s", "unique columns needs to be set")
	}
	if len(returnColumns) == 0 {
		return NewSQLServerRow(nil), fmt.Errorf("upsertreturning: %s", "return columns needs to be set")
	}
	if len(insertColumns) != len(args) {
		return NewSQLServerRow(nil), fmt.Errorf("upsertreturning: %s", "insert columns count and arguments mismatch")
	}
	if len(updateColumns) > 0 {
		for _, updCol := range updateColumns {
			found := false
			for _, insCol := range insertColumns {
				if strings.EqualFold(insCol, updCol) {
					found = true
					break
				}
			}
			if !found {
				return NewSQLServerRow(nil), fmt.Errorf("upsertreturning: %s", "update columns does not exist in insert columns")
			}
		}
	}

	if hndl == nil {
		dh.rw.Lock()
		dh.err = fmt.Errorf("upsertreturning: %w", dhl.ErrHandleNotSet)
		dh.rw.Unlock()
		return NewSQLServerRow(nil), dh.err
	}

	db := hndl.DB()
	if db == nil {
		dh.rw.Lock()
		dh.err = fmt.Errorf("upsertreturning: %w", dhl.ErrHandleDBNotSet)
		dh.rw.Unlock()
		return NewSQLServerRow(nil), dh.err
	}

	// Build query
	sep := ""
	sql := "UPDATE t \n"
	sql += "SET "
	if len(updateColumns) == 0 {
		col := insertColumns[0]
		sql += col + " = " + "t." + col + "\n"
	} else {
		for _, updCol := range updateColumns {
			sql += sep + updCol + " = s." + updCol
			sep = ","
		}
		sql += "\n"
	}

	sql += "OUTPUT "
	sep = ""
	for _, retCol := range returnColumns {
		sql += sep + "inserted." + retCol
		sep = ","
	}
	sql += "\n"

	sql += "FROM " + tableName + " AS t \n"
	sql += "	INNER JOIN (\n"
	sql += "		SELECT \n"
	for i, insCol := range insertColumns {
		sep = ","
		if i == len(insertColumns)-1 {
			sep = ""
		}
		sql += "		? AS " + insCol + sep + "\n"
	}
	sql += ") AS s ON "
	sep = ""
	for _, unqCol := range uniqueColumns {
		sql += sep + " t." + unqCol + " = s." + unqCol
		sep = " AND "
	}
	sql += "\n"

	sql += "IF @@ROWCOUNT = 0\n"
	sql += "BEGIN\n"
	sql += "	INSERT INTO " + tableName + " (" + strings.Join(insertColumns, ",") + ")\n"
	sql += "	OUTPUT "
	sep = ""
	for _, retCol := range returnColumns {
		sql += sep + "inserted." + retCol
		sep = ","
	}
	sql += "\n"
	sql += "	VALUES (" + strings.TrimSuffix(strings.Repeat("?,", len(insertColumns)), ",") + ")\n"
	sql += "END"

	placeholder, paraminseq, schema := dh.getParamDataInfo()

	// replace question mark (?) parameter with configured query parameter, if there are any
	sql = dhl.InterpolateTable(dhl.ReplaceQueryParamMarker(sql, paraminseq, placeholder), schema)
	args = refineParameters(args...)

	// Since we are populating a pseudo table with values from arguments,
	// we should create a duplicate and add it to the arguments
	execArgs := append([]any{}, args...)
	execArgs = append(execArgs, args...)

	defer handlePanic(nil)
	if tx != nil {
		return NewSQLServerRow(tx.QueryRowContext(dh.ctx, sql, execArgs...)), nil
	}

	return NewSQLServerRow(db.QueryRowContext(dh.ctx, sql, execArgs...)), nil
}

// VendorStatement returns a vendor-specific statement or query when present. Returns an empty string if not present
func (dh *SQLServerHelper) VendorStatement(key string) string {
	for _, vs := range dh.vendorStatements {
		if strings.EqualFold(vs.Key, key) {
			return vs.Value
		}
	}
	return ""
}

// VendorStatements Lists the vendor-specific statements implemented in a helper
func (dh *SQLServerHelper) VendorStatements() []string {
	stmts := make([]string, 0)
	for _, vs := range dh.vendorStatements {
		stmts = append(stmts, vs.Key)
	}
	return stmts
}

func (dh *SQLServerHelper) getParamDataInfo() (ph string, pis bool, sch string) {
	dh.rw.RLock()
	h := dh.hndl
	dh.rw.RUnlock()
	ph = "?"
	sch = "dbo"
	if h == nil || h.DI() == nil {
		return
	}
	if h.DI().ParameterPlaceHolder != nil && *h.DI().ParameterPlaceHolder != "" {
		ph = *h.DI().ParameterPlaceHolder
	}
	if h.DI().ParameterInSequence != nil {
		pis = *h.DI().ParameterInSequence
	}
	if h.DI().Schema != nil && *h.DI().Schema != "" {
		sch = *h.DI().Schema
	}
	return
}

func sanitizeName(s string) string {
	// replace non [A-Za-z0-9_] with _
	b := make([]byte, len(s))
	for i := range s {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
			b[i] = c
		} else {
			b[i] = '_'
		}
	}
	if len(b) == 0 {
		return "sp"
	}
	return string(b)
}
