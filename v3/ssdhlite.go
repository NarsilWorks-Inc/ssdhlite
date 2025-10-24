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
	hndl       dhl.DataHelperHandler
	tx         *sql.Tx
	ctx        context.Context
	trCnt      uint16
	rw         sync.RWMutex
	finalizeMu sync.Mutex
	err        error
	rollbackTriggered,
	committed bool
	// LIFO stack of per-scope state. true = ARMED, false = DISARMED
	frames    []bool
	manualCnt uint16 // manual-mode nesting
}

func init() {
	dhl.SetHelper(`ssdhlite`, &SQLServerHelper{})
	dhl.SetErrNoRows(sql.ErrNoRows)
}

// NewHelper instantiates new helper
func (dh *SQLServerHelper) NewHelper() dhl.DataHelperLiter {
	return &SQLServerHelper{}
}

// Acquire sets all queries to a new context from pool.
func (dh *SQLServerHelper) Acquire(ctx context.Context, h dhl.DataHelperHandler) error {
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
func (dh *SQLServerHelper) Begin() error {
	dh.rw.Lock()
	defer dh.rw.Unlock()
	if dh.err != nil {
		return dh.err
	}
	if dh.hndl == nil {
		dh.err = fmt.Errorf("begin: %w", dhl.ErrHandleNotSet)
		return dh.err
	}
	if dh.manualCnt > 0 {
		dh.err = errors.New("begin: cannot mix Begin() with BeginManually() in the same transaction")
		return dh.err
	}
	if dh.tx == nil {
		var err error
		dh.tx, err = dh.hndl.DB().BeginTx(dh.ctx, nil)
		if err != nil {
			dh.err = fmt.Errorf("begin: %w", err)
			return dh.err
		}
	}
	// Increment transaction count
	dh.trCnt++
	dh.committed = false         // ✅ Reset commit state
	dh.rollbackTriggered = false // ✅ Reset rollback state

	// Track this scope’s deferred rollback
	dh.frames = append(dh.frames, true) // armed

	return nil
}

// BeginManually starts a transaction that does not support deferred rollback.
func (dh *SQLServerHelper) BeginManually() error {
	dh.rw.Lock()
	defer dh.rw.Unlock()
	if dh.err != nil {
		return dh.err
	}
	if dh.hndl == nil {
		dh.err = fmt.Errorf("begin-manually: %w", dhl.ErrHandleNotSet)
		return dh.err
	}
	if dh.trCnt > 0 || len(dh.frames) > 0 {
		dh.err = errors.New("begin-manually: cannot mix BeginManually() with Begin() in the same transaction")
		return dh.err
	}
	if dh.tx == nil {
		var err error
		dh.tx, err = dh.hndl.DB().BeginTx(dh.ctx, nil)
		if err != nil {
			dh.err = fmt.Errorf("begin-manually: %w", err)
			return dh.err
		}
	}

	// Increment transaction count
	dh.trCnt++
	dh.manualCnt++
	dh.committed = false         // Reset commit state
	dh.rollbackTriggered = false // Reset rollback state
	return nil
}

func (dh *SQLServerHelper) Commit() error {
	dh.rw.RLock()
	tx, trCnt, committed, rb, herr, hndl, manualCnt := dh.tx, dh.trCnt, dh.committed, dh.rollbackTriggered, dh.err, dh.hndl, dh.manualCnt
	dh.rw.RUnlock()
	if tx == nil || trCnt == 0 || rb || committed {
		return nil
	}
	if herr != nil {
		return dh.Rollback()
	}

	// Manual mode
	if manualCnt > 0 {
		if hndl == nil {
			dh.setDHErr(fmt.Errorf("commit: %w", dhl.ErrHandleNotSet))
			return dh.err
		}
		if manualCnt > 1 {
			dh.rw.Lock()
			dh.manualCnt--
			if dh.trCnt > 0 {
				dh.trCnt--
			}
			dh.rw.Unlock()
			return nil
		}
		// outermost manual: real commit
		dh.finalizeMu.Lock()
		err := tx.Commit()
		dh.finalizeMu.Unlock()
		if err != nil && !errors.Is(err, sql.ErrTxDone) {
			dh.setDHErr(fmt.Errorf("commit: %w", err))
			return dh.err
		}

		dh.rw.Lock()
		dh.manualCnt = 0
		dh.tx = nil
		// Treat ErrTxDone as success (idempotent commit)
		if err == nil || errors.Is(err, sql.ErrTxDone) {
			dh.committed = true
			dh.err = nil
		} else {
			dh.committed = false
		}
		dh.rollbackTriggered = false
		dh.frames = nil
		dh.trCnt = 0
		dh.rw.Unlock()
		return nil
	}

	// Deferred mode
	// If the transaction is not the outermost transaction, reduce transaction count.
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
		dh.setDHErr(fmt.Errorf("commit: %w", dhl.ErrHandleNotSet))
		return dh.err
	}

	// Serialize finalization
	// Commit the outermost transaction
	dh.finalizeMu.Lock()
	err := tx.Commit()
	dh.finalizeMu.Unlock()
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		dh.setDHErr(fmt.Errorf("commit: %w", err))
		return dh.err
	}

	// Mark committed, set transaction to nil and set rollback flag to false
	dh.rw.Lock()
	dh.trCnt = 0
	if err == nil || errors.Is(err, sql.ErrTxDone) {
		dh.committed = true
		dh.err = nil
	}
	dh.tx = nil
	dh.rollbackTriggered = false
	dh.frames = nil
	dh.rw.Unlock()
	return nil
}

func (dh *SQLServerHelper) Rollback() error {
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
		return dh.rollbk()
	}

	// Deferred mode
	// If this scope was committed earlier, its defer no-ops
	dh.rw.Lock()
	if n := len(dh.frames); n > 0 && !dh.frames[n-1] {
		dh.frames = dh.frames[:n-1]
		dh.rw.Unlock()
		return nil
	}
	dh.rw.Unlock()

	if herr != nil {
		return dh.rollbk()
	}

	if trCnt > 1 {
		dh.rw.Lock()
		dh.rollbackTriggered = true
		if n := len(dh.frames); n > 0 {
			dh.frames = dh.frames[:n-1] // pop armed
		}
		if dh.trCnt > 0 {
			dh.trCnt--
		}
		dh.rw.Unlock()
		return nil
	}

	// If this is the outermost transaction, rollback the transaction
	return dh.rollbk()
}

func (dh *SQLServerHelper) rollbk() error {
	dh.rw.RLock()
	tx, hndl := dh.tx, dh.hndl
	dh.rw.RUnlock()
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("rollbk: %w", dhl.ErrHandleNotSet))
		return dh.err
	}
	dh.rw.Lock()
	dh.rollbackTriggered = true // 🔧 Mark rollback occurred
	dh.rw.Unlock()

	// serialize finalization
	dh.finalizeMu.Lock()
	err := tx.Rollback()
	dh.finalizeMu.Unlock()
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		dh.setDHErr(fmt.Errorf("rollbk: %w", err))
		return dh.err
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
func (dh *SQLServerHelper) Mark(name string) error {
	dh.rw.RLock()
	tx, herr, trCnt, hndl := dh.tx, dh.err, dh.trCnt, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return herr
	}

	if tx == nil {
		dh.setDHErr(fmt.Errorf("mark: %w", dhl.ErrNoTx))
		return dh.err
	}
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("mark: %w", dhl.ErrHandleNotSet))
		return dh.err
	}

	if trCnt > 0 {
		_, err := tx.ExecContext(dh.ctx, `SAVE TRAN sp_`+sanitizeName(name)+`;`)
		if err != nil {
			dh.setDHErr(fmt.Errorf("mark: %w", err))
			return dh.err
		}
	}

	return nil
}

// Discard a savepoint
func (dh *SQLServerHelper) Discard(name string) error {
	dh.rw.RLock()
	tx, herr, trCnt, hndl := dh.tx, dh.err, dh.trCnt, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return herr
	}
	if tx == nil {
		dh.setDHErr(fmt.Errorf("discard: %w", dhl.ErrNoTx))
		return dh.err
	}
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("discard: %w", dhl.ErrHandleNotSet))
		return dh.err
	}

	if trCnt > 0 {
		_, err := tx.ExecContext(dh.ctx, `ROLLBACK TRAN sp_`+sanitizeName(name)+`;`)
		if err != nil {
			dh.setDHErr(fmt.Errorf("discard: %w", err))
			return dh.err
		}
	}
	return nil
}

// Save a savepoint
func (dh *SQLServerHelper) Save(name string) error {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return herr
	}
	if tx == nil {
		dh.setDHErr(fmt.Errorf("save: %w", dhl.ErrNoTx))
		return dh.err
	}
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("save: %w", dhl.ErrHandleNotSet))
		return dh.err
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
func (dh *SQLServerHelper) Query(querySql string, args ...any) (dhl.Rows, error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return nil, herr
	}

	if hndl == nil {
		dh.setDHErr(fmt.Errorf("query: %w", dhl.ErrHandleNotSet))
		return nil, dh.err
	}

	placeholder, paraminseq, schema := dh.getParamDataInfo()

	// Replace question mark (?) parameter with configured query parameter, if there are any
	// Replace tables meant for interpolation {table} for putting the schema
	querySql = dhl.ReplaceQueryParamMarker(querySql, paraminseq, placeholder)
	querySql = dhl.InterpolateTable(querySql, schema)
	args = refineParameters(args...)

	var (
		sqr *sql.Rows
		err error
	)
	if tx != nil {
		sqr, err = tx.QueryContext(dh.ctx, querySql, args...)
	} else {
		sqr, err = hndl.DB().QueryContext(dh.ctx, querySql, args...)
	}
	if err != nil {
		dh.setDHErr(fmt.Errorf("query: %w", err))
		return nil, dh.err
	}

	return NewSQLServerRows(sqr), nil
}

// QueryArray puts the single column result to an output array
func (dh *SQLServerHelper) QueryArray(querySql string, out any, args ...any) error {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return herr
	}

	if hndl == nil {
		dh.setDHErr(fmt.Errorf("queryarray: %w", dhl.ErrHandleNotSet))
		return dh.err
	}

	placeholder, paraminseq, schema := dh.getParamDataInfo()

	switch out.(type) {
	case *[]string, *[]int, *[]int8, *[]int16, *[]int32, *[]int64, *[]bool, *[]float32, *[]float64:
	case *[]time.Time:
	default:
		dh.setDHErr(fmt.Errorf("queryarray: %w", dhl.ErrArrayTypeNotSupported))
		return dh.err
	}

	// Replace question mark (?) parameter with configured query parameter, if there are any
	// Replace tables meant for interpolation {table} for putting the schema
	querySql = dhl.InterpolateTable(dhl.ReplaceQueryParamMarker(querySql, paraminseq, placeholder), schema)
	args = refineParameters(args...)

	var (
		sqr *sql.Rows
		err error
	)
	if tx != nil {
		sqr, err = tx.QueryContext(dh.ctx, querySql, args...)
	} else {
		sqr, err = hndl.DB().QueryContext(dh.ctx, querySql, args...)
	}
	if err != nil {
		dh.setDHErr(fmt.Errorf("queryarray: %w", err))
		return dh.err
	}
	defer sqr.Close()

	switch t := out.(type) {
	case *[]string:
		for sqr.Next() {
			var v string
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]int:
		for sqr.Next() {
			var v int
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]int8:
		for sqr.Next() {
			var v int8
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]int16:
		for sqr.Next() {
			var v int16
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]int32:
		for sqr.Next() {
			var v int32
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]int64:
		for sqr.Next() {
			var v int64
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]bool:
		for sqr.Next() {
			var v bool
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]float32:
		for sqr.Next() {
			var v float32
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]float64:
		for sqr.Next() {
			var v float64
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]time.Time:
		for sqr.Next() {
			var v time.Time
			if err = sqr.Scan(&v); err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if err = sqr.Err(); err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
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
		dh.setDHErr(fmt.Errorf("queryrow: %w", dhl.ErrHandleNotSet))
		return NewSQLServerRow(nil)
	}
	placeholder, paraminseq, schema := dh.getParamDataInfo()

	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.InterpolateTable(dhl.ReplaceQueryParamMarker(querySql, paraminseq, placeholder), schema)
	args = refineParameters(args...)

	if tx != nil {
		return NewSQLServerRow(tx.QueryRowContext(dh.ctx, querySql, args...))
	}

	return NewSQLServerRow(hndl.DB().QueryRowContext(dh.ctx, querySql, args...))
}

// Exec executes data manipulation command and returns the number of affected rows
func (dh *SQLServerHelper) Exec(querySql string, args ...any) (int64, error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return 0, herr
	}

	if hndl == nil {
		dh.setDHErr(fmt.Errorf("exec: %w", dhl.ErrHandleNotSet))
		return 0, dh.err
	}

	placeholder, paraminseq, schema := dh.getParamDataInfo()

	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.ReplaceQueryParamMarker(querySql, paraminseq, placeholder)
	querySql = dhl.InterpolateTable(querySql, schema)
	args = refineParameters(args...)

	var (
		sqr sql.Result
		err error
	)
	if tx != nil {
		sqr, err = tx.ExecContext(dh.ctx, querySql, args...)
	} else {
		sqr, err = hndl.DB().ExecContext(dh.ctx, querySql, args...)
	}
	if err != nil {
		dh.setDHErr(fmt.Errorf("exec: %w", err))
		return 0, dh.err
	}
	ra, _ := sqr.RowsAffected()
	return ra, nil
}

// Exists checks if a record exist
func (dh *SQLServerHelper) Exists(sqlWithParams string, args ...any) (bool, error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return false, herr
	}

	if hndl == nil {
		dh.setDHErr(fmt.Errorf("exists: %w", dhl.ErrHandleNotSet))
		return false, dh.err
	}

	placeholder, paraminseq, schema := dh.getParamDataInfo()

	// replace question mark (?) parameter with configured query parameter, if there are any
	sqlWithParams = dhl.ReplaceQueryParamMarker(sqlWithParams, paraminseq, placeholder)
	sqlWithParams = dhl.InterpolateTable(sqlWithParams, schema)
	sqlWithParams = strings.TrimSpace(sqlWithParams)
	if strings.HasSuffix(sqlWithParams, `;`) {
		dh.setDHErr(errors.New(`semicolons are not allowed at the end of this query`))
		return false, dh.err
	}
	args = refineParameters(args...)

	var b strings.Builder
	b.Grow(len(sqlWithParams) + 20)
	b.WriteString("SELECT TOP 1 1 FROM ")
	b.WriteString(sqlWithParams)
	b.WriteByte(';')
	sqlq := b.String()

	var cnt int
	if tx != nil {
		err := tx.QueryRowContext(dh.ctx, sqlq, args...).Scan(&cnt)
		if err != nil {
			if !errors.Is(err, dhl.ErrNoRows) {
				dh.setDHErr(fmt.Errorf("exists: %w", err))
				return false, dh.err
			}
		}
		return cnt == 1, nil
	}
	err := hndl.DB().QueryRowContext(dh.ctx, sqlq, args...).Scan(&cnt)
	if err != nil {
		if !errors.Is(err, dhl.ErrNoRows) {
			dh.setDHErr(fmt.Errorf("exists: %w", err))
			return false, dh.err
		}
	}
	return cnt == 1, nil
}

// ExistsExt a set of validation expression against the underlying database table
func (dh *SQLServerHelper) ExistsExt(tableName string, values []dhl.ColumnFilter) (Valid bool, Error error) {
	dh.rw.RLock()
	herr, hndl := dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return false, herr
	}
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("existsext: %w", dhl.ErrHandleNotSet))
		return false, dh.err
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

	sqlq = dhl.InterpolateTable(`SELECT CAST(CASE WHEN (SELECT TOP(1) 1 FROM `+tableNameWithParameters+`) = 1 THEN 1 ELSE 0 END AS BIT);`, schema)
	err := dh.QueryRow(sqlq, args...).Scan(&exists)
	if err != nil {
		if !errors.Is(err, dhl.ErrNoRows) {
			dh.rw.Lock()
			dh.err = fmt.Errorf("existsext: %w", err)
			dh.rw.Unlock()
			return false, dh.err
		}
		return false, nil
	}

	return exists, nil
}

// Next gets the next serial number
func (dh *SQLServerHelper) Next(serial string, next *int64) error {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return herr
	}

	if hndl == nil {
		dh.setDHErr(fmt.Errorf("next: %w", dhl.ErrHandleNotSet))
		return dh.err
	}

	if next == nil {
		dh.setDHErr(fmt.Errorf("next: %w", dhl.ErrVarMustBeInit))
		return dh.err
	}

	di := hndl.DI()
	if di == nil {
		dh.setDHErr(errors.New("next: database info not configured"))
		return dh.err
	}

	schema := "dbo"
	if di.Schema != nil && *di.Schema != "" {
		schema = *di.Schema
	}

	// if the database config has set a sequence generator, this will use it
	sg := di.SequenceGenerator
	if sg != nil {
		if sg.NamePlaceHolder == "" {
			dh.setDHErr(
				errors.New(`next: name place holder should be provided. ` +
					`Set name place holder in {placeholder} format. ` +
					`Place holder name should also be present in the upsert or select query`))
			return dh.err
		}
		if sg.ResultQuery == "" {
			dh.setDHErr(errors.New(`next: result query must be provided`))
			return dh.err
		}
		// Upsert is usually an insert or an update, so we execute it.
		// It is optional when all queries are set in the result query.
		// affr (affected rows) must be at least 1 to proceed
		affr := int64(1)
		var err error
		if sg.UpsertQuery != "" {
			var sqr sql.Result
			sqlq := dhl.InterpolateTable(strings.ReplaceAll(sg.UpsertQuery, sg.NamePlaceHolder, serial), schema)
			if tx != nil {
				sqr, err = tx.ExecContext(dh.ctx, sqlq)
			} else {
				sqr, err = hndl.DB().ExecContext(dh.ctx, sqlq)
			}
			if err != nil {
				dh.setDHErr(fmt.Errorf("next: %w", err))
				return dh.err
			}
			affr, _ = sqr.RowsAffected()
		}
		// in the event that the upsert alters the affr variable to 0, we return an error
		if affr == 0 {
			dh.setDHErr(errors.New(`next: upsert query did not insert or update any records`))
			return dh.err
		}
		// result query needs a single scalar value to be returned
		sqlq := dhl.InterpolateTable(strings.ReplaceAll(sg.ResultQuery, sg.NamePlaceHolder, serial), schema)
		if tx != nil {
			err = tx.QueryRowContext(dh.ctx, sqlq).Scan(next)
		} else {
			err = hndl.DB().QueryRowContext(dh.ctx, sqlq).Scan(next)
		}
		if err != nil {
			dh.setDHErr(fmt.Errorf("next: %w", err))
			return dh.err
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
		return hndl.DB().QueryRowContext(dh.ctx, sqlq).Scan(next)
	}
	if err := scan(); err == nil {
		return nil
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
					return fmt.Errorf("next: %w", err2)
				}
			} else {
				if _, err2 := hndl.DB().ExecContext(dh.ctx, ddl); err2 != nil {
					return fmt.Errorf("next: %w", err2)
				}
			}
			return scan() // retry
		}
		return fmt.Errorf("next: %w", err) // some other error
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
func (dh *SQLServerHelper) Ping() error {
	dh.rw.RLock()
	hndl, ctx := dh.hndl, dh.ctx
	dh.rw.RUnlock()
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("ping: %w", dhl.ErrHandleNotSet))
		return dh.err
	}
	return hndl.DB().PingContext(ctx)
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

func (dh *SQLServerHelper) setDHErr(err error) {
	dh.rw.Lock()
	dh.err = err
	dh.rw.Unlock()
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
