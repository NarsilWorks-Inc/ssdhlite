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

	dhl "github.com/NarsilWorks-Inc/datahelperlite"
	mssql "github.com/denisenkom/go-mssqldb"
	cfg "github.com/eaglebush/config"
)

// SQLServerHelper implements DataHelperLite
type SQLServerHelper struct {
	db  *sql.DB
	tx  *sql.Tx
	dbi *cfg.DatabaseInfo
	ctx context.Context
	trCnt,
	reuseCnt uint8
	rw        sync.RWMutex
	txInst    map[uint8]uint8
	txInstIdx uint8
	conn      *sql.Conn
	err       error
}

func init() {
	dhl.SetHelper(`ssdhlite`, &SQLServerHelper{})
	dhl.SetErrNoRows(sql.ErrNoRows)
}

// NewHelper instantiates new helper
func (h *SQLServerHelper) NewHelper() dhl.DataHelperLite {
	return &SQLServerHelper{
		txInst:    make(map[uint8]uint8),
		txInstIdx: 0,
	}
}

// Open a new connection
func (h *SQLServerHelper) Open(ctx context.Context, di *cfg.DatabaseInfo) error {
	h.err = nil
	h.txInst = map[uint8]uint8{}
	h.txInstIdx = 0
	h.dbi = di
	if ctx == nil {
		ctx = context.Background()
	}
	h.ctx = ctx

	if !(h.db == nil || h.conn == nil) {
		h.rw.Lock()
		h.reuseCnt++
		h.rw.Unlock()
		return nil
	}

	h.db, h.err = sql.Open(`sqlserver`, di.ConnectionString)
	if h.err != nil {
		return fmt.Errorf("open: %w", h.err)
	}
	if di.MaxOpenConnection != nil {
		h.db.SetMaxOpenConns(*di.MaxOpenConnection)
	}
	if di.MaxIdleConnection != nil {
		h.db.SetMaxIdleConns(*di.MaxIdleConnection)
	}
	if di.MaxConnectionLifetime != nil {
		h.db.SetConnMaxLifetime(time.Duration(*di.MaxConnectionLifetime))
	}
	if di.MaxConnectionIdleTime != nil {
		h.db.SetConnMaxIdleTime(time.Duration(*di.MaxConnectionIdleTime))
	}
	h.conn, h.err = h.db.Conn(h.ctx)
	if h.err != nil {
		return fmt.Errorf("open: %w", h.err)
	}
	h.rw.Lock()
	h.reuseCnt = 0
	h.rw.Unlock()
	return nil
}

// Close the helper
func (h *SQLServerHelper) Close() error {
	if h.db == nil && h.conn == nil {
		return nil
	}
	// if reused, closing will be prevented
	// until reusing is zero
	if h.reuseCnt > 0 {
		h.rw.Lock()
		h.reuseCnt--
		h.rw.Unlock()
		return nil
	}
	// check if transaction exists
	if h.tx != nil {
		h.Rollback()
	}
	if h.err = h.conn.Close(); h.err != nil {
		h.db = nil
		h.conn = nil
		return h.err
	}
	if h.err = h.db.Close(); h.err != nil {
		h.db = nil
		h.conn = nil
		return h.err
	}
	h.rw.Lock()
	h.trCnt = 0
	h.db = nil
	h.conn = nil
	h.err = nil
	h.rw.Unlock()
	return nil
}

// Begin a transaction. If there is an existing transaction, begin is ignored
func (h *SQLServerHelper) Begin() error {
	if h.err != nil {
		return h.err
	}
	if h.db == nil || h.conn == nil {
		return dhl.ErrNoConn
	}
	if h.tx == nil {
		h.tx, h.err = h.conn.BeginTx(h.ctx, &sql.TxOptions{})
		if h.err != nil {
			return fmt.Errorf("begin: %w", h.err)
		}
	}
	// Increment transaction count
	// The transaction count will serve as the key for the new map value, set to 1
	// Move the new index to the forward position
	h.rw.Lock()
	h.trCnt++
	h.txInst[h.trCnt] = 1
	h.txInstIdx = h.trCnt
	h.rw.Unlock()
	return nil
}

// Commit a transaction
func (h *SQLServerHelper) Commit() error {

	// if h.err != nil {
	// 	return h.Rollback()
	// }

	// txInst is used to identify the current transaction
	// If the current tx index (txInstIdx) is not found on the map,
	// or the flag was set to 0, it will not do anything
	flag, ok := h.txInst[h.txInstIdx]
	if !ok {
		return nil
	}
	// Move down one transaction instance since we can't find this
	if flag == 0 {
		h.rw.Lock()
		h.txInstIdx--
		h.rw.Unlock()
		return nil
	}

	// If the transaction count is greater than 1, this is reused, exit.
	if h.trCnt > 1 {
		h.rw.Lock()
		h.trCnt--                 // Deduct from transaction count
		h.txInst[h.txInstIdx] = 0 // Set flag to 0 to indicate the current instance has been called
		h.rw.Unlock()
		return nil
	}

	// If the db and connection is not set, return an error
	// If the transaction is not set, return an error
	if h.db == nil || h.conn == nil {
		return fmt.Errorf("commit: %w", dhl.ErrNoConn)
	}
	if h.tx == nil {
		return fmt.Errorf("commit: %w", dhl.ErrNoTx)
	}

	// If this is the outer transaction, commit
	if h.trCnt == 1 {
		if h.err = h.tx.Commit(); h.err != nil {
			if !errors.Is(h.err, sql.ErrTxDone) {
				return fmt.Errorf("commit: %w", h.err)
			}
		}
	}

	// Reset all transaction logs
	h.rw.Lock()
	h.tx = nil
	h.trCnt = 0
	h.txInstIdx = 0
	h.txInst = make(map[uint8]uint8)
	h.rw.Unlock()
	return nil
}

// Rollback a transaction.
func (h *SQLServerHelper) Rollback() error {

	// If any of the queries have encountered error, rollback
	if h.err != nil {
		h.rw.Lock()
		h.trCnt = 1
		h.rw.Unlock()
	} else {
		// txInst is used to identify the current transaction
		// If the current tx index (txInstIdx) is not found on the map,
		// or the flag was set to 0, it will not do anything
		flag, ok := h.txInst[h.txInstIdx]
		if !ok {
			return nil
		}

		// Move down one transaction instance since we can't find this
		if flag == 0 {
			h.rw.Lock()
			h.txInstIdx--
			h.rw.Unlock()
			return nil
		}

		// If the transaction count is greater than 1, this is reused, exit.
		if h.trCnt > 1 {
			h.rw.Lock()
			h.trCnt--                 // Deduct from transaction count
			h.txInst[h.txInstIdx] = 0 // Set flag to 0 to indicate the current index has been called
			h.rw.Unlock()
			return nil
		}
	}

	// If the db and connection is not set, return an error
	// If the transaction is not set, return an error
	if h.db == nil || h.conn == nil {
		return fmt.Errorf("rollback: %w", dhl.ErrNoConn)
	}
	if h.tx == nil {
		return fmt.Errorf("rollback: %w", dhl.ErrNoTx)
	}

	// If this is the outer transaction, rollback
	if h.trCnt == 1 {
		if h.err = h.tx.Rollback(); h.err != nil {
			if !errors.Is(h.err, sql.ErrTxDone) {
				return fmt.Errorf("rollback: %w", h.err)
			}
		}
	}

	// Reset all transaction logs
	h.rw.Lock()
	h.tx = nil
	h.trCnt = 0
	h.txInstIdx = 0
	h.txInst = make(map[uint8]uint8)
	h.rw.Unlock()
	return nil
}

// Mark a savepoint
func (h *SQLServerHelper) Mark(name string) error {
	if h.err != nil {
		return h.err
	}
	if h.db == nil || h.conn == nil {
		return fmt.Errorf("mark: %w", dhl.ErrNoConn)
	}
	if h.tx == nil {
		return fmt.Errorf("rollback: %w", dhl.ErrNoTx)
	}
	if h.trCnt > 0 {
		_, h.err = h.tx.ExecContext(h.ctx, `SAVE TRAN sp_`+name+`;`)
	}
	return fmt.Errorf("mark: %w", h.err)
}

// Discard a savepoint
func (h *SQLServerHelper) Discard(name string) error {
	if h.err != nil {
		return h.err
	}
	if h.db == nil || h.conn == nil {
		return dhl.ErrNoConn
	}
	if h.tx == nil {
		return dhl.ErrNoTx
	}
	if h.trCnt > 0 {
		_, h.err = h.tx.ExecContext(h.ctx, `ROLLBACK TRAN sp_`+name+`;`)
	}
	return fmt.Errorf("discard: %w", h.err)
}

// Save a savepoint
func (h *SQLServerHelper) Save(name string) error {
	if h.err != nil {
		return h.err
	}
	if h.db == nil || h.conn == nil {
		return fmt.Errorf("save: %w", dhl.ErrNoConn)
	}
	if h.tx == nil {
		return fmt.Errorf("save: %w", dhl.ErrNoTx)
	}
	if h.trCnt > 0 {
		_, h.err = h.tx.ExecContext(h.ctx, `COMMIT TRAN sp_`+name+`;`)
	}
	return fmt.Errorf("save: %w", h.err)
}

// Query retrieves rows from database
func (h *SQLServerHelper) Query(querySql string, args ...interface{}) (dhl.Rows, error) {
	var (
		sqr *sql.Rows
	)
	if h.err != nil {
		return nil, h.err
	}
	if h.db == nil || h.conn == nil {
		return nil, fmt.Errorf("query: %w", dhl.ErrNoConn)
	}
	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.ReplaceQueryParamMarker(querySql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
	// replace tables meant for interpolation {table} for putting the schema
	querySql = dhl.InterpolateTable(querySql, h.dbi.Schema)
	args = refineParameters(args...)
	if h.tx != nil {
		sqr, h.err = h.tx.QueryContext(h.ctx, querySql, args...)
	} else {
		sqr, h.err = h.conn.QueryContext(h.ctx, querySql, args...)
	}
	if h.err != nil {
		return nil, fmt.Errorf("query: %w", h.err)
	}
	if sqr == nil {
		return nil, fmt.Errorf("query: %w", dhl.ErrNoConn)
	}
	return NewSQLServerRows(sqr), nil
}

// QueryArray puts the single column result to an output array
func (h *SQLServerHelper) QueryArray(querySql string, out interface{}, args ...interface{}) error {

	var (
		sqr *sql.Rows
	)
	if h.err != nil {
		return h.err
	}

	switch out.(type) {
	case *[]string, *[]int, *[]int8, *[]int16, *[]int32, *[]int64, *[]bool, *[]float32, *[]float64:
	case *[]time.Time:
	default:
		return fmt.Errorf("queryarray: %w", dhl.ErrArrayTypeNotSupported)
	}
	if h.db == nil || h.conn == nil {
		return fmt.Errorf("queryarray: %w", dhl.ErrNoConn)
	}
	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.ReplaceQueryParamMarker(querySql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
	// replace tables meant for interpolation {table} for putting the schema
	querySql = dhl.InterpolateTable(querySql, h.dbi.Schema)
	args = refineParameters(args...)
	if h.tx != nil {
		sqr, h.err = h.tx.QueryContext(h.ctx, querySql, args...)
	} else {
		sqr, h.err = h.conn.QueryContext(h.ctx, querySql, args...)
	}
	if h.err != nil {
		return fmt.Errorf("queryarray: %w", h.err)
	}
	if sqr == nil {
		return fmt.Errorf("queryarray: %w", dhl.ErrNoConn)
	}
	defer sqr.Close()

	switch t := out.(type) {
	case *[]string:
		idx := 0
		if t == nil {
			t = new([]string)
		}
		for sqr.Next() {
			*t = append(*t, "")
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				return fmt.Errorf("queryarray: %w", h.err)
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			return fmt.Errorf("queryarray: %w", h.err)
		}
		_ = t
	case *[]int:
		idx := 0
		if t == nil {
			t = new([]int)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				return fmt.Errorf("queryarray: %w", h.err)
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			return fmt.Errorf("queryarray: %w", h.err)
		}
		_ = t
	case *[]int8:
		idx := 0
		if t == nil {
			t = new([]int8)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				return fmt.Errorf("queryarray: %w", h.err)
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			return fmt.Errorf("queryarray: %w", h.err)
		}
		_ = t
	case *[]int16:
		idx := 0
		if t == nil {
			t = new([]int16)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				return fmt.Errorf("queryarray: %w", h.err)
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			return fmt.Errorf("queryarray: %w", h.err)
		}
		_ = t
	case *[]int32:
		idx := 0
		if t == nil {
			t = new([]int32)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				return fmt.Errorf("queryarray: %w", h.err)
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			return fmt.Errorf("queryarray: %w", h.err)
		}
		_ = t
	case *[]int64:
		idx := 0
		if t == nil {
			t = new([]int64)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				return fmt.Errorf("queryarray: %w", h.err)
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			return fmt.Errorf("queryarray: %w", h.err)
		}
		_ = t
	case *[]bool:
		idx := 0
		if t == nil {
			t = new([]bool)
		}
		for sqr.Next() {
			*t = append(*t, false)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				return fmt.Errorf("queryarray: %w", h.err)
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			return fmt.Errorf("queryarray: %w", h.err)
		}
		_ = t
	case *[]float32:
		idx := 0
		if t == nil {
			t = new([]float32)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				return fmt.Errorf("queryarray: %w", h.err)
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			return fmt.Errorf("queryarray: %w", h.err)
		}
		_ = t
	case *[]float64:
		idx := 0
		if t == nil {
			t = new([]float64)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				return fmt.Errorf("queryarray: %w", h.err)
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			return fmt.Errorf("queryarray: %w", h.err)
		}
		_ = t
	case *[]time.Time:
		idx := 0
		if t == nil {
			t = new([]time.Time)
		}
		for sqr.Next() {
			*t = append(*t, time.Time{})
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				return fmt.Errorf("queryarray: %w", h.err)
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			return fmt.Errorf("queryarray: %w", h.err)
		}
		_ = t
	}
	return nil
}

// func (h *SQLServerHelper) QueryPaged(param pgr.Parameter, querySql string, args ...interface{}) (dhl.Rows, error) {
// 	var (
// 		hasPId bool
// 		b      []byte
// 		err    error
// 		rws    dhl.Rows
// 		pgc    int
// 	)

// 	if h.pager == nil {
// 		return nil, dhl.ErrNoPagerSet
// 	}
// 	if h.pageSize == 0 {
// 		h.pageSize = 25
// 	}
// 	hasPId = param.PageID != ""
// 	if !hasPId {
// 		param.PageID = ksuid.New().String()
// 	}
// 	if param.PageNumber == 0 {
// 		param.PageNumber = 1
// 	}

// 	// If there is no pager id, we'll retrieve
// 	if hasPId {
// 		b, err = (*h.pager).Fetch(param.PageID, 0)
// 		if err != nil {
// 			return nil, err
// 		}
// 	}
// 	// If cache is empty, we get the records by querying
// 	if len(b) == 0 {
// 		rws, err = h.Query(querySql, args...)
// 		if err != nil {
// 			return nil, err
// 		}
// 		defer rws.Close()

// 		cols, err := rws.Columns()
// 		if err != nil {
// 			return nil, err
// 		}
// 		rc := 0
// 		rows := make([]map[string]interface{}, 0)
// 		for rws.Next() {
// 			row := make([]interface{}, len(cols))
// 			rowPtr := make([]interface{}, len(cols))
// 			for i := range row {
// 				rowPtr[i] = &row[i]
// 			}
// 			if err = rws.Scan(rowPtr...); err != nil {
// 				return nil, err
// 			}
// 			m := make(map[string]interface{})
// 			for i := range cols {
// 				// TODO: Check if a key is already present
// 				m[cols[i].Name()] = row[i]
// 			}
// 			rows = append(rows, m)
// 			rc += 1

// 			// If row count is greater than page size
// 			// store, reset slice and rc
// 			if rc >= h.pageSize {
// 				b, err = json.Marshal(rows)
// 				if err != nil {
// 					return nil, err
// 				}
// 				pgc += 1
// 				if err = (*h.pager).Store(param.PageID, pgc, b); err != nil {
// 					return nil, err
// 				}
// 				rows = nil
// 				rc = 0
// 			}
// 		}
// 		if err = rws.Err(); err != nil {
// 			return nil, err
// 		}

// 		// Store the remaining records
// 		if rc > 0 {
// 			b, err = json.Marshal(rows)
// 			if err != nil {
// 				return nil, err
// 			}
// 			pgc += 1
// 			if err = (*h.pager).Store(param.PageID, pgc, b); err != nil {
// 				return nil, err
// 			}
// 			rows = nil
// 			rc = 0
// 		}
// 		// Store page count
// 		if err = (*h.pager).Store(param.PageID, 0, []byte(fmt.Sprintf("%d", pgc))); err != nil {
// 			return nil, err
// 		}
// 	}

// 	// Convert page count
// 	pgc, _ = strconv.Atoi(string(b))

// 	// process b

// 	return nil, nil
// }

// QueryRow retrieves a single row from a query
func (h *SQLServerHelper) QueryRow(querySql string, args ...interface{}) dhl.Row {
	if h.err != nil {
		return nil
	}
	if h.db == nil || h.conn == nil {
		return nil
	}
	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.ReplaceQueryParamMarker(querySql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
	querySql = dhl.InterpolateTable(querySql, h.dbi.Schema)
	args = refineParameters(args...)
	if h.tx != nil {
		return NewSQLServerRow(h.tx.QueryRowContext(h.ctx, querySql, args...))
	}
	return NewSQLServerRow(h.conn.QueryRowContext(h.ctx, querySql, args...))
}

// Exec executes data manipulation command and returns the number of affected rows
func (h *SQLServerHelper) Exec(querySql string, args ...interface{}) (int64, error) {

	var (
		ra int64
		sq sql.Result
	)
	if h.err != nil {
		return 0, h.err
	}
	if h.db == nil || h.conn == nil {
		return 0, fmt.Errorf("exec: %w", dhl.ErrNoConn)
	}
	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.ReplaceQueryParamMarker(querySql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
	querySql = dhl.InterpolateTable(querySql, h.dbi.Schema)
	args = refineParameters(args...)
	if h.tx != nil {
		sq, h.err = h.tx.ExecContext(h.ctx, querySql, args...)
	} else {
		sq, h.err = h.conn.ExecContext(h.ctx, querySql, args...)
	}
	if h.err != nil {
		return 0, fmt.Errorf("exec: %w", h.err)
	}
	ra, _ = sq.RowsAffected()
	return ra, nil
}

// Exists checks if a record exist
func (h *SQLServerHelper) Exists(sqlWithParams string, args ...interface{}) (bool, error) {

	var (
		cnt int
		sql string
	)
	if h.err != nil {
		return false, h.err
	}
	if h.db == nil || h.conn == nil {
		return false, nil
	}
	// replace question mark (?) parameter with configured query parameter, if there are any
	sqlWithParams = dhl.ReplaceQueryParamMarker(sqlWithParams, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
	sqlWithParams = dhl.InterpolateTable(sqlWithParams, h.dbi.Schema)
	args = refineParameters(args...)
	sql = `SELECT TOP 1 1 FROM ` + sqlWithParams + `;`
	if h.tx != nil {
		h.err = h.tx.QueryRowContext(h.ctx, sql, args...).Scan(&cnt)
		if errors.Is(h.err, dhl.ErrNoRows) {
			return false, nil
		}
		if h.err != nil {
			return false, fmt.Errorf("exists: %w", h.err)
		}
		return cnt == 1, nil
	}
	h.err = h.conn.QueryRowContext(h.ctx, sql, args...).Scan(&cnt)
	if errors.Is(h.err, dhl.ErrNoRows) {
		return false, nil
	}
	if h.err != nil {
		return false, fmt.Errorf("exists: %w", h.err)
	}
	return cnt == 1, nil
}

// Next gets the next serial number
func (h *SQLServerHelper) Next(serial string, next *int64) error {

	var (
		sql  string
		affr int64
	)
	if h.err != nil {
		return h.err
	}
	if next == nil {
		return fmt.Errorf("next: %w", dhl.ErrVarMustBeInit)
	}
	// if the database config has set a sequence generator, this will use it
	sg := h.dbi.SequenceGenerator
	if sg != nil {
		if sg.NamePlaceHolder == "" {
			return errors.New(`next: name place holder should be provided. ` +
				`Set name place holder in {placeholder} format. ` +
				`Place holder name should also be present in the upsert or select query`)
		}
		if sg.ResultQuery == "" {
			return errors.New(`next: result query must be provided`)
		}
		// Upsert is usually an insert or an update, so we execute it.
		// It is optional when all queries are set in the result query.
		// affr (affected rows) must be at least 1 to proceed
		affr = 1
		if sg.UpsertQuery != "" {
			sql = strings.ReplaceAll(sg.UpsertQuery, sg.NamePlaceHolder, serial)
			affr, h.err = h.Exec(sql)
			if h.err != nil {
				return fmt.Errorf("next: %w", h.err)
			}
		}
		// in the event that the upsert alters the affr variable to 0, we return an error
		if affr == 0 {
			return errors.New(`next: upsert query did not insert or update any records`)
		}
		// result query needs a single scalar value to be returned
		sql = strings.ReplaceAll(sg.ResultQuery, sg.NamePlaceHolder, serial)
		h.err = h.QueryRow(sql).Scan(next)
		if h.err != nil {
			return fmt.Errorf("next: %w", h.err)
		}
		return nil
	}

	// if the sequence generator was not set, we use the sequence (SQL Server 2012 and later)
	sql = fmt.Sprintf("SELECT NEXT VALUE FOR %s;", h.Escape(serial))
	h.err = h.QueryRow(sql).Scan(next)
	if h.err != nil {
		return fmt.Errorf("next: %w", h.err)
	}
	return nil
}

// VerifyWithin a set of validation expression against the underlying database table
func (h *SQLServerHelper) VerifyWithin(tableName string, values []dhl.VerifyExpression) (Valid bool, Error error) {
	if h.err != nil {
		return false, h.err
	}
	if h.db == nil || h.conn == nil {
		return false, fmt.Errorf("verify: %w", dhl.ErrNoConn)
	}
	tableNameWithParameters := tableName
	args := make([]interface{}, len(values))
	i := 0
	andstr := ""
	placeholder := h.dbi.ParameterPlaceholder
	if len(values) > 0 {
		tableNameWithParameters += ` WHERE `
	}
	for _, v := range values {
		if h.dbi.ParameterInSequence {
			placeholder = h.dbi.ParameterPlaceholder + strconv.Itoa(i+1)
		}
		// If there is no operator, we default to "="
		if v.Operator == "" {
			v.Operator = "="
		}
		if v.Value == nil {
			v.Operator = " IS "
		}
		tableNameWithParameters += andstr + v.Name + v.Operator + placeholder
		args[i] = v.Value
		i++
		andstr = " AND "
	}

	var (
		sql    string
		exists bool
	)

	args = refineParameters(args...)
	sql = `SELECT CAST(CASE WHEN (SELECT TOP(1) 1 FROM ` + tableNameWithParameters + `) = 1 THEN 1 ELSE 0 END AS BIT);`
	h.err = h.QueryRow(sql, args...).Scan(&exists)
	if h.err != nil {
		if !errors.Is(h.err, dhl.ErrNoRows) {
			return false, fmt.Errorf("verify: %w", h.err)
		}
		return false, nil
	}

	return exists, nil
}

// Escape a field value (fv) from disruption by single quote
func (h *SQLServerHelper) Escape(fv string) string {
	if len(fv) == 0 {
		return ""
	}
	senc := *h.dbi.StringEnclosingChar
	sesc := *h.dbi.StringEscapeChar
	if len(senc) == 0 {
		senc = `'`
	}
	if len(sesc) == 0 {
		sesc = `'`
	}
	return strings.ReplaceAll(fv, senc, sesc+sesc)
}

// DatabaseVersion returns database version
func (h *SQLServerHelper) DatabaseVersion() string {
	var (
		version string
	)
	h.err = h.QueryRow(`SELECT @@VERSION;`).Scan(&version)
	if h.err != nil {
		version = h.err.Error()
	}
	return version
}

// Now gets the current server date
func (h *SQLServerHelper) Now() *time.Time {
	var tm time.Time
	h.err = h.QueryRow(`SELECT GETDATE();`).Scan(&tm)
	if h.err != nil {
		tm = time.Now()
		return &tm
	}
	return &tm
}

// NowUTC gets the current server date in UTC
func (h *SQLServerHelper) NowUTC() *time.Time {
	var tm time.Time
	h.err = h.QueryRow(`SELECT GETUTCDATE();`).Scan(&tm)
	if h.err != nil {
		tm = time.Now().UTC()
		return &tm
	}
	return &tm
}

// refineParameters sets the built-in type of the datahelper-specified parameter type
// to mssql parameter type. The default type for strings is nvarchar
func refineParameters(args ...interface{}) []interface{} {
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
