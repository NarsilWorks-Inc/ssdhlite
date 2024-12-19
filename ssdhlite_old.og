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
	"github.com/segmentio/ksuid"
)

// SQLServerHelper - a struct derived from datahelperlite
type SQLServerHelper struct {
	db  *sql.DB
	tx  *sql.Tx
	dbi *cfg.DatabaseInfo
	ctx context.Context
	trCnt,
	reuseCnt int
	trnmap     map[string]string
	closemu    sync.RWMutex
	instanceId string
	conn       *sql.Conn
}

func init() {
	dhl.SetHelper(`ssdhlite`, &SQLServerHelper{})
	dhl.SetErrNoRows(sql.ErrNoRows)
}

// NewHelper instantiates new helper
func (h *SQLServerHelper) NewHelper() dhl.DataHelperLite {
	return &SQLServerHelper{
		trnmap: make(map[string]string),
	}
}

// Open a new connection
func (h *SQLServerHelper) Open(ctx context.Context, di *cfg.DatabaseInfo) error {
	var (
		err error
	)
	if ctx == nil {
		ctx = context.Background()
	}
	h.dbi = di
	h.ctx = ctx
	if h.db == nil || h.conn == nil {
		h.db, err = sql.Open(`sqlserver`, di.ConnectionString)
		if err != nil {
			return err
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
		h.conn, err = h.db.Conn(h.ctx)
		if err != nil {
			return err
		}
		h.closemu.Lock()
		h.reuseCnt = 0
	} else {
		h.closemu.Lock()
		h.reuseCnt++
	}
	h.closemu.Unlock()
	h.instanceId = ksuid.New().String()
	return nil
}

// Close the helper
func (h *SQLServerHelper) Close() error {
	if h.db == nil && h.conn == nil {
		return dhl.ErrNoConn
	}
	// if reused, closing will be prevented
	// until reusing is zero
	if h.reuseCnt > 0 {
		h.closemu.Lock()
		h.reuseCnt--
		h.closemu.Unlock()
		return nil
	}
	// check if transaction exists
	if h.tx != nil {
		h.Rollback()
	}
	if err := h.conn.Close(); err != nil {
		h.db = nil
		h.conn = nil
		return err
	}
	if err := h.db.Close(); err != nil {
		h.db = nil
		h.conn = nil
		return err
	}
	h.closemu.Lock()
	defer h.closemu.Unlock()
	h.trCnt = 0
	h.db = nil
	h.conn = nil
	return nil
}

// Begin a transaction. If there is an existing transaction, begin is ignored
func (h *SQLServerHelper) Begin() error {
	var (
		err error
	)
	if h.db == nil || h.conn == nil {
		return dhl.ErrNoConn
	}
	if h.tx == nil {
		h.tx, err = h.conn.BeginTx(h.ctx, &sql.TxOptions{})
		if err != nil {
			return err
		}
	}
	h.closemu.Lock()
	h.trCnt++
	h.closemu.Unlock()
	return nil
}

// BeginDR begins a transaction with a transaction id
// also stored in to a local map or list. It will
// be useful if used in a deferred rollback setup
func (h *SQLServerHelper) BeginDR() (string, error) {
	tranid := ksuid.New().String()
	h.trnmap[tranid] = `OK`
	return tranid, h.Begin()
}

// Commit a transaction. The tranid argument is supplied
// using the BeginDR() function.
func (h *SQLServerHelper) Commit(tranid ...string) error {

	// tranid is used to identify the current transaction
	// if the coding style used is deferring rollback
	// after Begin() is called, this would solve the
	// problem of rolled back transaction in reusablity mode

	// If the tranid is not found on the map, it will
	// not take any action
	if len(tranid) > 0 {
		if _, ok := h.trnmap[tranid[0]]; !ok {
			return nil
		}
		// the key is deleted after calling Commit
		defer delete(h.trnmap, tranid[0])
	}

	// exit if the connection was just reused
	if h.trCnt > 1 {
		h.closemu.Lock()
		defer h.closemu.Unlock()
		h.trCnt-- // deduct from transaction count
		return nil
	}
	if h.db == nil || h.conn == nil {
		return dhl.ErrNoConn
	}
	if h.tx == nil {
		return dhl.ErrNoTx
	}
	if h.trCnt == 1 {
		if err := h.tx.Commit(); err != nil {
			if !errors.Is(err, sql.ErrTxDone) {
				return err
			}
			h.tx = nil
			return err
		}
	}
	// decrement transaction
	h.closemu.Lock()
	defer h.closemu.Unlock()
	if h.trCnt > 0 {
		h.trCnt--
	}
	// if trancount is zero, we can set the tx to nil
	if h.trCnt == 0 {
		h.tx = nil
	}
	return nil
}

// Rollback a transaction. The tranid argument is supplied
// using the BeginDR() function.
func (h *SQLServerHelper) Rollback(tranid ...string) error {

	// tranid is used to identify the current transaction
	// if the coding style used is deferring rollback
	// after Begin() is called, this would solve the
	// problem of rolled back transaction in reusablity mode

	// If the tranid is not found on the map, it will
	// not take any action
	if len(tranid) > 0 {
		if _, ok := h.trnmap[tranid[0]]; !ok {
			return nil
		}
		// the tranid is deleted when Rollback() is called
		defer delete(h.trnmap, tranid[0])
	}

	//log.Printf("Rollback TranCount (%s): %d", h.instanceID, h.trcnt)

	// exit if the connection was just reused
	if h.trCnt > 1 {
		h.closemu.Lock()
		defer h.closemu.Unlock()
		h.trCnt-- // deduct from transaction count
		return nil
	}
	if h.db == nil || h.conn == nil {
		return dhl.ErrNoConn
	}
	if h.tx == nil {
		return dhl.ErrNoTx
	}
	if h.trCnt == 1 {
		if err := h.tx.Rollback(); err != nil {
			if !errors.Is(err, sql.ErrTxDone) {
				return err
			}
			h.tx = nil
			return err
		}
	}
	// decrement transaction
	h.closemu.Lock()
	defer h.closemu.Unlock()
	if h.trCnt > 0 {
		h.trCnt--
	}
	// if trancount is zero, we can set the tx to nil
	if h.trCnt == 0 {
		h.tx = nil
	}
	return nil
}

// Mark a savepoint
func (h *SQLServerHelper) Mark(name string) error {
	var err error
	if h.db == nil || h.conn == nil {
		return dhl.ErrNoConn
	}
	if h.tx == nil {
		return dhl.ErrNoTx
	}
	if h.trCnt > 0 {
		_, err = h.tx.ExecContext(h.ctx, `SAVE TRAN sp_`+name+`;`)
	}
	return err
}

// Discard a savepoint
func (h *SQLServerHelper) Discard(name string) error {
	var err error
	if h.db == nil || h.conn == nil {
		return dhl.ErrNoConn
	}
	if h.tx == nil {
		return dhl.ErrNoTx
	}
	if h.trCnt > 0 {
		_, err = h.tx.ExecContext(h.ctx, `ROLLBACK TRAN sp_`+name+`;`)
	}
	return err
}

// Save a savepoint
func (h *SQLServerHelper) Save(name string) error {
	var err error
	if h.db == nil || h.conn == nil {
		return dhl.ErrNoConn
	}
	if h.tx == nil {
		return dhl.ErrNoTx
	}
	if h.trCnt > 0 {
		_, err = h.tx.ExecContext(h.ctx, `COMMIT TRAN sp_`+name+`;`)
	}
	return err
}

// Query retrieves rows from database
func (h *SQLServerHelper) Query(querySql string, args ...interface{}) (dhl.Rows, error) {
	var (
		err error
		sqr *sql.Rows
	)
	if h.db == nil || h.conn == nil {
		return nil, dhl.ErrNoConn
	}
	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.ReplaceQueryParamMarker(querySql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
	// replace tables meant for interpolation {table} for putting the schema
	querySql = dhl.InterpolateTable(querySql, h.dbi.Schema)
	args = refineParameters(args...)
	if h.tx != nil {
		sqr, err = h.tx.QueryContext(h.ctx, querySql, args...)
	} else {
		sqr, err = h.conn.QueryContext(h.ctx, querySql, args...)
	}
	if err != nil {
		return nil, err
	}
	if sqr == nil {
		return nil, dhl.ErrNoConn
	}
	return NewSQLServerRows(sqr), nil
}

// QueryArray puts the single column result to an output array
func (h *SQLServerHelper) QueryArray(querySql string, out interface{}, args ...interface{}) error {

	var (
		err error
		sqr *sql.Rows
	)

	switch out.(type) {
	case *[]string, *[]int, *[]int8, *[]int16, *[]int32, *[]int64, *[]bool, *[]float32, *[]float64:
	case *[]time.Time:
	default:
		return dhl.ErrArrayTypeNotSupported
	}
	if h.db == nil || h.conn == nil {
		return dhl.ErrNoConn
	}
	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.ReplaceQueryParamMarker(querySql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
	// replace tables meant for interpolation {table} for putting the schema
	querySql = dhl.InterpolateTable(querySql, h.dbi.Schema)
	args = refineParameters(args...)
	if h.tx != nil {
		sqr, err = h.tx.QueryContext(h.ctx, querySql, args...)
	} else {
		sqr, err = h.conn.QueryContext(h.ctx, querySql, args...)
	}
	if err != nil {
		return err
	}
	if sqr == nil {
		return dhl.ErrNoConn
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
			if err = sqr.Scan(&(*t)[idx]); err != nil {
				return err
			}
			idx++
		}
		if err = sqr.Err(); err != nil {
			return err
		}
		_ = t
	case *[]int:
		idx := 0
		if t == nil {
			t = new([]int)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if err = sqr.Scan(&(*t)[idx]); err != nil {
				return err
			}
			idx++
		}
		if err = sqr.Err(); err != nil {
			return err
		}
		_ = t
	case *[]int8:
		idx := 0
		if t == nil {
			t = new([]int8)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if err = sqr.Scan(&(*t)[idx]); err != nil {
				return err
			}
			idx++
		}
		if err = sqr.Err(); err != nil {
			return err
		}
		_ = t
	case *[]int16:
		idx := 0
		if t == nil {
			t = new([]int16)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if err = sqr.Scan(&(*t)[idx]); err != nil {
				return err
			}
			idx++
		}
		if err = sqr.Err(); err != nil {
			return err
		}
		_ = t
	case *[]int32:
		idx := 0
		if t == nil {
			t = new([]int32)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if err = sqr.Scan(&(*t)[idx]); err != nil {
				return err
			}
			idx++
		}
		if err = sqr.Err(); err != nil {
			return err
		}
		_ = t
	case *[]int64:
		idx := 0
		if t == nil {
			t = new([]int64)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if err = sqr.Scan(&(*t)[idx]); err != nil {
				return err
			}
			idx++
		}
		if err = sqr.Err(); err != nil {
			return err
		}
		_ = t
	case *[]bool:
		idx := 0
		if t == nil {
			t = new([]bool)
		}
		for sqr.Next() {
			*t = append(*t, false)
			if err = sqr.Scan(&(*t)[idx]); err != nil {
				return err
			}
			idx++
		}
		if err = sqr.Err(); err != nil {
			return err
		}
		_ = t
	case *[]float32:
		idx := 0
		if t == nil {
			t = new([]float32)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if err = sqr.Scan(&(*t)[idx]); err != nil {
				return err
			}
			idx++
		}
		if err = sqr.Err(); err != nil {
			return err
		}
		_ = t
	case *[]float64:
		idx := 0
		if t == nil {
			t = new([]float64)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if err = sqr.Scan(&(*t)[idx]); err != nil {
				return err
			}
			idx++
		}
		if err = sqr.Err(); err != nil {
			return err
		}
		_ = t
	case *[]time.Time:
		idx := 0
		if t == nil {
			t = new([]time.Time)
		}
		for sqr.Next() {
			*t = append(*t, time.Time{})
			if err = sqr.Scan(&(*t)[idx]); err != nil {
				return err
			}
			idx++
		}
		if err = sqr.Err(); err != nil {
			return err
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
		err error
		ra  int64
		sq  sql.Result
	)
	if h.db == nil || h.conn == nil {
		return 0, dhl.ErrNoConn
	}
	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.ReplaceQueryParamMarker(querySql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
	querySql = dhl.InterpolateTable(querySql, h.dbi.Schema)
	args = refineParameters(args...)
	if h.tx != nil {
		sq, err = h.tx.ExecContext(h.ctx, querySql, args...)
	} else {
		sq, err = h.conn.ExecContext(h.ctx, querySql, args...)
	}
	if err != nil {
		return 0, err
	}
	ra, _ = sq.RowsAffected()
	return ra, nil
}

// Exists checks if a record exist
func (h *SQLServerHelper) Exists(sqlWithParams string, args ...interface{}) (bool, error) {

	var (
		err error
		cnt int
		sql string
	)
	if h.db == nil || h.conn == nil {
		return false, nil
	}
	// replace question mark (?) parameter with configured query parameter, if there are any
	sqlWithParams = dhl.ReplaceQueryParamMarker(sqlWithParams, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
	sqlWithParams = dhl.InterpolateTable(sqlWithParams, h.dbi.Schema)
	args = refineParameters(args...)
	sql = `SELECT TOP 1 1 FROM ` + sqlWithParams + `;`
	if h.tx != nil {
		err = h.tx.QueryRowContext(h.ctx, sql, args...).Scan(&cnt)
		if errors.Is(err, dhl.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return cnt == 1, nil
	}
	err = h.conn.QueryRowContext(h.ctx, sql, args...).Scan(&cnt)
	if errors.Is(err, dhl.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return cnt == 1, nil
}

// Next gets the next serial number
func (h *SQLServerHelper) Next(serial string, next *int64) error {

	var (
		err  error
		sql  string
		affr int64
	)
	if next == nil {
		return dhl.ErrVarMustBeInit
	}
	// if the database config has set a sequence generator, this will use it
	sg := h.dbi.SequenceGenerator
	if sg != nil {
		if sg.NamePlaceHolder == "" {
			return errors.New(`name place holder should be provided. ` +
				`Set name place holder in {placeholder} format. ` +
				`Place holder name should also be present in the upsert or select query`)
		}
		if sg.ResultQuery == "" {
			return errors.New(`nesult query must be provided`)
		}
		// Upsert is usually an insert or an update, so we execute it.
		// It is optional when all queries are set in the result query.
		// affr (affected rows) must be at least 1 to proceed
		affr = 1
		if sg.UpsertQuery != "" {
			sql = strings.ReplaceAll(sg.UpsertQuery, sg.NamePlaceHolder, serial)
			affr, err = h.Exec(sql)
			if err != nil {
				return err
			}
		}
		// in the event that the upsert alters the affr variable to 0, we return an error
		if affr == 0 {
			return errors.New(`upsert query did not insert or update any records`)
		}
		// result query needs a single scalar value to be returned
		sql = strings.ReplaceAll(sg.ResultQuery, sg.NamePlaceHolder, serial)
		err = h.QueryRow(sql).Scan(next)
		if err != nil {
			return err
		}
		return nil
	}

	// if the sequence generator was not set, we use the sequence (SQL Server 2012 and later)
	sql = fmt.Sprintf("SELECT NEXT VALUE FOR %s;", h.Escape(serial))
	err = h.QueryRow(sql).Scan(next)
	if err != nil {
		return err
	}
	return nil
}

// VerifyWithin a set of validation expression against the underlying database table
func (h *SQLServerHelper) VerifyWithin(tableName string, values []dhl.VerifyExpression) (Valid bool, Error error) {

	if h.db == nil || h.conn == nil {
		return false, dhl.ErrNoConn
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
		err    error
	)

	args = refineParameters(args...)
	sql = `SELECT CAST(CASE WHEN (SELECT TOP(1) 1 FROM ` + tableNameWithParameters + `) = 1 THEN 1 ELSE 0 END AS BIT);`
	err = h.QueryRow(sql, args...).Scan(&exists)
	if err != nil {
		if !errors.Is(err, dhl.ErrNoRows) {
			return false, err
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
		err     error
		version string
	)
	err = h.QueryRow(`SELECT @@VERSION;`).Scan(&version)
	if err != nil {
		version = err.Error()
	}
	return version
}

// Now gets the current server date
func (h *SQLServerHelper) Now() *time.Time {
	var tm time.Time
	err := h.QueryRow(`SELECT GETDATE();`).Scan(&tm)
	if err != nil {
		tm = time.Now()
		return &tm
	}
	return &tm
}

// NowUTC gets the current server date in UTC
func (h *SQLServerHelper) NowUTC() *time.Time {
	var tm time.Time
	err := h.QueryRow(`SELECT GETUTCDATE();`).Scan(&tm)
	if err != nil {
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
