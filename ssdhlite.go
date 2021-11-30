package ssdhlite

import (
	"context"
	dsql "database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	dhl "github.com/NarsilWorks-Inc/datahelperlite"

	cfg "github.com/eaglebush/config"
	std "github.com/eaglebush/stdutil"
)

// SQLServerHelper - a struct derived from datahelperlite
type SQLServerHelper struct {
	db       *dsql.DB
	tx       *dsql.Tx
	dbi      *cfg.DatabaseInfo
	ctx      context.Context
	rws      dhl.Rows
	rw       dhl.Row
	trcnt    int
	reusecnt int
}

func init() {
	dhl.SetHelper(`ssdhlite`, &SQLServerHelper{})
	dhl.SetErrNoRows(dsql.ErrNoRows)
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

	if h.db == nil {
		h.db, err = dsql.Open(`sqlserver`, di.ConnectionString)
		if err != nil {
			return err
		}
		h.reusecnt = 0
	} else {
		h.reusecnt++
	}

	return nil
}

// Close the helper
func (h *SQLServerHelper) Close() error {

	if h.db == nil {
		return dhl.ErrNoConn
	}

	// if reused, closing will be prevented
	// until reusing is zero
	if h.reusecnt > 0 {
		h.reusecnt--
		return nil
	}

	if err := h.db.Close(); err != nil {
		h.db = nil
		return err
	}

	h.trcnt = 0
	h.db = nil

	return nil
}

// Begin a transaction. If there is an existing transaction, begin is ignored
func (h *SQLServerHelper) Begin() error {

	var (
		err error
	)

	if h.db == nil {
		return dhl.ErrNoConn
	}

	if h.tx == nil {
		h.tx, err = h.db.Begin()
		if err != nil {
			return err
		}
	}

	h.trcnt++

	return nil
}

// Commit a transaction
func (h *SQLServerHelper) Commit() error {

	// exit if the connection was just reused
	if h.trcnt > 1 {
		h.trcnt-- // deduct from transaction count
		return nil
	}

	if h.db == nil {
		return dhl.ErrNoConn
	}

	if h.tx == nil {
		return dhl.ErrNoTx
	}

	if h.trcnt == 1 {
		if err := h.tx.Commit(); err != nil {
			return err
		}
	}

	// decrement transaction
	if h.trcnt > 0 {
		h.trcnt--
	}

	// if trancount is zero, we can set the tx to nil
	if h.trcnt == 0 {
		h.tx = nil
	}

	return nil
}

// Rollback a transaction
func (h *SQLServerHelper) Rollback() error {

	// exit if the connection was just reused
	if h.trcnt > 1 {
		h.trcnt-- // deduct from transaction count
		return nil
	}

	if h.db == nil {
		return dhl.ErrNoConn
	}

	if h.tx == nil {
		return dhl.ErrNoTx
	}

	if h.trcnt == 1 {
		if err := h.tx.Rollback(); err != nil {
			return err
		}
	}

	// decrement transaction
	if h.trcnt > 0 {
		h.trcnt--
	}

	// if trancount is zero, we can set the tx to nil
	if h.trcnt == 0 {
		h.tx = nil
	}

	return nil
}

// Mark a savepoint
func (h *SQLServerHelper) Mark(name string) error {

	var err error

	if h.db == nil {
		return dhl.ErrNoConn
	}

	if h.tx == nil {
		return dhl.ErrNoTx
	}

	if h.trcnt > 0 {
		_, err = h.tx.ExecContext(h.ctx, `SAVE TRAN sp_`+name+`;`)
	}

	return err
}

// Discard a savepoint
func (h *SQLServerHelper) Discard(name string) error {
	var err error

	if h.db == nil {
		return dhl.ErrNoConn
	}

	if h.tx == nil {
		return dhl.ErrNoTx
	}

	if h.trcnt > 0 {
		_, err = h.tx.ExecContext(h.ctx, `ROLLBACK TRAN sp_`+name+`;`)
	}

	return err
}

// Save a savepoint
func (h *SQLServerHelper) Save(name string) error {
	var err error

	if h.db == nil {
		return dhl.ErrNoConn
	}

	if h.tx == nil {
		return dhl.ErrNoTx
	}

	if h.trcnt > 0 {
		_, err = h.tx.ExecContext(h.ctx, `COMMIT TRAN sp_`+name+`;`)
	}

	return err
}

// Query from PostgreSQL helper
func (h *SQLServerHelper) Query(sql string, args ...interface{}) (dhl.Rows, error) {

	var (
		err error
		sqr *dsql.Rows
	)

	if h.db == nil {
		return nil, dhl.ErrNoConn
	}

	// replace question mark (?) parameter with configured query parameter, if there are any
	sql = dhl.ReplaceQueryParamMarker(sql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)

	// replace tables meant for interpolation {table} for putting the schema
	sql = dhl.InterpolateTable(sql, h.dbi.Schema)

	if h.tx != nil {
		sqr, err = h.tx.QueryContext(h.ctx, sql, args...)
	} else {
		sqr, err = h.db.QueryContext(h.ctx, sql, args...)
	}

	if err != nil {
		return nil, err
	}

	if sqr == nil {
		return nil, dhl.ErrNoConn
	}

	h.rws = NewSQLServerRows(sqr)

	return h.rws, err
}

// QueryArray puts the single column result to an output array
func (h *SQLServerHelper) QueryArray(sql string, out interface{}, args ...interface{}) error {

	var (
		err error
		sqr *dsql.Rows
	)

	switch out.(type) {
	case *[]string, *[]int, *[]int8, *[]int16, *[]int32, *[]int64, *[]bool, *[]float32, *[]float64:
	case *[]time.Time:
	default:
		return dhl.ErrArrayTypeNotSupported
	}

	if h.db == nil {
		return dhl.ErrNoConn
	}

	// replace question mark (?) parameter with configured query parameter, if there are any
	sql = dhl.ReplaceQueryParamMarker(sql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)

	// replace tables meant for interpolation {table} for putting the schema
	sql = dhl.InterpolateTable(sql, h.dbi.Schema)

	if h.tx != nil {
		sqr, err = h.tx.QueryContext(h.ctx, sql, args...)
	} else {
		sqr, err = h.db.QueryContext(h.ctx, sql, args...)
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

		arr := make([]string, 0)
		var a string

		for sqr.Next() {
			if err = sqr.Scan(&a); err != nil {
				return err
			}

			arr = append(arr, a)
		}

		if err = sqr.Err(); err != nil {
			return err
		}

		*t = arr

		_ = t
	case *[]int:
		arr := make([]int, 0)
		var a int

		for sqr.Next() {
			if err = sqr.Scan(&a); err != nil {
				return err
			}

			arr = append(arr, a)
		}

		if err = sqr.Err(); err != nil {
			return err
		}

		*t = arr
		_ = t
	case *[]int8:
		arr := make([]int8, 0)
		var a int8

		for sqr.Next() {
			if err = sqr.Scan(&a); err != nil {
				return err
			}

			arr = append(arr, a)
		}

		if err = sqr.Err(); err != nil {
			return err
		}

		*t = arr
		_ = t
	case *[]int16:
		arr := make([]int16, 0)
		var a int16

		for sqr.Next() {
			if err = sqr.Scan(&a); err != nil {
				return err
			}

			arr = append(arr, a)
		}

		if err = sqr.Err(); err != nil {
			return err
		}

		*t = arr
		_ = t
	case *[]int32:
		arr := make([]int32, 0)
		var a int32

		for sqr.Next() {
			if err = sqr.Scan(&a); err != nil {
				return err
			}

			arr = append(arr, a)
		}

		if err = sqr.Err(); err != nil {
			return err
		}

		*t = arr
		_ = t
	case *[]int64:
		arr := make([]int64, 0)
		var a int64

		for sqr.Next() {
			if err = sqr.Scan(&a); err != nil {
				return err
			}

			arr = append(arr, a)
		}

		if err = sqr.Err(); err != nil {
			return err
		}

		*t = arr
		_ = t
	case *[]bool:
		arr := make([]bool, 0)
		var a bool

		for sqr.Next() {
			if err = sqr.Scan(&a); err != nil {
				return err
			}

			arr = append(arr, a)
		}

		if err = sqr.Err(); err != nil {
			return err
		}

		*t = arr
		_ = t
	case *[]float32:
		arr := make([]float32, 0)
		var a float32

		for sqr.Next() {
			if err = sqr.Scan(&a); err != nil {
				return err
			}

			arr = append(arr, a)
		}

		if err = sqr.Err(); err != nil {
			return err
		}

		*t = arr
		_ = t
	case *[]float64:
		arr := make([]float64, 0)
		var a float64

		for sqr.Next() {
			if err = sqr.Scan(&a); err != nil {
				return err
			}

			arr = append(arr, a)
		}

		if err = sqr.Err(); err != nil {
			return err
		}

		*t = arr
		_ = t
	case *[]time.Time:
		arr := make([]time.Time, 0)
		var a time.Time

		for sqr.Next() {
			if err = sqr.Scan(&a); err != nil {
				return err
			}

			arr = append(arr, a)
		}

		if err = sqr.Err(); err != nil {
			return err
		}

		*t = arr
		_ = t
	}

	return nil
}

// QueryRow from PostgreSQL helper
func (h *SQLServerHelper) QueryRow(sql string, args ...interface{}) dhl.Row {

	if h.db == nil {
		return nil
	}

	// replace question mark (?) parameter with configured query parameter, if there are any
	sql = dhl.ReplaceQueryParamMarker(sql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
	sql = dhl.InterpolateTable(sql, h.dbi.Schema)

	if h.tx != nil {
		h.rw = NewSQLServerRow(h.tx.QueryRowContext(h.ctx, sql, args...))
		return h.rw
	}

	h.rw = NewSQLServerRow(h.db.QueryRowContext(h.ctx, sql, args...))
	return h.rw
}

// Exec from SQLServerHelper helper
func (h *SQLServerHelper) Exec(sql string, args ...interface{}) (int64, error) {

	var (
		err error
		ra  int64
		sq  dsql.Result
	)

	if h.db == nil {
		return 0, dhl.ErrNoConn
	}

	// replace question mark (?) parameter with configured query parameter, if there are any
	sql = dhl.ReplaceQueryParamMarker(sql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)

	sql = dhl.InterpolateTable(sql, h.dbi.Schema)

	if h.tx != nil {
		sq, err = h.tx.ExecContext(h.ctx, sql, args...)
	} else {
		sq, err = h.db.ExecContext(h.ctx, sql, args...)
	}

	if err != nil {
		return 0, err
	}

	ra, _ = sq.RowsAffected()
	return ra, nil
}

// Exists checks if a record exist
func (h *SQLServerHelper) Exists(sqlwparams string, args ...interface{}) (bool, error) {

	var (
		err error
		cnt int
		sql string
	)

	if h.db == nil {
		return false, nil
	}

	// replace question mark (?) parameter with configured query parameter, if there are any
	sqlwparams = dhl.ReplaceQueryParamMarker(sqlwparams, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
	sqlwparams = dhl.InterpolateTable(sqlwparams, h.dbi.Schema)

	sql = `SELECT TOP 1 1 FROM ` + sqlwparams + `;`

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

	err = h.db.QueryRowContext(h.ctx, sql, args...).Scan(&cnt)
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
func (h *SQLServerHelper) VerifyWithin(tablename string, values []std.VerifyExpression) (Valid bool, QueryOK bool, Message string) {

	if h.db == nil {
		return false, false, dhl.ErrNoConn.Error()
	}

	tableNameWithParameters := tablename

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

	sql = `SELECT CAST(CASE WHEN (SELECT TOP(1) 1 FROM ` + tableNameWithParameters + `) = 1 THEN 1 ELSE 0 END AS BIT);`
	err = h.QueryRow(sql, args...).Scan(&exists)
	if err != nil {
		if !errors.Is(err, dhl.ErrNoRows) {
			return false, false, err.Error()
		}
		return false, true, ""
	}

	return exists, true, ""
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
