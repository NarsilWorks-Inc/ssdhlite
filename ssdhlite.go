package sshlite

import (
	"context"
	dsql "database/sql"
	"errors"
	"strconv"
	"strings"

	dhl "github.com/NarsilWorks-Inc/datahelperlite"

	cfg "github.com/eaglebush/config"
	std "github.com/eaglebush/stdutil"
)

// SQLServerHelper - a struct derived from datahelperlite
type SQLServerHelper struct {
	db     *dsql.DB
	tx     *dsql.Tx
	dbi    *cfg.DatabaseInfo
	ctx    context.Context
	rws    dhl.Rows
	trcnt  int
	reused bool
}

func init() {
	dhl.SetHelper(`ssdhlite`, &SQLServerHelper{})
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
	h.reused = true

	if h.db == nil {
		h.db, err = dsql.Open(`sqlserver`, di.ConnectionString)
		if err != nil {
			return err
		}
		h.reused = false
	}

	return nil
}

// Close the helper
func (h *SQLServerHelper) Close() error {

	if h.db == nil {
		return errors.New(`No connection of the object was initialized`)
	}

	if h.reused {
		return nil
	}

	if err := h.db.Close(); err != nil {
		return err
	}

	h.trcnt = 0
	h.reused = false

	return nil
}

// Begin a transaction. If there is an existing transaction, begin is ignored
func (h *SQLServerHelper) Begin() error {

	var (
		err error
	)

	if h.db == nil {
		return errors.New(`No connection of the object was initialized`)
	}

	if h.tx == nil {
		h.tx, err = h.db.Begin()
		if err != nil {
			return err
		}
		h.trcnt++
	}

	return nil
}

// Commit a transaction
func (h *SQLServerHelper) Commit() error {

	// exit if the connection was just reused
	if h.reused {
		return nil
	}

	if h.tx == nil {
		return errors.New(`No transaction was initialized`)
	}

	if err := h.tx.Commit(); err != nil {
		return err
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
	if h.reused {
		return nil
	}

	if h.tx == nil {
		return errors.New(`No transaction was initialized`)
	}

	if err := h.tx.Rollback(); err != nil {
		return err
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

	if h.tx == nil {
		return errors.New(`No transaction was initialized`)
	}

	_, err = h.tx.ExecContext(h.ctx, `SAVE TRAN sp_`+name+`;`)

	return err
}

// Discard a savepoint
func (h *SQLServerHelper) Discard(name string) error {
	var err error

	if h.tx == nil {
		return errors.New(`No transaction was initialized`)
	}

	_, err = h.tx.ExecContext(h.ctx, `ROLLBACK TRAN sp_`+name+`;`)

	return err
}

// Save a savepoint
func (h *SQLServerHelper) Save(name string) error {
	var err error

	if h.tx == nil {
		return errors.New(`No transaction was initialized`)
	}

	_, err = h.tx.ExecContext(h.ctx, `COMMIT TRAN sp_`+name+`;`)

	return err
}

// Query from PostgreSQL helper
func (h *SQLServerHelper) Query(sql string, args ...interface{}) (dhl.Rows, error) {

	var (
		err error
		sqr *dsql.Rows
	)

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

	if sqr != nil {
		h.rws = NewSQLServerRows(sqr)
	}

	return h.rws, err
}

// QueryRow from PostgreSQL helper
func (h *SQLServerHelper) QueryRow(sql string, args ...interface{}) dhl.Row {

	// replace question mark (?) parameter with configured query parameter, if there are any
	sql = dhl.ReplaceQueryParamMarker(sql, h.dbi.ParameterInSequence, h.dbi.ParameterPlaceholder)
	sql = dhl.InterpolateTable(sql, h.dbi.Schema)

	if h.tx != nil {
		return h.tx.QueryRowContext(h.ctx, sql, args...)
	}

	return h.db.QueryRowContext(h.ctx, sql, args...)
}

// Exec from PostgreSQL helper
func (h *SQLServerHelper) Exec(sql string, args ...interface{}) (int64, error) {

	var (
		err error
		ra  int64
		sq  dsql.Result
	)

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

// VerifyWithin a set of validation expression against the underlying database table
func (h *SQLServerHelper) VerifyWithin(tablename string, values []std.VerifyExpression) (Valid bool, QueryOK bool, Message string) {
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

	sql = dhl.InterpolateTable(`SELECT EXISTS (SELECT 1 FROM `+tableNameWithParameters+`);`, h.dbi.Schema)

	err = h.QueryRow(sql, args...).Scan(&exists)
	if err != nil {
		return false, false, err.Error()
	}

	return exists, true, ""
}

// Escape a field value (fv) from disruption by single quote
func (h *SQLServerHelper) Escape(fv string) string {

	if len(fv) == 0 {
		return ""
	}

	senc := h.dbi.StringEnclosingChar
	sesc := h.dbi.StringEscapeChar

	if len(senc) == 0 {
		senc = `'`
	}

	if len(sesc) == 0 {
		sesc = `'`
	}

	return strings.ReplaceAll(fv, senc, sesc+sesc)
}
