package ssdhlite

import (
	"database/sql"
	"errors"

	dhl "github.com/NarsilWorks-Inc/datahelperlite/v3"
)

// SQLServerRow struct
type SQLServerRow struct {
	sqr *sql.Row
}

// NewSQLServerRow generates a datahelper compatible SQLServerRows
func NewSQLServerRow(sqlr *sql.Row) SQLServerRow {
	return SQLServerRow{
		sqr: sqlr,
	}
}

// Scan to destination variables
func (ss SQLServerRow) Scan(dest ...any) (err error) {
	destq := prepareDest(dest)
	handlePanic(&err)
	if err = ss.sqr.Scan(destq...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return dhl.ErrNoRows
		}
		return err
	}
	if err = copyScannedToDest(dest, destq); err != nil {
		return err
	}
	return nil

}
