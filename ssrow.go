package ssdhlite

import (
	"database/sql"
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
func (ss SQLServerRow) Scan(dest ...interface{}) error {

	destq := prepareDest(dest)

	err := ss.sqr.Scan(destq...)
	if err != nil {
		return err
	}

	// return values
	err = copyScannedToDest(dest, destq)
	if err != nil {
		return err
	}

	return nil

}
