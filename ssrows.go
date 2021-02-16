package sshlite

import "database/sql"

// SQLServerRows struct
type SQLServerRows struct {
	sqr *sql.Rows
}

// NewSQLServerRows generates a datahelper compatible SQLServerRows
func NewSQLServerRows(sqlr *sql.Rows) SQLServerRows {
	return SQLServerRows{
		sqr: sqlr,
	}
}

// Close rows
func (ss SQLServerRows) Close() {
	if ss.sqr != nil {
		ss.sqr.Close()
	}

	return
}

// Err check
func (ss SQLServerRows) Err() error {
	return ss.sqr.Err()
}

// Next row in the sequence
func (ss SQLServerRows) Next() bool {
	return ss.sqr.Next()
}

// Scan to destination variables
func (ss SQLServerRows) Scan(dest ...interface{}) error {
	return ss.Scan(dest...)
}

// Values from the rows
func (ss SQLServerRows) Values() ([]interface{}, error) {
	return nil, nil
}

// RawValues from the rows
func (ss SQLServerRows) RawValues() [][]byte {
	return nil
}
