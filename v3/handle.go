package ssdhlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	dhl "github.com/NarsilWorks-Inc/datahelperlite/v3"
	dn "github.com/eaglebush/datainfo"
)

// Handle manages the handle to the database connection
//
// It manages the resident database connection for proper pooling.
// This struct implements DataHelperHandler interface.
type Handle struct {
	db  *sql.DB
	dbi *dn.DataInfo
	err error
}

func init() {
	dhl.SetHandler(`ssdhlite`, &Handle{})
}

// Open connects to the database and initializes it
func (h *Handle) Open(di *dn.DataInfo) (err error) {
	if di == nil {
		err = fmt.Errorf("open: no data info set")
		h.err = err
		return
	}
	if di.ConnectionString == nil {
		err = fmt.Errorf("open: no data connection string set")
		h.err = err
		return
	}
	handlePanic(&err)
	db, err := sql.Open("sqlserver", *di.ConnectionString)
	if err != nil {
		err = fmt.Errorf("open: %w", err)
		h.err = err
		return
	}
	h.db = db
	h.dbi = di

	h.db.SetMaxOpenConns(20)
	if di.MaxOpenConnection != nil {
		h.db.SetMaxOpenConns(*di.MaxOpenConnection)
	}
	h.db.SetMaxIdleConns(2)
	if di.MaxIdleConnection != nil {
		h.db.SetMaxIdleConns(*di.MaxIdleConnection)
	}
	h.db.SetConnMaxLifetime(30 * time.Minute)
	if di.MaxConnectionLifetime != nil {
		h.db.SetConnMaxLifetime(time.Duration(*di.MaxConnectionLifetime))
	}
	h.db.SetConnMaxIdleTime(2 * time.Minute)
	if di.MaxConnectionIdleTime != nil {
		h.db.SetConnMaxIdleTime(time.Duration(*di.MaxConnectionIdleTime))
	}
	// Use a timeout for ping
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err = h.db.PingContext(ctx); err != nil {
		// A failed ping should nullify the db because this is the Open() method
		h.db = nil
		err = fmt.Errorf("open: ping failed: %w", err)
		h.err = err
		return
	}
	h.err = nil
	return nil
}

// Ping tests the database connection
func (h *Handle) Ping() (err error) {
	if h.db == nil {
		err = fmt.Errorf("ping: %s to use", dhl.ErrHandleNoHandle)
		h.err = err
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	handlePanic(&err)
	if err = h.db.PingContext(ctx); err != nil {
		err = fmt.Errorf("ping: %w", err)
		h.err = err
		return
	}
	h.err = nil
	return nil
}

// DB returns the database handle
func (h *Handle) DB() *sql.DB {
	return h.db
}

// DI returns the data info that configured the handle
func (h *Handle) DI() *dn.DataInfo {
	return h.dbi
}

// Close the database connection
func (h *Handle) Close() (err error) {
	if h.db == nil {
		err = fmt.Errorf("close: %s to close", dhl.ErrHandleNoHandle)
		h.err = err
		return
	}
	handlePanic(&err)
	if err = h.db.Close(); err != nil {
		h.err = err
		return
	}
	h.db = nil
	h.err = nil
	return nil
}

// Err returns the last error
func (h *Handle) Err() error {
	return h.err
}
