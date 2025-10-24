package ssdhlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	dhl "github.com/NarsilWorks-Inc/datahelperlite/v3"
	dn "github.com/eaglebush/datainfo"
)

// DataHelperHandle manages the handle to the database connection
//
// It manages the resident database connection for proper pooling.
// This struct implements DataHelperHandler interface.
type DataHelperHandle struct {
	db  *sql.DB
	dbi *dn.DataInfo
	err error
}

// Open connects to the database and initializes it
func (h *DataHelperHandle) Open(di *dn.DataInfo) error {
	if di == nil {
		return fmt.Errorf("open: no data info set")
	}
	if di.ConnectionString == nil {
		return fmt.Errorf("open: no data connection string set")
	}
	h.db, h.err = sql.Open("sqlserver", *di.ConnectionString)
	if h.err != nil {
		return fmt.Errorf("open: %w", h.err)
	}
	h.dbi = di
	if di.MaxOpenConnection != nil {
		h.db.SetMaxOpenConns(*di.MaxOpenConnection)
	}
	h.db.SetMaxIdleConns(0)
	if di.MaxIdleConnection != nil {
		h.db.SetMaxIdleConns(*di.MaxIdleConnection)
	}
	if di.MaxConnectionLifetime != nil {
		h.db.SetConnMaxLifetime(time.Duration(*di.MaxConnectionLifetime))
	}
	if di.MaxConnectionIdleTime != nil {
		h.db.SetConnMaxIdleTime(time.Duration(*di.MaxConnectionIdleTime))
	}
	if err := h.db.PingContext(context.Background()); err != nil {
		h.err = fmt.Errorf("open: %w", err)
		return h.err
	}
	return nil
}

// Ping tests the database connection
func (h *DataHelperHandle) Ping() error {
	if h.db == nil {
		return fmt.Errorf("ping: %s to use", dhl.ErrHandleNoHandle)
	}
	if err := h.db.PingContext(context.Background()); err != nil {
		h.err = fmt.Errorf("ping: %w", err)
		return h.err
	}
	return nil
}

// DB returns the database handle
func (h *DataHelperHandle) DB() *sql.DB {
	return h.db
}

// DI returns the data info that configured the handle
func (h *DataHelperHandle) DI() *dn.DataInfo {
	return h.dbi
}

// Close the database connection
func (h *DataHelperHandle) Close() error {
	if h.db == nil {
		return fmt.Errorf("ping: %s to close", dhl.ErrHandleNoHandle)
	}
	if h.err = h.db.Close(); h.err != nil {
		return h.err
	}
	h.db = nil
	return nil
}

// Err returns the last error
func (h *DataHelperHandle) Err() error {
	return h.err
}
