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
func (h *Handle) Open(di *dn.DataInfo) error {
	if di == nil {
		return fmt.Errorf("open: no data info set")
	}
	if di.ConnectionString == nil {
		return fmt.Errorf("open: no data connection string set")
	}
	// Added to handle sql.Open panic
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Recovered from DB panic: %v", r)
		}
	}()
	db, err := sql.Open("sqlserver", *di.ConnectionString)
	if err != nil {
		h.err = fmt.Errorf("open: %w", err)
		return h.err
	}
	h.db = db
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
	// Use a timeout for ping
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.db.PingContext(ctx); err != nil {
		h.err = fmt.Errorf("open: ping failed: %w", err)
		return h.err
	}
	h.err = nil
	return nil
}

// Ping tests the database connection
func (h *Handle) Ping() error {
	if h.db == nil {
		return fmt.Errorf("ping: %s to use", dhl.ErrHandleNoHandle)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.db.PingContext(ctx); err != nil {
		h.err = fmt.Errorf("ping: %w", err)
		return h.err
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
func (h *Handle) Close() error {
	if h.db == nil {
		return fmt.Errorf("close: %s to close", dhl.ErrHandleNoHandle)
	}
	if h.err = h.db.Close(); h.err != nil {
		return h.err
	}
	h.db = nil
	h.err = nil
	return nil
}

// Err returns the last error
func (h *Handle) Err() error {
	return h.err
}
