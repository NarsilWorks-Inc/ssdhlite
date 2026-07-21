package ssdhlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	dhl "github.com/NarsilWorks-Inc/datahelperlite/v3"
	dn "github.com/eaglebush/datainfo"
)

// Handle manages the resident database connection pool.
type Handle struct {
	db  *sql.DB
	dbi *dn.DataInfo
	err error

	maintenanceMu sync.Mutex
	stateMu       sync.RWMutex
}

func init() {
	dhl.SetHandler("ssdhlite", &Handle{})
}

// Open creates and validates a pool before publishing it to callers.
func (h *Handle) Open(di *dn.DataInfo) (err error) {
	if h == nil {
		return errors.New("open: handle is nil")
	}

	h.maintenanceMu.Lock()
	defer h.maintenanceMu.Unlock()

	var candidate *sql.DB
	published := false
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("open: recovered panic: %v", r)
		}
		if err != nil && candidate != nil && !published {
			if closeErr := closeDatabase(candidate); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("open: cleanup: %w", closeErr))
			}
		}
		h.setErr(err)
	}()

	if di == nil {
		return errors.New("open: no data info set")
	}

	info := dn.Copy(di)
	if info == nil {
		return errors.New("open: could not copy data info")
	}
	if info.ConnectionString == nil {
		return errors.New("open: no data connection string set")
	}

	candidate, err = sql.Open("sqlserver", *info.ConnectionString)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}

	configurePool(candidate, info)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = candidate.PingContext(ctx); err != nil {
		return fmt.Errorf("open: ping failed: %w", err)
	}

	h.stateMu.Lock()
	previous := h.db
	h.db = candidate
	h.dbi = info
	h.err = nil
	published = true
	h.stateMu.Unlock()

	if previous != nil {
		if closeErr := closeDatabase(previous); closeErr != nil {
			return fmt.Errorf("open: close previous pool: %w", closeErr)
		}
	}

	return nil
}

// Ping tests the currently published database pool.
func (h *Handle) Ping() (err error) {
	if h == nil {
		return errors.New("ping: handle is nil")
	}

	h.maintenanceMu.Lock()
	defer h.maintenanceMu.Unlock()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("ping: recovered panic: %v", r)
		}
		h.setErr(err)
	}()

	db := h.DB()
	if db == nil {
		return fmt.Errorf("ping: %w", dhl.ErrHandleNoHandle)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	return nil
}

// DB returns the currently published database pool.
func (h *Handle) DB() *sql.DB {
	if h == nil {
		return nil
	}
	h.stateMu.RLock()
	defer h.stateMu.RUnlock()
	return h.db
}

// DI returns a copy of the data info used by the current pool.
func (h *Handle) DI() *dn.DataInfo {
	if h == nil {
		return nil
	}
	h.stateMu.RLock()
	defer h.stateMu.RUnlock()
	if h.dbi == nil {
		return nil
	}
	return dn.Copy(h.dbi)
}

// Close detaches and closes the currently published database pool.
func (h *Handle) Close() (err error) {
	if h == nil {
		return errors.New("close: handle is nil")
	}

	h.maintenanceMu.Lock()
	defer h.maintenanceMu.Unlock()

	h.stateMu.Lock()
	db := h.db
	if db == nil {
		err = fmt.Errorf("close: %w", dhl.ErrHandleNoHandle)
		h.err = err
		h.stateMu.Unlock()
		return err
	}
	h.db = nil
	h.dbi = nil
	h.err = nil
	h.stateMu.Unlock()

	err = closeDatabase(db)
	if err != nil {
		err = fmt.Errorf("close: %w", err)
	}
	h.setErr(err)
	return err
}

// Err returns the result of the most recently completed maintenance operation.
func (h *Handle) Err() error {
	if h == nil {
		return errors.New("handle is nil")
	}
	h.stateMu.RLock()
	defer h.stateMu.RUnlock()
	return h.err
}

func (h *Handle) setErr(err error) {
	h.stateMu.Lock()
	defer h.stateMu.Unlock()
	h.err = err
}

func configurePool(db *sql.DB, di *dn.DataInfo) {
	db.SetMaxOpenConns(20)
	if di.MaxOpenConnection != nil {
		db.SetMaxOpenConns(*di.MaxOpenConnection)
	}
	db.SetMaxIdleConns(2)
	if di.MaxIdleConnection != nil {
		db.SetMaxIdleConns(*di.MaxIdleConnection)
	}
	db.SetConnMaxLifetime(30 * time.Minute)
	if di.MaxConnectionLifetime != nil {
		db.SetConnMaxLifetime(time.Duration(*di.MaxConnectionLifetime))
	}
	db.SetConnMaxIdleTime(2 * time.Minute)
	if di.MaxConnectionIdleTime != nil {
		db.SetConnMaxIdleTime(time.Duration(*di.MaxConnectionIdleTime))
	}
}

func closeDatabase(db *sql.DB) (err error) {
	if db == nil {
		return nil
	}
	defer handlePanic(&err)
	return db.Close()
}
