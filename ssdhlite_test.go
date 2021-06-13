package ssdhlite

import (
	"context"
	"fmt"
	"testing"
	"time"

	dhl "github.com/NarsilWorks-Inc/datahelperlite"
	//dhl "eaglebush/datahelperlite"

	cfg "github.com/eaglebush/config"

	_ "github.com/denisenkom/go-mssqldb"
)

func TestGetRows(t *testing.T) {

	var (
		err error
		c   dhl.DataHelperLite
	)

	//c = &SQLServerHelper{}

	c, err = dhl.New(nil, `ssdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.LoadConfig(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	rows, err := c.Query(`SELECT EmailKey, Subject, Format, SenderName, SenderAddress, DateQueued FROM tnfEmailSent;`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer rows.Close()

	var (
		emailkey                            int64
		subject, format, sender, senderaddr string
		datequeued                          time.Time
	)

	for rows.Next() {
		err = rows.Scan(
			&emailkey,
			&subject,
			&format,
			&sender,
			&senderaddr,
			&datequeued)

		if err != nil {
			t.Log(err.Error())
			t.Fail()
			return
		}

		// t.Logf("EmailKey: %d, Subject: %s, Format: %s, Sender: %s, SenderAddress: %s, Date Queued: %s",
		// 	emailkey, subject, format, sender, senderaddr, datequeued.Format(`2006-01-02T15:04:05.000Z`))

		t.Logf("EmailKey: %d, Subject: %s, Format: %s, Sender: %s, SenderAddress: %s, Date Queued: %s",
			emailkey, subject, format, sender, senderaddr, datequeued)
	}

	if rows.Err() != nil {
		t.Log(err.Error())
		return
	}
}

func TestGetRow(t *testing.T) {
	var (
		err error
		c   dhl.DataHelperLite
	)

	//c = &SQLServerHelper{}

	c, err = dhl.New(nil, `ssdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.LoadConfig(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	/*
		var (
			emailkey                            int64
			subject, format, sender, senderaddr string
			//datequeued                          sql.NullTime
			datequeued time.Time
		)

		err = c.QueryRow(`SELECT EmailKey, Subject, Format,
							SenderName, SenderAddress, DateQueued
							FROM tnfEmailSent;`).Scan(
			&emailkey,
			&subject,
			&format,
			&sender,
			&senderaddr,
			&datequeued)

	*/

	/*
		type teststruct struct {
			EmailKey   *int
			Subject    *string
			Format     *string
			Sender     *string
			SenderAddr *string
			DateQueued *time.Time
		}

		ts := teststruct{}

		err = c.QueryRow(`SELECT EmailKey, Subject, Format,
							SenderName, SenderAddress, DateQueued
							FROM tnfEmailSent;`).Scan(
			&ts.EmailKey,
			&ts.Subject,
			&ts.Format,
			&ts.Sender,
			&ts.SenderAddr,
			&ts.DateQueued)
	*/

	type teststruct struct {
		EmailKey   int
		Subject    string
		Format     string
		Sender     string
		SenderAddr string
		DateQueued time.Time
	}

	ts := teststruct{}

	err = c.QueryRow(`SELECT EmailKey, Subject, Format,
							SenderName, SenderAddress, DateQueued
							FROM tnfEmailSent WHERE 1=2;`).Scan(
		&ts.EmailKey,
		&ts.Subject,
		&ts.Format,
		&ts.Sender,
		&ts.SenderAddr,
		&ts.DateQueued)

	if err != nil {

		if err != dhl.ErrNoRows {
			t.Log(err.Error())
			t.Fail()
			return
		}

		t.Log(err.Error())
	}

	// t.Logf("EmailKey: %v, Subject: %v, Format: %v, Sender: %v, SenderAddress: %v, Date Queued: %v",
	// 	emailkey, subject, format, sender, senderaddr, datequeued)

	// t.Logf("EmailKey: %v, Subject: %v, Format: %v, Sender: %v, SenderAddress: %v, Date Queued: %v",
	// 	*ts.EmailKey, *ts.Subject, *ts.Format, *ts.Sender, *ts.SenderAddr, *ts.DateQueued)

	t.Logf("EmailKey: %v, Subject: %v, Format: %v, Sender: %v, SenderAddress: %v, Date Queued: %v",
		ts.EmailKey, ts.Subject, ts.Format, ts.Sender, ts.SenderAddr, ts.DateQueued)
}

func TestWriteTransactions(t *testing.T) {

	var (
		err error
		//affr int64
		c dhl.DataHelperLite
	)

	//c = &SQLServerHelper{}

	c, err = dhl.New(nil, `ssdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.LoadConfig(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	i := 0

	c.Begin()

	for {

		if i > 999 {
			break
		}

		_, err = c.Exec(`INSERT INTO tnfTelegramOutBox (
									ApplicationID,
									TelegramID,
									Message,
									[Status],
									TransactionDate,
									GuiID,
									Principal,
									PrinGroup,
									CustPONo,
									PoStatus)
							VALUES ('TestApp',
									'3dadasdas',
									 'Message' + @p1,
									1,
									GETDATE(),
									NEWID(),
									@p2,
									'TESTGRP',
									'PONO',
									'OK');`, fmt.Sprintf("%d", i), i)
		if err != nil {
			c.Rollback()
			t.Log(err.Error())
			break
		}

		/*
			if (i % 5) == 0 {
				c.Mark(`MO`)
			}

			if (i % 10) == 0 {
				//c.Save(`MO`)
				c.Discard(`MO`)
			}
		*/

		//t.Logf("%d affected rows", affr)

		i++
	}

	// c.Rollback()
	c.Commit()

}

func TestSequence(t *testing.T) {
	var (
		err error
		//affr int64
		c dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `ssdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.LoadConfig(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	// pointer, must be initialized to int64

	seq := new(int64)

	err = c.Next(`testsequence`, seq)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	/*
		// non pointer
		var seq int64

		err = c.Next(`testsequence`, &seq)
		if err != nil {
			t.Log(err.Error())
			t.Fail()
			return
		}
	*/

	t.Logf("Sequence for testsequence: %d", *seq)
}

func TestMultipleOpen(t *testing.T) {

	var repeat = func() {
		var (
			err error
			c   dhl.DataHelperLite
		)

		c, err = dhl.New(nil, `ssdhlite`)
		if err != nil {
			t.Log(err.Error())
			t.Fail()
			return
		}

		cf, err := cfg.LoadConfig(`config.json`)
		if err != nil {
			t.Log(err.Error())
			t.Fail()
			return
		}

		if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
			t.Log(err.Error())
			t.Fail()
			return
		}
		defer c.Close()

		rows, err := c.Query(`SELECT TOP 1 EmailKey, Subject, Format, SenderName, SenderAddress, DateQueued FROM tnfEmailSent;`)
		if err != nil {
			t.Log(err.Error())
			t.Fail()
			return
		}
		defer rows.Close()

		var (
			emailkey                            int64
			subject, format, sender, senderaddr string
			datequeued                          time.Time
		)

		for rows.Next() {
			err = rows.Scan(
				&emailkey,
				&subject,
				&format,
				&sender,
				&senderaddr,
				&datequeued)

			if err != nil {
				t.Log(err.Error())
				t.Fail()
				return
			}

			// t.Logf("EmailKey: %d, Subject: %s, Format: %s, Sender: %s, SenderAddress: %s, Date Queued: %s",
			// 	emailkey, subject, format, sender, senderaddr, datequeued.Format(`2006-01-02T15:04:05.000Z`))

			t.Logf("EmailKey: %d, Subject: %s, Format: %s, Sender: %s, SenderAddress: %s, Date Queued: %s",
				emailkey, subject, format, sender, senderaddr, datequeued)
		}

		if rows.Err() != nil {
			t.Log(err.Error())
			return
		}

	}

	for i := 0; i < 3; i++ {
		repeat()
	}
}

func TestExists(t *testing.T) {
	var (
		err    error
		exists bool
		c      dhl.DataHelperLite
	)

	//c = &SQLServerHelper{}

	c, err = dhl.New(nil, `ssdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.LoadConfig(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	exists, err = c.Exists(`tnfEmailSent WHERE EmailKey = @p1;`, 7)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	t.Logf("Exists: %t", exists)

}

func TestQueryArray(t *testing.T) {
	var (
		err    error
		exists bool
		c      dhl.DataHelperLite
	)

	//c = &SQLServerHelper{}

	c, err = dhl.New(nil, `ssdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.LoadConfig(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	//var arr []int
	var arr []string

	//err = c.QueryArray(`SELECT EmailKey FROM tnfEmailSent;`, &arr)
	err = c.QueryArray(`SELECT EmailKey FROM tnfEmailSent;`, &arr)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	t.Logf("Exists: %t", exists)

}
