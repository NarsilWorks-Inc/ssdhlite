package ssdhlite

import (
	"context"
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
							FROM tnfEmailSent;`).Scan(
		&ts.EmailKey,
		&ts.Subject,
		&ts.Format,
		&ts.Sender,
		&ts.SenderAddr,
		&ts.DateQueued)

	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// t.Logf("EmailKey: %v, Subject: %v, Format: %v, Sender: %v, SenderAddress: %v, Date Queued: %v",
	// 	emailkey, subject, format, sender, senderaddr, datequeued)

	// t.Logf("EmailKey: %v, Subject: %v, Format: %v, Sender: %v, SenderAddress: %v, Date Queued: %v",
	// 	*ts.EmailKey, *ts.Subject, *ts.Format, *ts.Sender, *ts.SenderAddr, *ts.DateQueued)

	t.Logf("EmailKey: %v, Subject: %v, Format: %v, Sender: %v, SenderAddress: %v, Date Queued: %v",
		ts.EmailKey, ts.Subject, ts.Format, ts.Sender, ts.SenderAddr, ts.DateQueued)
}
