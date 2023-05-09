package uuidutil

import (
	"github.com/google/uuid"
	"time"
)

const (
	lillian    = 2299160          // Julian day of 15 Oct 1582
	unix       = 2440587          // Julian day of 1 Jan 1970
	epoch      = unix - lillian   // Days between epochs
	g1582      = epoch * 86400    // seconds between epochs
	g1582ns100 = g1582 * 10000000 // 100s of a nanoseconds between epochs
)

func FromTime(t time.Time) uuid.Time {
	return uuid.Time(t.UnixNano()/100 + g1582ns100)
}

func ToTime(t uuid.Time) time.Time {
	return time.Unix(t.UnixTime())
}

func Now() uuid.Time {
	return FromTime(time.Now())
}
