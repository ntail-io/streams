package backoff

import "time"

// Exponential backoff
type Backoff struct {
	Until      time.Time
	MaxRetries int
	Interval   time.Duration
	retries    int
}

func (b *Backoff) Try() bool {
	if b.retries == 0 {
		b.retries++
		return true
	}

	if b.retries == b.MaxRetries {
		return false
	}

	if b.Until.Before(time.Now()) {
		return false
	}

	time.Sleep(b.Interval * (1 << time.Duration(b.retries)))

	b.retries++
	return true
}
