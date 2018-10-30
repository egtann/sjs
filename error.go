package sjs

import (
	"sync"

	"github.com/pkg/errors"
)

// OptErr is a channel that we can modify in a threadsafe way. The mutex is
// required to read or modify the underlying channel, which can be created at
// any time by the caller of the library. The exported "C" fieldname
// representating the channel is inspired by the standard library's time.Ticker
// design.
type OptErr struct {
	C chan error
	sync.RWMutex
}

func (o *OptErr) Send(err error) {
	o.RLock()
	defer o.RUnlock()

	if o.C == nil {
		return
	}
	o.C <- err
}

type Missing error

func IsMissing(err error) bool {
	switch errors.Cause(err).(type) {
	case Missing:
		return true
	}
	return false
}
