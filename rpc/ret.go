
package rpc

import (
	"errors"
)

const (
	// common
	// ok
	OK = 0
	// param error
	ParamErr = 65534
	// internal error
	InternalErr = 65535
)

var (
	ErrParam = errors.New("parameter error")
)
