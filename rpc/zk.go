
package rpc

import (
	"time"
)

const (
	// node event
	eventNodeAdd    = 1
	eventNodeDel    = 2
	eventNodeUpdate = 3

	// wait node
	waitNodeDelay       = 3
	waitNodeDelaySecond = waitNodeDelay * time.Second
)
