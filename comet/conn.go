
package main

import (
	log "code.google.com/p/log4go"
	"fmt"
	"net"
)

// Connection
type Connection struct {
	Conn    net.Conn
	Proto   uint8
	Version string
	Buf     chan []byte
}

// HandleWrite start a goroutine get msg from chan, then send to the conn.
func (c *Connection) HandleWrite(key string) {
	go func() {
		var (
			n   int
			err error
		)
		log.Debug("user_key: \"%s\" HandleWrite goroutine start", key)
		for {
			msg, ok := <-c.Buf
			if !ok {
				log.Debug("user_key: \"%s\" HandleWrite goroutine stop", key)
				return
			}
			if c.Proto == WebsocketProto {
				// raw
				n, err = c.Conn.Write(msg)
			} else if c.Proto == TCPProto {
				// redis protocol
				msg = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(msg), string(msg)))
				n, err = c.Conn.Write(msg)
			} else {
				log.Error("unknown connection protocol: %d", c.Proto)
				panic(ErrConnProto)
			}
			// update stat
			if err != nil {
				log.Error("user_key: \"%s\" conn.Write() error(%v)", key, err)
				//MsgStat.IncrFailed(1)
			} else {
				log.Debug("user_key: \"%s\" write \r\n========%s(%d)========", key, string(msg), n)
				//MsgStat.IncrSucceed(1)
			}
		}
	}()
}

// Write different message to client by different protocol
func (c *Connection) Write(key string, msg []byte) {
	select {
	case c.Buf <- msg:
	default:
		c.Conn.Close()
		log.Warn("user_key: \"%s\" discard message: \"%s\" and close connection", key, string(msg))
	}
}
