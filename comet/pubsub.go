
package main

import (
	log "code.google.com/p/log4go"
	"errors"
	"time"
)

const (
	TCPProto               = uint8(0)
	WebsocketProto         = uint8(1)
	WebsocketProtoStr      = "websocket"
	TCPProtoStr            = "tcp"
	Heartbeat              = "h"
	minHearbeatSec         = 30
	delayHeartbeatSec      = 5
	fitstPacketTimedoutSec = time.Second * 5
	Second                 = int64(time.Second)
)

var (
	// Exceed the max subscriber per key
	ErrMaxConn = errors.New("Exceed the max subscriber connection per key")
	// Assection type failed
	ErrAssertType = errors.New("Subscriber assert type failed")
	// Heartbeat
	// HeartbeatLen = len(Heartbeat)
	// hearbeat
	HeartbeatReply = []byte("+h\r\n")
	// auth failed reply
	AuthReply = []byte("-a\r\n")
	// channle not found reply
	ChannelReply = []byte("-c\r\n")
	// param error reply
	ParamReply = []byte("-p\r\n")
	// node error reply
	NodeReply = []byte("-n\r\n")
)

// StartListen start accept client.
func StartComet() error {
	for _, proto := range Conf.Proto {
		if proto == WebsocketProtoStr {
			// Start http push service
			if err := StartWebsocket(); err != nil {
				return err
			}
		} else if proto == TCPProtoStr {
			// Start tcp push service
			if err := StartTCP(); err != nil {
				return err
			}
		} else {
			log.Warn("unknown gopush-cluster protocol %s, (\"websocket\" or \"tcp\")", proto)
		}
	}
	return nil
}
