
package main

import (
	log "code.google.com/p/log4go"
	"errors"
	"github.com/lucas-chi/push-service/hlist"
	"github.com/lucas-chi/push-service/id"
	myrpc "github.com/lucas-chi/push-service/rpc"
	"sync"
)

var (
	ErrMessageSave   = errors.New("Message set failed")
	ErrMessageGet    = errors.New("Message get failed")
	ErrMessageRPC    = errors.New("Message RPC not init")
	ErrAssectionConn = errors.New("Assection type Connection failed")
)

// Sequence Channel struct.
type SeqChannel struct {
	// Mutex
	mutex *sync.Mutex
	// client conn double linked-list
	conn *hlist.Hlist
	// Remove time id or lazy New
	// timeID *id.TimeID
}

// New a user seq stored message channel.
func NewSeqChannel() *SeqChannel {
	ch := &SeqChannel{
		mutex: &sync.Mutex{},
		conn:  hlist.New(),
		//timeID: id.NewTimeID()
	}

	return ch
}

// WriteMsg implements the Channel WriteMsg method.
func (c *SeqChannel) WriteMsg(key string, m *myrpc.Message) (err error) {
	c.mutex.Lock()
	err = c.writeMsg(key, m)
	c.mutex.Unlock()
	return
}

// writeMsg write msg to conn.
func (c *SeqChannel) writeMsg(key string, m *myrpc.Message) (err error) {
	var (
		oldMsg, msg, sendMsg []byte
	)
	// push message
	for e := c.conn.Front(); e != nil; e = e.Next() {
		conn, _ := e.Value.(*Connection)
		// if version empty then use old protocol
		if conn.Version == "" {
			if oldMsg == nil {
				if oldMsg, err = m.OldBytes(); err != nil {
					return
				}
			}
			sendMsg = oldMsg
		} else {
			if msg == nil {
				if msg, err = m.Bytes(); err != nil {
					return
				}
			}
			sendMsg = msg
		}
		// TODO use goroutine
		conn.Write(key, sendMsg)
	}
	return
}

// PushMsg implements the Channel PushMsg method.
func (c *SeqChannel) PushMsg(key string, m *myrpc.Message, expire uint) (err error) {
	client := myrpc.MessageRPC.Get()
	if client == nil {
		return ErrMessageRPC
	}
	c.mutex.Lock()
	// private message need persistence
	// if message expired no need persistence, only send online message
	// rewrite message id
	//m.MsgId = c.timeID.ID()
	m.MsgId = id.Get()
	if m.GroupId != myrpc.PublicGroupId && expire > 0 {
		args := &myrpc.MessageSavePrivateArgs{Key: key, Msg: m.Msg, MsgId: m.MsgId, Expire: expire}
		ret := 0
		if err = client.Call(myrpc.MessageServiceSavePrivate, args, &ret); err != nil {
			c.mutex.Unlock()
			log.Error("%s(\"%s\", \"%v\", &ret) error(%v)", myrpc.MessageServiceSavePrivate, key, args, err)
			return
		}
	}
	// push message
	if err = c.writeMsg(key, m); err != nil {
		c.mutex.Unlock()
		log.Error("c.WriteMsg(\"%s\", m) error(%v)", key, err)
		return
	}
	c.mutex.Unlock()
	return
}

// AddConn implements the Channel AddConn method.
func (c *SeqChannel) AddConn(key string, conn *Connection) (*hlist.Element, error) {
	c.mutex.Lock()
	if c.conn.Len()+1 > Conf.MaxSubscriberPerChannel {
		c.mutex.Unlock()
		log.Error("user_key:\"%s\" exceed conn", key)
		return nil, ErrMaxConn
	}
	// send first heartbeat to tell client service is ready for accept heartbeat
	if _, err := conn.Conn.Write(HeartbeatReply); err != nil {
		c.mutex.Unlock()
		log.Error("user_key:\"%s\" write first heartbeat to client error(%v)", key, err)
		return nil, err
	}
	// add conn
	conn.Buf = make(chan []byte, Conf.MsgBufNum)
	conn.HandleWrite(key)
	e := c.conn.PushFront(conn)
	c.mutex.Unlock()
	//ConnStat.IncrAdd()
	log.Info("user_key:\"%s\" add conn = %d", key, c.conn.Len())
	return e, nil
}

// RemoveConn implements the Channel RemoveConn method.
func (c *SeqChannel) RemoveConn(key string, e *hlist.Element) error {
	c.mutex.Lock()
	tmp := c.conn.Remove(e)
	c.mutex.Unlock()
	conn, ok := tmp.(*Connection)
	if !ok {
		return ErrAssectionConn
	}
	close(conn.Buf)
	//ConnStat.IncrRemove()
	log.Info("user_key:\"%s\" remove conn = %d", key, c.conn.Len())
	return nil
}

// Close implements the Channel Close method.
func (c *SeqChannel) Close() error {
	c.mutex.Lock()
	for e := c.conn.Front(); e != nil; e = e.Next() {
		if conn, ok := e.Value.(*Connection); !ok {
			c.mutex.Unlock()
			return ErrAssectionConn
		} else {
			if err := conn.Conn.Close(); err != nil {
				// ignore close error
				log.Warn("conn.Close() error(%v)", err)
			}
		}
	}
	c.mutex.Unlock()
	return nil
}
