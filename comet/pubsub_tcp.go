
package main

import (
	"bufio"
	log "code.google.com/p/log4go"
	"errors"
	"io"
	"net"
	"strconv"
	"time"
)

const (
	minCmdNum = 1
	maxCmdNum = 5
)

var (
	// cmd parse failed
	ErrProtocol = errors.New("cmd format error")
)

// tcpBuf cache.
type tcpBufCache struct {
	instance []chan *bufio.Reader
	round    int
}

// newTCPBufCache return a new tcpBuf cache.
func newtcpBufCache() *tcpBufCache {
	inst := make([]chan *bufio.Reader, 0, Conf.BufioInstance)
	log.Debug("create %d read buffer instance", Conf.BufioInstance)
	for i := 0; i < Conf.BufioInstance; i++ {
		inst = append(inst, make(chan *bufio.Reader, Conf.BufioNum))
	}
	return &tcpBufCache{instance: inst, round: 0}
}

// Get return a chan bufio.Reader (round-robin).
func (b *tcpBufCache) Get() chan *bufio.Reader {
	rc := b.instance[b.round]
	// split requets to diff buffer chan
	if b.round++; b.round == Conf.BufioInstance {
		b.round = 0
	}
	return rc
}

// newBufioReader get a Reader by chan, if chan empty new a Reader.
func newBufioReader(c chan *bufio.Reader, r io.Reader) *bufio.Reader {
	select {
	case p := <-c:
		p.Reset(r)
		return p
	default:
		log.Warn("tcp bufioReader cache empty")
		return bufio.NewReaderSize(r, Conf.RcvbufSize)
	}
}

// putBufioReader pub back a Reader to chan, if chan full discard it.
func putBufioReader(c chan *bufio.Reader, r *bufio.Reader) {
	r.Reset(nil)
	select {
	case c <- r:
	default:
		log.Warn("tcp bufioReader cache full")
	}
}

// StartTCP Start tcp listen.
func StartTCP() error {
	for _, bind := range Conf.TCPBind {
		log.Info("start tcp listen addr:\"%s\"", bind)
		go tcpListen(bind)
	}

	return nil
}

func tcpListen(bind string) {
	addr, err := net.ResolveTCPAddr("tcp", bind)
	if err != nil {
		log.Error("net.ResolveTCPAddr(\"tcp\"), %s) error(%v)", bind, err)
		panic(err)
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Error("net.ListenTCP(\"tcp4\", \"%s\") error(%v)", bind, err)
		panic(err)
	}
	// free the listener resource
	defer func() {
		log.Info("tcp addr: \"%s\" close", bind)
		if err := l.Close(); err != nil {
			log.Error("listener.Close() error(%v)", err)
		}
	}()
	// init reader buffer instance
	rb := newtcpBufCache()
	for {
		log.Debug("start accept")
		conn, err := l.AcceptTCP()
		if err != nil {
			log.Error("listener.AcceptTCP() error(%v)", err)
			continue
		}
		if err = conn.SetKeepAlive(Conf.TCPKeepalive); err != nil {
			log.Error("conn.SetKeepAlive() error(%v)", err)
			conn.Close()
			continue
		}
		if err = conn.SetReadBuffer(Conf.RcvbufSize); err != nil {
			log.Error("conn.SetReadBuffer(%d) error(%v)", Conf.RcvbufSize, err)
			conn.Close()
			continue
		}
		if err = conn.SetWriteBuffer(Conf.SndbufSize); err != nil {
			log.Error("conn.SetWriteBuffer(%d) error(%v)", Conf.SndbufSize, err)
			conn.Close()
			continue
		}
		// first packet must sent by client in specified seconds
		if err = conn.SetReadDeadline(time.Now().Add(fitstPacketTimedoutSec)); err != nil {
			log.Error("conn.SetReadDeadLine() error(%v)", err)
			conn.Close()
			continue
		}
		rc := rb.Get()
		// one connection one routine
		go handleTCPConn(conn, rc)
		log.Debug("accept finished")
	}
}

// hanleTCPConn handle a long live tcp connection.
func handleTCPConn(conn net.Conn, rc chan *bufio.Reader) {
	addr := conn.RemoteAddr().String()
	log.Debug("<%s> handleTcpConn routine start", addr)
	rd := newBufioReader(rc, conn)
	if args, err := parseCmd(rd); err == nil {
		// return buffer bufio.Reader
		putBufioReader(rc, rd)
		switch args[0] {
		case "sub":
			SubscribeTCPHandle(conn, args[1:])
			break
		default:
			conn.Write(ParamReply)
			log.Warn("<%s> unknown cmd \"%s\"", addr, args[0])
			break
		}
	} else {
		// return buffer bufio.Reader
		putBufioReader(rc, rd)
		log.Error("<%s> parseCmd() error(%v)", addr, err)
	}
	// close the connection
	if err := conn.Close(); err != nil {
		log.Error("<%s> conn.Close() error(%v)", addr, err)
	}
	log.Debug("<%s> handleTcpConn routine stop", addr)
	return
}

// SubscribeTCPHandle handle the subscribers's connection.
func SubscribeTCPHandle(conn net.Conn, args []string) {
	argLen := len(args)
	addr := conn.RemoteAddr().String()
	if argLen < 2 {
		conn.Write(ParamReply)
		log.Error("<%s> subscriber missing argument", addr)
		return
	}
	// key, heartbeat
	key := args[0]
	if key == "" {
		conn.Write(ParamReply)
		log.Warn("<%s> key param error", addr)
		return
	}
	heartbeatStr := args[1]
	i, err := strconv.Atoi(heartbeatStr)
	if err != nil {
		conn.Write(ParamReply)
		log.Error("<%s> user_key:\"%s\" heartbeat:\"%s\" argument error (%v)", addr, key, heartbeatStr, err)
		return
	}
	if i < minHearbeatSec {
		conn.Write(ParamReply)
		log.Warn("<%s> user_key:\"%s\" heartbeat argument error, less than %d", addr, key, minHearbeatSec)
		return
	}
	heartbeat := i + delayHeartbeatSec

	version := ""
	if argLen > 2 {
		version = args[2]
	}
	log.Info("<%s> subscribe to key = %s, heartbeat = %d, version = %s", addr, key, heartbeat, version)
	// fetch subscriber from the channel
	c, err := UserChannel.Get(key, true)
	if err != nil {
		log.Warn("<%s> user_key:\"%s\" can't get a channel (%s)", addr, key, err)
		if err == ErrChannelKey {
			conn.Write(NodeReply)
		} else {
			conn.Write(ChannelReply)
		}
		return
	}

	// add a conn to the channel
	connElem, err := c.AddConn(key, &Connection{Conn: conn, Proto: TCPProto, Version: version})
	if err != nil {
		log.Error("<%s> user_key:\"%s\" add conn error(%v)", addr, key, err)
		return
	}
	// blocking wait client heartbeat
	reply := []byte{0}
	// reply := make([]byte, HeartbeatLen)
	begin := time.Now().UnixNano()
	end := begin + Second
	for {
		// more then 1 sec, reset the timer
		if end-begin >= Second {
			if err = conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(heartbeat))); err != nil {
				log.Error("<%s> user_key:\"%s\" conn.SetReadDeadLine() error(%v)", addr, key, err)
				break
			}
			begin = end
		}
		if _, err = conn.Read(reply); err != nil {
			if err != io.EOF {
				log.Warn("<%s> user_key:\"%s\" conn.Read() failed, read heartbeat timedout error(%v)", addr, key, err)
			} else {
				// client connection close
				log.Warn("<%s> user_key:\"%s\" client connection close error(%v)", addr, key, err)
			}
			break
		}
		if string(reply) == Heartbeat {
			if _, err = conn.Write(HeartbeatReply); err != nil {
				log.Error("<%s> user_key:\"%s\" conn.Write() failed, write heartbeat to client error(%v)", addr, key, err)
				break
			}
			log.Debug("<%s> user_key:\"%s\" receive heartbeat (%s)", addr, key, reply)
		} else {
			log.Warn("<%s> user_key:\"%s\" unknown heartbeat protocol (%s)", addr, key, reply)
			break
		}
		end = time.Now().UnixNano()
	}
	// remove exists conn
	if err := c.RemoveConn(key, connElem); err != nil {
		log.Error("<%s> user_key:\"%s\" remove conn error(%v)", addr, key, err)
	}
	return
}

// parseCmd parse the tcp request command.
func parseCmd(rd *bufio.Reader) ([]string, error) {
	// get argument number
	argNum, err := parseCmdSize(rd, '*')
	if err != nil {
		log.Error("tcp:cmd format error when find '*' (%v)", err)
		return nil, err
	}
	if argNum < minCmdNum || argNum > maxCmdNum {
		log.Error("tcp:cmd argument number length error")
		return nil, ErrProtocol
	}
	args := make([]string, 0, argNum)
	for i := 0; i < argNum; i++ {
		// get argument length
		cmdLen, err := parseCmdSize(rd, '$')
		if err != nil {
			log.Error("tcp:parseCmdSize(rd, '$') error(%v)", err)
			return nil, err
		}
		// get argument data
		d, err := parseCmdData(rd, cmdLen)
		if err != nil {
			log.Error("tcp:parseCmdData() error(%v)", err)
			return nil, err
		}
		// append args
		args = append(args, string(d))
	}
	return args, nil
}

// parseCmdSize get the request protocol cmd size.
func parseCmdSize(rd *bufio.Reader, prefix uint8) (int, error) {
	// get command size
	cs, err := rd.ReadBytes('\n')
	if err != nil {
		log.Error("tcp:rd.ReadBytes('\\n') error(%v)", err)
		return 0, err
	}
	csl := len(cs)
	if csl <= 3 || cs[0] != prefix || cs[csl-2] != '\r' {
		log.Error("tcp:\"%v\"(%d) number format error, length error or prefix error or no \\r", cs, csl)
		return 0, ErrProtocol
	}
	// skip the \r\n
	cmdSize, err := strconv.Atoi(string(cs[1 : csl-2]))
	if err != nil {
		log.Error("tcp:\"%v\" number parse int error(%v)", cs, err)
		return 0, ErrProtocol
	}
	return cmdSize, nil
}

// parseCmdData get the sub request protocol cmd data not included \r\n.
func parseCmdData(rd *bufio.Reader, cmdLen int) ([]byte, error) {
	d, err := rd.ReadBytes('\n')
	if err != nil {
		log.Error("tcp:rd.ReadBytes('\\n') error(%v)", err)
		return nil, err
	}
	dl := len(d)
	// check last \r\n
	if dl != cmdLen+2 || d[dl-2] != '\r' {
		log.Error("tcp:\"%v\"(%d) number format error, length error or no \\r", d, dl)
		return nil, ErrProtocol
	}
	// skip last \r\n
	return d[0:cmdLen], nil
}
