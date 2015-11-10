
package main

import (
	// TODO change the 3rd websocket lib
	"golang.org/x/net/websocket"
	log "code.google.com/p/log4go"
	"net"
	"net/http"
	"strconv"
	"time"
)

type KeepAliveListener struct {
	net.Listener
}

func (l *KeepAliveListener) Accept() (c net.Conn, err error) {
	c, err = l.Listener.Accept()
	if err != nil {
		log.Error("Listener.Accept() error(%v)", err)
		return
	}
	// set keepalive
	if tc, ok := c.(*net.TCPConn); !ok {
		log.Error("net.TCPConn assection type failed")
		panic("Assection type failed c.(net.TCPConn)")
	} else {
		err = tc.SetKeepAlive(true)
		if err != nil {
			log.Error("tc.SetKeepAlive(true) error(%v)", err)
			return
		}
	}
	return
}

// StartHttp start http listen.
func StartWebsocket() error {
	for _, bind := range Conf.WebsocketBind {
		log.Info("start websocket listen addr:\"%s\"", bind)
		go websocketListen(bind)
	}

	return nil
}

func websocketListen(bind string) {
	httpServeMux := http.NewServeMux()
	httpServeMux.Handle("/sub", websocket.Handler(SubscribeHandle))
	if Conf.TCPKeepalive {
		server := &http.Server{Handler: httpServeMux}
		l, err := net.Listen("tcp", bind)
		if err != nil {
			log.Error("net.Listen(\"tcp\", \"%s\") error(%v)", bind, err)
			panic(err)
		}
		if err := server.Serve(&KeepAliveListener{Listener: l}); err != nil {
			log.Error("server.Serve(\"%s\") error(%v)", bind, err)
			panic(err)
		}
	} else {
		if err := http.ListenAndServe(bind, httpServeMux); err != nil {
			log.Error("http.ListenAdServe(\"%s\") error(%v)", bind, err)
			panic(err)
		}
	}
}

// Subscriber Handle is the websocket handle for sub request.
func SubscribeHandle(ws *websocket.Conn) {
	log.Debug("connected to websocket...")
	addr := ws.Request().RemoteAddr
	params := ws.Request().URL.Query()
	// get subscriber key
	key := params.Get("key")
	if key == "" {
		ws.Write(ParamReply)
		log.Warn("<%s> key param error", addr)
		return
	}
	// get heartbeat second
	heartbeatStr := params.Get("heartbeat")
	i, err := strconv.Atoi(heartbeatStr)
	if err != nil {
		ws.Write(ParamReply)
		log.Error("<%s> user_key:\"%s\" heartbeat argument error(%v)", addr, key, err)
		return
	}
	if i < minHearbeatSec {
		ws.Write(ParamReply)
		log.Warn("<%s> user_key:\"%s\" heartbeat argument error, less than %d", addr, key, minHearbeatSec)
		return
	}
	heartbeat := i + delayHeartbeatSec

	version := params.Get("ver")
	log.Info("<%s> subscribe to key = %s, heartbeat = %d, version = %s", addr, key, heartbeat, version)
	// fetch subscriber from the channel
	c, err := UserChannel.Get(key, true)
	if err != nil {
		log.Warn("<%s> user_key:\"%s\" can't get a channel (%s)", addr, key, err)
		if err == ErrChannelKey {
			ws.Write(NodeReply)
		} else {
			ws.Write(ChannelReply)
		}
		return
	}

	// add a conn to the channel
	connElem, err := c.AddConn(key, &Connection{Conn: ws, Proto: WebsocketProto, Version: version})
	if err != nil {
		log.Error("<%s> user_key:\"%s\" add conn error(%v)", addr, key, err)
		return
	}
	// blocking wait client heartbeat
	reply := ""
	begin := time.Now().UnixNano()
	end := begin + Second
	for {
		// more then 1 sec, reset the timer
		if end-begin >= Second {
			if err = ws.SetReadDeadline(time.Now().Add(time.Second * time.Duration(heartbeat))); err != nil {
				log.Error("<%s> user_key:\"%s\" websocket.SetReadDeadline() error(%v)", addr, key, err)
				break
			}
			begin = end
		}
		if err = websocket.Message.Receive(ws, &reply); err != nil {
			log.Error("<%s> user_key:\"%s\" websocket.Message.Receive() error(%v)", addr, key, err)
			break
		}
		if reply == Heartbeat {
			if _, err = ws.Write(HeartbeatReply); err != nil {
				log.Error("<%s> user_key:\"%s\" write heartbeat to client error(%s)", addr, key, err)
				break
			}
			log.Debug("<%s> user_key:\"%s\" receive heartbeat", addr, key)
		} else {
			log.Warn("<%s> user_key:\"%s\" unknown heartbeat protocol", addr, key)
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
