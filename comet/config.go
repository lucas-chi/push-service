
package main

import (
	"flag"
	"github.com/lucas-chi/push-service/conf"
	"runtime"
	"time"
)

var (
	Conf     *Config
	confFile string
)

func init() {
	flag.StringVar(&confFile, "c", "./comet.conf", " set gopush-cluster comet config file path")
}

type Config struct {
	// base
	User          string   `goconf:"base:user"`
	PidFile       string   `goconf:"base:pidfile"`
	Dir           string   `goconf:"base:dir"`
	Log           string   `goconf:"base:log"`
	MaxProc       int      `goconf:"base:maxproc"`
	TCPBind       []string `goconf:"base:tcp.bind:,"`
	WebsocketBind []string `goconf:"base:websocket.bind:,"`
	RPCBind       []string `goconf:"base:rpc.bind:,"`
	PprofBind     []string `goconf:"base:pprof.bind:,"`
	StatBind      []string `goconf:"base:stat.bind:,"`
	// zookeeper
	ZookeeperAddr        []string      `goconf:"zookeeper:addr:,"`
	ZookeeperTimeout     time.Duration `goconf:"zookeeper:timeout:time"`
	ZookeeperCometPath   string        `goconf:"zookeeper:comet.path"`
	ZookeeperCometNode   string        `goconf:"zookeeper:comet.node"`
	ZookeeperCometWeight int           `goconf:"zookeeper:comet.weight"`
	ZookeeperMessagePath string        `goconf:"zookeeper:message.path"`
	ZookeeperAgentPath string          `goconf:"zookeeper:agent.path"`
	// rpc
	RPCPing  time.Duration `goconf:"rpc:ping:time"`
	RPCRetry time.Duration `goconf:"rpc:retry:time"`
	// channel
	SndbufSize              int           `goconf:"channel:sndbuf.size:memory"`
	RcvbufSize              int           `goconf:"channel:rcvbuf.size:memory"`
	Proto                   []string      `goconf:"channel:proto:,"`
	BufioInstance           int           `goconf:"channel:bufio.instance"`
	BufioNum                int           `goconf:"channel:bufio.num"`
	TCPKeepalive            bool          `goconf:"channel:tcp.keepalive"`
	MaxSubscriberPerChannel int           `goconf:"channel:maxsubscriber"`
	ChannelBucket           int           `goconf:"channel:bucket"`
	MsgBufNum               int           `goconf:"channel:msgbuf.num"`
}

// InitConfig get a new Config struct.
func InitConfig() error {
	Conf = &Config{
		// base
		User:          "nobody nobody",
		PidFile:       "/tmp/gopush-cluster-comet.pid",
		Dir:           "./",
		Log:           "./log/xml",
		MaxProc:       runtime.NumCPU(),
		WebsocketBind: []string{"localhost:6968"},
		TCPBind:       []string{"localhost:6969"},
		RPCBind:       []string{"localhost:6970"},
		PprofBind:     []string{"localhost:6971"},
		StatBind:      []string{"localhost:6972"},
		// zookeeper
		ZookeeperAddr:        []string{"localhost:2181"},
		ZookeeperTimeout:     30 * time.Second,
		ZookeeperCometPath:   "/gopush-cluster-comet",
		ZookeeperCometNode:   "node1",
		ZookeeperCometWeight: 1,
		ZookeeperMessagePath: "/gopush-cluster-message",
		ZookeeperAgentPath: "/gopush-cluster-agent",
		// rpc
		RPCPing:  1 * time.Second,
		RPCRetry: 1 * time.Second,
		// channel
		SndbufSize:              2048,
		RcvbufSize:              256,
		Proto:                   []string{"tcp", "websocket"},
		BufioInstance:           runtime.NumCPU(),
		BufioNum:                128,
		TCPKeepalive:            false,
		MaxSubscriberPerChannel: 64,
		ChannelBucket:           runtime.NumCPU(),
		MsgBufNum:               30,
	}
	c := conf.New()
	if err := c.Parse(confFile); err != nil {
		return err
	}
	if err := c.Unmarshal(Conf); err != nil {
		return err
	}
	return nil
}
