
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

// InitConfig initialize config file path
func init() {
	flag.StringVar(&confFile, "c", "./web.conf", " set web config file path")
}

type Config struct {
	HttpBind             []string      `goconf:"base:http.bind:,"`
	AdminBind            []string      `goconf:"base:admin.bind:,"`
	HttpServerTimeout    time.Duration `goconf:"base:http.servertimeout:time"`
	MaxProc              int           `goconf:"base:maxproc"`
	PprofBind            []string      `goconf:"base:pprof.bind:,"`
	User                 string        `goconf:"base:user"`
	PidFile              string        `goconf:"base:pidfile"`
	Dir                  string        `goconf:"base:dir"`
	Log                  string        `goconf:"base:log"`
	ZookeeperAddr        []string      `goconf:"zookeeper:addr:,"`
	ZookeeperTimeout     time.Duration `goconf:"zookeeper:timeout:time"`
	ZookeeperCometPath   string        `goconf:"zookeeper:comet.path"`
	ZookeeperMessagePath string        `goconf:"zookeeper:message.path"`
	ZookeeperMigratePath string        `goconf:"zookeeper:migrate.path"`
	RPCRetry             time.Duration `goconf:"rpc:retry:time"`
	RPCPing              time.Duration `goconf:"rpc:ping:time"`
}

// InitConfig init configuration file.
func InitConfig() error {
	gconf := conf.New()
	if err := gconf.Parse(confFile); err != nil {
		return err
	}
	// Default config
	Conf = &Config{
		HttpBind:             []string{"localhost:80"},
		AdminBind:            []string{"localhost:81"},
		HttpServerTimeout:    10 * time.Second,
		MaxProc:              runtime.NumCPU(),
		PprofBind:            []string{"localhost:8190"},
		User:                 "nobody nobody",
		PidFile:              "/tmp/gopush-cluster-web.pid",
		Dir:                  "./",
		Log:                  "./log/xml",
		ZookeeperAddr:        []string{":2181"},
		ZookeeperTimeout:     30 * time.Second,
		ZookeeperCometPath:   "/gopush-cluster-comet",
		ZookeeperMessagePath: "/gopush-cluster-message",
		ZookeeperMigratePath: "/gopush-migrate-lock",
		RPCRetry:             3 * time.Second,
		RPCPing:              1 * time.Second,
	}
	if err := gconf.Unmarshal(Conf); err != nil {
		return err
	}
	return nil
}
