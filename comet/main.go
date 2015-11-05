
package main

import (
	log "code.google.com/p/log4go"
	"flag"
	"github.com/lucas-chi/push-service/perf"
	"github.com/lucas-chi/push-service/process"
	"github.com/lucas-chi/push-service/ver"
	"runtime"
)

func main() {
	// parse cmd-line arguments
	flag.Parse()
	log.Info("comet ver: \"%s\" start", ver.Version)
	// init config
	if err := InitConfig(); err != nil {
		panic(err)
	}
	// set max routine
	runtime.GOMAXPROCS(Conf.MaxProc)
	// init log
	log.LoadConfiguration(Conf.Log)
	defer log.Close()
	// start pprof
	perf.Init(Conf.PprofBind)
	// create channel
	// if process exit, close channel
	UserChannel = NewChannelList()
	defer UserChannel.Close()

	// start rpc
	if err := StartRPC(); err != nil {
		panic(err)
	}
	// start comet
	if err := StartComet(); err != nil {
		panic(err)
	}
	// init zookeeper
	zkConn, err := InitZK()
	if err != nil {
		if zkConn != nil {
			zkConn.Close()
		}
		panic(err)
	}
	// process init
	if err = process.Init(Conf.User, Conf.Dir, Conf.PidFile); err != nil {
		panic(err)
	}
	// init signals, block wait signals
	signalCH := InitSignal()
	HandleSignal(signalCH)
	// exit
	log.Info("comet stop")
}
