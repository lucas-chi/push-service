
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
	var err error
	// Parse cmd-line arguments
	flag.Parse()
	log.Info("agent ver: \"%s\" start", ver.Version)
	if err = InitConfig(); err != nil {
		panic(err)
	}
	// Set max routine
	runtime.GOMAXPROCS(Conf.MaxProc)
	// init log
	log.LoadConfiguration(Conf.Log)
	defer log.Close()
	
	// init rpc service
	if err = InitRPC(); err != nil {
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
	// start pprof http
	perf.Init(Conf.PprofBind)
	// start http listen.
	StartHTTP()
	// process init
	if err = process.Init(Conf.User, Conf.Dir, Conf.PidFile); err != nil {
		panic(err)
	}
	// init signals, block wait signals
	signalCH := InitSignal()
	HandleSignal(signalCH)
	log.Info("web stop")
}
