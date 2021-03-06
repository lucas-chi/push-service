
package main

import (
	log "code.google.com/p/log4go"
	"errors"
	"flag"
	"github.com/lucas-chi/push-service/conf"
)

var (
	Conf               *Config
	ConfFile           string
	ErrNoConfigSection = errors.New("no config section")
)

func init() {
	flag.StringVar(&ConfFile, "c", "./comet-test.conf", " set gopush-cluster comet test config file path")
}

type Config struct {
	// base
	Addr      string `goconf:"base:addr"`
	Key       string `goconf:"base:key"`
	Heartbeat int    `goconf:"base:heartbeat"`
}

// InitConfig get a new Config struct.
func InitConfig(file string) (*Config, error) {
	cf := &Config{
		// base
		Addr:      "localhost:6969",
		Key:       "lucas-chi",
		Heartbeat: 30,
	}
	c := conf.New()
	if err := c.Parse(file); err != nil {
		log.Error("goconf.Parse(\"%s\") failed (%s)", file, err.Error())
		return nil, err
	}
	if err := c.Unmarshal(cf); err != nil {
		log.Error("goconf.Unmarshal() failed (%s)", err.Error())
		return nil, err
	}
	return cf, nil
}
