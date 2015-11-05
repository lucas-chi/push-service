
package main

import (
	log "code.google.com/p/log4go"
	myrpc "github.com/lucas-chi/push-service/rpc"
	myzk "github.com/lucas-chi/push-service/zk"
	"github.com/samuel/go-zookeeper/zk"
)

func InitZK() (*zk.Conn, error) {
	conn, err := myzk.Connect(Conf.ZookeeperAddr, Conf.ZookeeperTimeout)
	if err != nil {
		log.Error("zk.Connect() error(%v)", err)
		return nil, err
	}
	myrpc.InitComet(conn, Conf.ZookeeperMigratePath, Conf.ZookeeperCometPath, Conf.RPCRetry, Conf.RPCPing)
	myrpc.InitMessage(conn, Conf.ZookeeperMessagePath, Conf.RPCRetry, Conf.RPCPing)
	return conn, nil
}
