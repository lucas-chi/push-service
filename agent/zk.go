
package main

import (
	log "code.google.com/p/log4go"
	myrpc "github.com/lucas-chi/push-service/rpc"
	myzk "github.com/lucas-chi/push-service/zk"
	"github.com/samuel/go-zookeeper/zk"
	"encoding/json"
)

func InitZK() (*zk.Conn, error) {
	conn, err := myzk.Connect(Conf.ZookeeperAddr, Conf.ZookeeperTimeout)
	if err != nil {
		log.Error("zk.Connect() error(%v)", err)
		return nil, err
	}
	
	if err = myzk.Create(conn, Conf.ZookeeperAgentPath); err != nil {
		log.Error("zk.Create() error(%v)", err)
		return conn, err
	}
	// agent rpc bind address store in the zk
	nodeInfo := &myrpc.AgentNodeInfo{}
	nodeInfo.Rpc = Conf.RPCBind
	nodeInfo.Weight = Conf.ZookeeperAgentNodeWeight
	data, err := json.Marshal(nodeInfo)
	if err != nil {
		log.Error("json.Marshal() error(%v)", err)
		return conn, err
	}
	log.Debug("zk data: \"%s\"", string(data))
	if err = myzk.RegisterTemp(conn, Conf.ZookeeperAgentPath, data); err != nil {
		log.Error("zk.RegisterTemp() error(%v)", err)
		return conn, err
	}
	myrpc.InitComet(conn, Conf.ZookeeperMigratePath, Conf.ZookeeperCometPath, Conf.RPCRetry, Conf.RPCPing)
	myrpc.InitMessage(conn, Conf.ZookeeperMessagePath, Conf.RPCRetry, Conf.RPCPing)
	return conn, nil
}
