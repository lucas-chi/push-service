
package main

import (
	log "code.google.com/p/log4go"
	"encoding/json"
	"github.com/lucas-chi/push-service/rpc"
	myzk "github.com/lucas-chi/push-service/zk"
	"github.com/samuel/go-zookeeper/zk"
	"path"
	"time"
)

const (
	// wait node
	waitNodeDelay       = 3
	waitNodeDelaySecond = waitNodeDelay * time.Second
)

func InitZK() (*zk.Conn, error) {
	conn, err := myzk.Connect(Conf.ZookeeperAddr, Conf.ZookeeperTimeout)
	if err != nil {
		log.Error("myzk.Connect() error(%v)", err)
		return nil, err
	}
	fpath := path.Join(Conf.ZookeeperCometPath, Conf.ZookeeperCometNode)
	if err = myzk.Create(conn, fpath); err != nil {
		log.Error("myzk.Create(conn,\"%s\",\"\") error(%v)", fpath, err)
		return conn, err
	}
	// comet tcp, websocket and rpc bind address store in the zk
	nodeInfo := &rpc.CometNodeInfo{}
	nodeInfo.RpcAddr = Conf.RPCBind
	nodeInfo.TcpAddr = Conf.TCPBind
	nodeInfo.WsAddr = Conf.WebsocketBind
	nodeInfo.Weight = Conf.ZookeeperCometWeight
	data, err := json.Marshal(nodeInfo)
	if err != nil {
		log.Error("json.Marshal() error(%v)", err)
		return conn, err
	}
	log.Debug("myzk node:\"%s\" registe data: \"%s\"", fpath, string(data))
	if err = myzk.RegisterTemp(conn, fpath, data); err != nil {
		log.Error("myzk.RegisterTemp() error(%v)", err)
		return conn, err
	}
	// watch and update
	rpc.InitMessage(conn, Conf.ZookeeperMessagePath, Conf.RPCRetry, Conf.RPCPing)
	return conn, nil
}
