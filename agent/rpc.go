
package main

import (
	log "code.google.com/p/log4go"
	"errors"
	myrpc "github.com/lucas-chi/push-service/rpc"
	"net"
	"net/rpc"
	"encoding/json"
	"github.com/lucas-chi/push-service/robot"
)

var (
	ErrCometNodeNotExist = errors.New("Comet node not exist")
	ErrInternal = errors.New("Internal error")
)

// Agent start rpc listen.
func InitRPC() error {
	c := &AgentRPC{}
	rpc.Register(c)
	for _, bind := range Conf.RPCBind {
		log.Info("start listen rpc addr: \"%s\"", bind)
		go rpcListen(bind)
	}

	return nil
}

func rpcListen(bind string) {
	l, err := net.Listen("tcp", bind)
	if err != nil {
		log.Error("net.Listen(\"tcp\", \"%s\") error(%v)", bind, err)
		panic(err)
	}
	// if process exit, then close the rpc bind
	defer func() {
		log.Info("rpc addr: \"%s\" close", bind)
		if err := l.Close(); err != nil {
			log.Error("listener.Close() error(%v)", err)
		}
	}()
	rpc.Accept(l)
}

// Agent RPC
type AgentRPC struct {
}


// Reply message expored a method for replying a user message.
// if it`s going failed then it`ll return an error
func (c *AgentRPC) ReplyMessage(args *myrpc.MessageReplyArgs, ret *int) error {
	if args == nil || args.SessionId == "" {
		return myrpc.ErrParam
	}
	
	node := myrpc.GetComet(args.SessionId)
	
	if node == nil || node.Rpc == nil {
		return ErrCometNodeNotExist
	}
	client := node.Rpc
	
	if client == nil {
		return ErrCometNodeNotExist
	}
	
	log.Debug("received from session id:<%s> , message:\"%s\"", args.SessionId, args.Msg)
	var reply string
	
	if args.NewSession {
		reply = robot.Welcome()
	} else {
		reply = robot.FindReply(string(args.Msg))
	}
	
	pushArgs := &myrpc.CometPushPrivateArgs{Msg: json.RawMessage(reply), Expire: 0, Key: args.SessionId}
	
	if err := client.Call(myrpc.CometServicePushPrivate, pushArgs, &ret); err != nil {
		log.Error("client.Call(\"%s\", \"%s\", &ret) error(%v)", myrpc.CometServicePushPrivate, pushArgs.Key, err)
		return ErrInternal
	}
	return nil
}

// Server Ping interface
func (r *AgentRPC) Ping(p int, ret *int) error {
	log.Debug("ping ok")
	return nil
}