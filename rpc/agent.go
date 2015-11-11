
package rpc

import (
	log "code.google.com/p/log4go"
	"encoding/json"
	myzk "github.com/lucas-chi/push-service/zk"
	"github.com/samuel/go-zookeeper/zk"
	"net/rpc"
	"path"
	"time"
)

const (
	// agent rpc service
	AgentService             = "AgentRPC"
	AgentServiceReply  = "AgentRPC.ReplyMessage"
)

var (
	AgentRPC *RandLB
)

func init() {
	AgentRPC, _ = NewRandLB(map[string]*WeightRpc{}, AgentService, 0, 0, false)
}

type AgentNodeEvent struct {
	Key *WeightRpc
	// event type
	Event int
}

// Agent node info
type AgentNodeInfo struct {
	Rpc    []string `json:"rpc"`
	Weight int      `json:"weight"`
}

// Message Reply args
type MessageReplyArgs struct {
	SessionId    string          //  session ID
	Msg    json.RawMessage // message content
	NewSession bool			// new session 
}

// Message SavePrivates response
type MessageReplyResp struct {
	FKeys []string // failed key
}

// watchAgentRoot watch the agent root path.
func watchAgentRoot(conn *zk.Conn, fpath string, ch chan *AgentNodeEvent) error {
	for {
		nodes, watch, err := myzk.GetNodesW(conn, fpath)
		if err == myzk.ErrNodeNotExist {
			log.Warn("zk don't have node \"%s\", retry in %d second", fpath, waitNodeDelay)
			time.Sleep(waitNodeDelaySecond)
			continue
		} else if err == myzk.ErrNoChild {
			log.Warn("zk don't have any children in \"%s\", retry in %d second", fpath, waitNodeDelay)
			// all child died, kick all the nodes
			for _, client := range AgentRPC.Clients {
				log.Debug("node: \"%s\" send del node event", client.Addr)
				ch <- &AgentNodeEvent{Event: eventNodeDel, Key: &WeightRpc{Addr: client.Addr, Weight: client.Weight}}
			}
			time.Sleep(waitNodeDelaySecond)
			continue
		} else if err != nil {
			log.Error("getNodes error(%v), retry in %d second", err, waitNodeDelay)
			time.Sleep(waitNodeDelaySecond)
			continue
		}
		nodesMap := map[string]bool{}
		// handle new add nodes
		for _, node := range nodes {
			data, _, err := conn.Get(path.Join(fpath, node))
			if err != nil {
				log.Error("zk.Get(\"%s\") error(%v)", path.Join(fpath, node), err)
				continue
			}
			// parse agent node info
			nodeInfo := &AgentNodeInfo{}
			if err := json.Unmarshal(data, nodeInfo); err != nil {
				log.Error("json.Unmarshal(\"%s\", nodeInfo) error(%v)", string(data), err)
				continue
			}
			for _, addr := range nodeInfo.Rpc {
				// if not exists in old map then trigger a add event
				if _, ok := AgentRPC.Clients[addr]; !ok {
					ch <- &AgentNodeEvent{Event: eventNodeAdd, Key: &WeightRpc{Addr: addr, Weight: nodeInfo.Weight}}
				}
				nodesMap[addr] = true
			}
		}
		// handle delete nodes
		for _, client := range AgentRPC.Clients {
			if _, ok := nodesMap[client.Addr]; !ok {
				ch <- &AgentNodeEvent{Event: eventNodeDel, Key: client}
			}
		}
		// blocking wait node changed
		event := <-watch
		log.Info("zk path: \"%s\" receive a event %v", fpath, event)
	}
}

// handleNodeEvent add and remove AgentRPC.Clients, copy the src map to a new map then replace the variable.
func handleAgentNodeEvent(conn *zk.Conn, retry, ping time.Duration, ch chan *AgentNodeEvent) {
	for {
		ev := <-ch
		// copy map from src
		tmpAgentRPCMap := make(map[string]*WeightRpc, len(AgentRPC.Clients))
		for k, v := range AgentRPC.Clients {
			tmpAgentRPCMap[k] = &WeightRpc{Client: v.Client, Addr: v.Addr, Weight: v.Weight}
			// reuse rpc connection
			v.Client = nil
		}
		// handle event
		if ev.Event == eventNodeAdd {
			log.Info("add agent rpc node: \"%s\"", ev.Key.Addr)
			rpcTmp, err := rpc.Dial("tcp", ev.Key.Addr)
			if err != nil {
				log.Error("rpc.Dial(\"tcp\", \"%s\") error(%v)", ev.Key, err)
				log.Warn("discard agent rpc node: \"%s\", connect failed", ev.Key)
				continue
			}
			ev.Key.Client = rpcTmp
			tmpAgentRPCMap[ev.Key.Addr] = ev.Key
		} else if ev.Event == eventNodeDel {
			log.Info("del agent rpc node: \"%s\"", ev.Key.Addr)
			delete(tmpAgentRPCMap, ev.Key.Addr)
		} else {
			log.Error("unknown node event: %d", ev.Event)
			panic("unknown node event")
		}
		tmpAgentRPC, err := NewRandLB(tmpAgentRPCMap, AgentService, retry, ping, true)
		if err != nil {
			log.Error("NewRandLR() error(%v)", err)
			panic(err)
		}
		oldAgentRPC := AgentRPC
		// atomic update
		AgentRPC = tmpAgentRPC
		// release resource
		oldAgentRPC.Destroy()
		log.Debug("AgentRPC.Client length: %d", len(AgentRPC.Clients))
	}
}

// InitAgent init a rand lb rpc for agent module.
func InitAgent(conn *zk.Conn, fpath string, retry, ping time.Duration) {
	// watch agent path
	ch := make(chan *AgentNodeEvent, 1024)
	go handleAgentNodeEvent(conn, retry, ping, ch)
	go watchAgentRoot(conn, fpath, ch)
}
