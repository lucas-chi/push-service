package robot

import (
	myrpc "github.com/lucas-chi/push-service/rpc"
)

const (
	defaultReply = "爱你一万年！"
)

var (
	dic = map[string]string{
		"卖个萌让我开心一下": "今天没吃药，感觉萌萌哒",
		"如何才能召唤你": "主人来都来了，评价一个先，拜托拜托～",
		"你喜欢干什么？":"如果经济条件允许的话,我想要到各地去旅游。",
	}
)

func FindReply(msg string) string {
	if reply, ok := dic[msg];ok {
		return reply
	} else {
		return defaultReply
	}
}

func Welcome() *myrpc.MessageGetResp {
	msgs := make([]*myrpc.Message, 0, 1)
	m := &myrpc.Message{MsgId: 0, Msg: []byte("尊敬的用户，我将竭诚为您服务！")}
	msgs[0] = m
	return &myrpc.MessageGetResp{Msgs : msgs, ContentType : myrpc.TextContentType}
}