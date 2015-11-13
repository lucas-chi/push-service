package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
	log "code.google.com/p/log4go"
	myrpc "github.com/lucas-chi/push-service/rpc"
	"github.com/garyburd/redigo/redis"
)

const (
	userMsgNamespace string = "userMsg"
	userMsgExpire uint = 3600 * 10
)

var (
	RedisNoConnErr       = errors.New("can't get a redis conn")
	redisProtocolSpliter = "@"
)


// RedisMessage struct encoding the composite info.
type RedisPrivateMessage struct {
	Msg    json.RawMessage `json:"msg"`    // message content
	Expire int64           `json:"expire"` // expire second
}

// Struct for delele message
type RedisDelMessage struct {
	Key  string
	MIds []int64
}

type RedisStorage struct {
	pool  redis.Pool
	delCH chan *RedisDelMessage
}

// NewRedis initialize the redis pool.
func NewRedisStorage() *RedisStorage {
	log.Debug("RedisAddr : %s", Conf.RedisAddr)
	reg := regexp.MustCompile("(.+)@(.+)")
	pw := reg.FindStringSubmatch(Conf.RedisAddr)
	
	tmpProto := pw[1]
	tmpAddr := pw[2]
	
	if len(pw) < 2 {
		log.Error("strings.regexp(\"%s\", \"%s\") failed (%v)", tmpAddr, pw)
		panic(fmt.Sprintf("config redis.source node:\"%s\" format error", tmpAddr))
	}
	
	// WARN: closures use
	redisNode := &redis.Pool{
		MaxIdle:     Conf.RedisMaxIdle,
		MaxActive:   Conf.RedisMaxActive,
		IdleTimeout: Conf.RedisIdleTimeout,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial(tmpProto, tmpAddr)
			if err != nil {
				log.Error("redis.Dial(\"%s\", \"%s\") error(%v)", tmpProto, tmpAddr, err)
				return nil, err
			}
			return conn, err
		},
	}
	
	s := &RedisStorage{pool: *redisNode, delCH: make(chan *RedisDelMessage, 10240)}
	go s.clean()
	return s
}

// SavePrivate implements the Storage SavePrivate method.
func (s *RedisStorage) SavePrivate(key string, msg json.RawMessage, mid int64, expire uint) error {
	rm := &RedisPrivateMessage{Msg: msg, Expire: int64(expire) + time.Now().Unix()}
	m, err := json.Marshal(rm)
	if err != nil {
		log.Error("json.Marshal() key:\"%s\" error(%v)", key, err)
		return err
	}
	conn := s.getConn()
	if conn == nil {
		return RedisNoConnErr
	}
	defer conn.Close()
	
	if err = conn.Send("ZADD", key, mid, m); err != nil {
		log.Error("conn.Send(\"ZADD\", \"%s\", %d, \"%s\") error(%v)", key, mid, string(m), err)
		return err
	}
	if err = conn.Send("ZREMRANGEBYRANK", key, 0, -1*(Conf.RedisMaxStore+1)); err != nil {
		log.Error("conn.Send(\"ZREMRANGEBYRANK\", \"%s\", 0, %d) error(%v)", key, -1*(Conf.RedisMaxStore+1), err)
		return err
	}
	if err = conn.Flush(); err != nil {
		log.Error("conn.Flush() error(%v)", err)
		return err
	}
	if _, err = conn.Receive(); err != nil {
		log.Error("conn.Receive() error(%v)", err)
		return err
	}
	if _, err = conn.Receive(); err != nil {
		log.Error("conn.Receive() error(%v)", err)
		return err
	}
	return nil
}

// SavePrivates implements the Storage SavePrivates method.
func (s *RedisStorage) SavePrivates(keys []string, msg json.RawMessage, mid int64, expire uint) error {
	// raw msg
	rm := &RedisPrivateMessage{Msg: msg, Expire: int64(expire) + time.Now().Unix()}
	m, err := json.Marshal(rm)
	
	if err != nil {
		log.Error("json.Marshal() key:\"%s\" error(%v)", keys, err)
		return err
	}
	
	conn := s.getConn()
	defer conn.Close()
	
	if conn == nil {
		return RedisNoConnErr
	}
	
	for _, key := range keys {

		if err = conn.Send("ZADD", key, mid, m); err != nil {
			conn.Close()
			log.Error("conn.Send(\"ZADD\", \"%s\", %d, \"%s\") error(%v)", key, mid, string(m), err)
			return err
		}
		
		if err = conn.Send("ZREMRANGEBYRANK", key, 0, -1*(Conf.RedisMaxStore+1)); err != nil {
			conn.Close()
			log.Error("conn.Send(\"ZREMRANGEBYRANK\", \"%s\", 0, %d) error(%v)", key, -1*(Conf.RedisMaxStore+1), err)
			return err
		} 
		
	}
	
	// flush commands
	if err = conn.Flush(); err != nil {
		conn.Close()
		log.Error("conn.Flush() error(%v)", err)
		return err
	}
	
	// receive
	for j := 0; j < len(keys); j++ {
		if _, err = conn.Receive(); err != nil {
			conn.Close()
			log.Error("conn.Receive() error(%v)", err)
				return err
			}

			if _, err = conn.Receive(); err != nil {
				conn.Close()
				log.Error("conn.Receive() error(%v)", err)
			return err
		}
	}
	
	return nil
}

// GetPrivate implements the Storage GetPrivate method.
func (s *RedisStorage) GetPrivate(key string, mid int64) ([]*myrpc.Message, error) {
	conn := s.getConn()
	if conn == nil {
		return nil, RedisNoConnErr
	}
	defer conn.Close()
	values, err := redis.Values(conn.Do("ZRANGEBYSCORE", key, fmt.Sprintf("(%d", mid), "+inf", "WITHSCORES"))
	if err != nil {
		log.Error("conn.Do(\"ZRANGEBYSCORE\", \"%s\", \"%d\", \"+inf\", \"WITHSCORES\") error(%v)", key, mid, err)
		return nil, err
	}
	msgs := make([]*myrpc.Message, 0, len(values))
	delMsgs := []int64{}
	now := time.Now().Unix()
	for len(values) > 0 {
		cmid := int64(0)
		b := []byte{}
		values, err = redis.Scan(values, &b, &cmid)
		if err != nil {
			log.Error("redis.Scan() error(%v)", err)
			return nil, err
		}
		rm := &RedisPrivateMessage{}
		if err = json.Unmarshal(b, rm); err != nil {
			log.Error("json.Unmarshal(\"%s\", rm) error(%v)", string(b), err)
			delMsgs = append(delMsgs, cmid)
			continue
		}
		// check expire
		if rm.Expire < now {
			log.Warn("user_key: \"%s\" msg: %d expired", key, cmid)
			delMsgs = append(delMsgs, cmid)
			continue
		}
		m := &myrpc.Message{MsgId: cmid, Msg: rm.Msg, GroupId: myrpc.PrivateGroupId}
		msgs = append(msgs, m)
	}
	// delete unmarshal failed and expired message
	if len(delMsgs) > 0 {
		select {
		case s.delCH <- &RedisDelMessage{Key: key, MIds: delMsgs}:
		default:
			log.Warn("user_key: \"%s\" send del messages failed, channel full", key)
		}
	}
	return msgs, nil
}

// DelPrivate implements the Storage DelPrivate method.
func (s *RedisStorage) DelPrivate(key string) error {
	conn := s.getConn()
	if conn == nil {
		return RedisNoConnErr
	}
	defer conn.Close()
	if _, err := conn.Do("DEL", key); err != nil {
		log.Error("conn.Do(\"DEL\", \"%s\") error(%v)", key, err)
		return err
	}
	return nil
}

// DelMulti implements the Storage DelMulti method.
func (s *RedisStorage) clean() {
	for {
		info := <-s.delCH
		conn := s.getConn()
		if conn == nil {
			log.Warn("get redis connection nil")
			continue
		}
		for _, mid := range info.MIds {
			if err := conn.Send("ZREMRANGEBYSCORE", info.Key, mid, mid); err != nil {
				log.Error("conn.Send(\"ZREMRANGEBYSCORE\", \"%s\", %d, %d) error(%v)", info.Key, mid, mid, err)
				conn.Close()
				continue
			}
		}
		if err := conn.Flush(); err != nil {
			log.Error("conn.Flush() error(%v)", err)
			conn.Close()
			continue
		}
		for _, _ = range info.MIds {
			_, err := conn.Receive()
			if err != nil {
				log.Error("conn.Receive() error(%v)", err)
				conn.Close()
				continue
			}
		}
		conn.Close()
	}
}

// SavePrivate implements the Storage SaveUserMsg method.
func (s *RedisStorage) SaveUserMsg(sessionId string, msg json.RawMessage, mid int64, expire uint) error {
	key := fmt.Sprintf("%s.%s", userMsgNamespace, sessionId)
	rm := &RedisPrivateMessage{Msg: msg, Expire: int64(expire) + time.Now().Unix()}
	m, err := json.Marshal(rm)
	if err != nil {
		log.Error("json.Marshal() key:\"%s\" error(%v)", key, err)
		return err
	}
	conn := s.getConn()
	if conn == nil {
		return RedisNoConnErr
	}
	defer conn.Close()
	
	if err = conn.Send("ZADD", key, mid, m); err != nil {
		log.Error("conn.Send(\"ZADD\", \"%s\", %d, \"%s\") error(%v)", key, mid, string(m), err)
		return err
	}
	if err = conn.Send("ZREMRANGEBYRANK", key, 0, -1*(Conf.RedisMaxStore+1)); err != nil {
		log.Error("conn.Send(\"ZREMRANGEBYRANK\", \"%s\", 0, %d) error(%v)", key, -1*(Conf.RedisMaxStore+1), err)
		return err
	}
	if err = conn.Flush(); err != nil {
		log.Error("conn.Flush() error(%v)", err)
		return err
	}
	if _, err = conn.Receive(); err != nil {
		log.Error("conn.Receive() error(%v)", err)
		return err
	}
	if _, err = conn.Receive(); err != nil {
		log.Error("conn.Receive() error(%v)", err)
		return err
	}
	return nil
}

// GetUserMsg implements the Storage GetUserMsg method.
func (s *RedisStorage) GetUserMsg(sessionId string) ([]*myrpc.Message, error) {
	key := fmt.Sprintf("%s.%s", userMsgNamespace, sessionId)
	conn := s.getConn()
	if conn == nil {
		return nil, RedisNoConnErr
	}
	defer conn.Close()
	values, err := redis.Values(conn.Do("ZRANGEBYSCORE", key, "-inf +inf", "WITHSCORES"))
	if err != nil {
		log.Error("conn.Do(\"ZRANGEBYSCORE\", \"%s\", \"%d\", \"-inf +inf\", \"WITHSCORES\") error(%v)", key, err)
		return nil, err
	}
	msgs := make([]*myrpc.Message, 0, len(values))
	delMsgs := []int64{}
	now := time.Now().Unix()
	for len(values) > 0 {
		cmid := int64(0)
		b := []byte{}
		values, err = redis.Scan(values, &b, &cmid)
		if err != nil {
			log.Error("redis.Scan() error(%v)", err)
			return nil, err
		}
		rm := &RedisPrivateMessage{}
		if err = json.Unmarshal(b, rm); err != nil {
			log.Error("json.Unmarshal(\"%s\", rm) error(%v)", string(b), err)
			delMsgs = append(delMsgs, cmid)
			continue
		}
		// check expire
		if rm.Expire < now {
			log.Warn("user_key: \"%s\" msg: %d expired", key, cmid)
			delMsgs = append(delMsgs, cmid)
			continue
		}
		m := &myrpc.Message{MsgId: cmid, Msg: rm.Msg}
		msgs = append(msgs, m)
	}
	// delete unmarshal failed and expired message
	if len(delMsgs) > 0 {
		select {
		case s.delCH <- &RedisDelMessage{Key: key, MIds: delMsgs}:
		default:
			log.Warn("user_key: \"%s\" send del messages failed, channel full", key)
		}
	}
	return msgs, nil
}

// getConn get the connection
func (s *RedisStorage) getConn() redis.Conn {
	return s.pool.Get()
}
