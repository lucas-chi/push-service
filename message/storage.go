package main

import (
	log "code.google.com/p/log4go"
	"encoding/json"
	"errors"
	"github.com/lucas-chi/push-service/rpc"
)

const (
	RedisStorageType = "redis"
	ketamaBase       = 255
	saveBatchNum     = 1000
)

var (
	UseStorage     Storage
	ErrStorageType = errors.New("unknown storage type")
)

// Stored messages interface
type Storage interface {
	// GetPrivate get private msgs.
	GetPrivate(key string, mid int64) ([]*rpc.Message, error)
	// SavePrivate Save single private msg.
	SavePrivate(key string, msg json.RawMessage, mid int64, expire uint) error
	// Save private msgs return failed keys.
	SavePrivates(keys []string, msg json.RawMessage, mid int64, expire uint) error
	// DelPrivate delete private msgs.
	DelPrivate(key string) error
}

// InitStorage init the storage type(mysql or redis).
func InitStorage() error {
	if Conf.StorageType == RedisStorageType {
		UseStorage = NewRedisStorage()
	} else {
		log.Error("unknown storage type: \"%s\"", Conf.StorageType)
		return ErrStorageType
	}
	return nil
}
