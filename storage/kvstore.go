package storage

import (
	"errors"
	"net/rpc"

	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("storage")

type KVStore struct {
	db map[string]string

	requestHandlers *RequestHandlers
}

func NewKVStore() *KVStore {
	kvs := &KVStore{}
	requestHandlers := NewRequestHandler(kvs)
	kvs.requestHandlers = requestHandlers

	return kvs
}

func (k *KVStore) Get(key string) (string, error) {
	value, ok := k.db[key]
	if !ok {
		return "", errors.New("key not found")
	}

	return value, nil
}

func (k *KVStore) Put(key, value string) error {
	k.db[key] = value
	return nil
}

func (k *KVStore) Delete(key string) error {
	_, ok := k.db[key]
	if !ok {
		return errors.New("key not found")
	}

	delete(k.db, key)
	return nil
}

func (k *KVStore) RegisterRPCHandlers(server *rpc.Server) error {
	server.RegisterName("KVS", k.requestHandlers)
	logger.Info("KVS request RPC handlers registered")
	return nil
}
