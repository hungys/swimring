package storage

import (
	"errors"
	"net/rpc"
	"time"

	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("storage")

type KVStore struct {
	address string
	db      map[string]*KVEntry

	requestHandlers *RequestHandlers
}

type KVEntry struct {
	Value     string
	Timestamp int
}

func NewKVStore(address string) *KVStore {
	kvs := &KVStore{
		address: address,
	}
	kvs.db = make(map[string]*KVEntry)

	requestHandlers := NewRequestHandler(kvs)
	kvs.requestHandlers = requestHandlers

	return kvs
}

func (k *KVStore) Get(key string) (*KVEntry, error) {
	value, ok := k.db[key]
	if !ok {
		return nil, errors.New("key not found")
	}

	return value, nil
}

func (k *KVStore) Put(key, value string) error {
	if _, ok := k.db[key]; !ok {
		k.db[key] = &KVEntry{}
	}

	k.db[key].Value = value
	k.db[key].Timestamp = int(time.Now().Unix())

	logger.Infof("Key-value pair (%s, %s) updated", key, value)

	return nil
}

func (k *KVStore) Delete(key string) error {
	_, ok := k.db[key]
	if !ok {
		return errors.New("key not found")
	}

	delete(k.db, key)

	logger.Infof("Key %s deleted", key)

	return nil
}

func (k *KVStore) RegisterRPCHandlers(server *rpc.Server) error {
	server.RegisterName("KVS", k.requestHandlers)
	logger.Info("Internal KVS request RPC handlers registered")
	return nil
}
