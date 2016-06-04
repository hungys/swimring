package storage

import (
	"errors"
	"net/rpc"
	"sync"
	"time"

	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("storage")

type KVStore struct {
	sync.RWMutex
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
	k.RLock()
	value, ok := k.db[key]
	k.RUnlock()

	if !ok {
		return nil, errors.New("key not found")
	}

	return value, nil
}

func (k *KVStore) Put(key, value string) error {
	k.Lock()
	if _, ok := k.db[key]; !ok {
		k.db[key] = &KVEntry{}
	}
	k.db[key].Value = value
	k.db[key].Timestamp = int(time.Now().Unix())
	k.Unlock()

	logger.Infof("Key-value pair (%s, %s) updated", key, value)

	return nil
}

func (k *KVStore) Delete(key string) error {
	k.Lock()
	_, ok := k.db[key]
	if !ok {
		k.Unlock()
		return errors.New("key not found")
	}

	delete(k.db, key)
	k.Unlock()

	logger.Infof("Key %s deleted", key)

	return nil
}

func (k *KVStore) RegisterRPCHandlers(server *rpc.Server) error {
	server.RegisterName("KVS", k.requestHandlers)
	logger.Info("Internal KVS request RPC handlers registered")
	return nil
}
