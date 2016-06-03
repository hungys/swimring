package main

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"

	"github.com/dgryski/go-farm"
	"github.com/hungys/swimring/hashring"
	"github.com/hungys/swimring/membership"
	"github.com/hungys/swimring/storage"
)

type configuration struct {
	Host                       string
	InternalPort, ExternalPort string

	BootstrapNodes []string
}

type SwimRing struct {
	config *configuration

	status      status
	statusMutex sync.RWMutex

	node *membership.Node
	ring *hashring.HashRing
	kvs  *storage.KVStore
}

type status uint

const (
	created status = iota
	initialized
	ready
	destroyed
)

func NewSwimRing(config *configuration) *SwimRing {
	sr := &SwimRing{
		config: config,
	}
	sr.setStatus(created)

	return sr
}

func (sr *SwimRing) init() error {
	address := fmt.Sprintf("%s:%s", sr.config.Host, sr.config.InternalPort)

	sr.node = membership.NewNode(sr, address, &membership.Options{
		BootstrapNodes: sr.config.BootstrapNodes,
	})

	sr.ring = hashring.NewHashRing(farm.Fingerprint32, 3)
	sr.kvs = storage.NewKVStore()

	sr.setStatus(initialized)

	return nil
}

func (sr *SwimRing) Status() status {
	sr.statusMutex.RLock()
	r := sr.status
	sr.statusMutex.RUnlock()
	return r
}

func (sr *SwimRing) setStatus(s status) {
	sr.statusMutex.Lock()
	sr.status = s
	sr.statusMutex.Unlock()
}

func (sr *SwimRing) Bootstrap() ([]string, error) {
	if sr.Status() < initialized {
		err := sr.init()
		if err != nil {
			return nil, err
		}
	}

	joined, err := sr.node.Bootstrap()
	if err != nil {
		sr.setStatus(initialized)
	}

	sr.registerRPCHandlers(false)
	sr.setStatus(ready)

	return joined, nil
}

func (sr *SwimRing) HandleChanges(changes []membership.Change) {
	var serversToAdd, serversToRemove []string

	for _, change := range changes {
		switch change.Status {
		case membership.Alive, membership.Suspect:
			serversToAdd = append(serversToAdd, change.Address)
		case membership.Faulty:
			serversToRemove = append(serversToRemove, change.Address)
		}
	}

	sr.ring.AddRemoveServers(serversToAdd, serversToRemove)
}

func (sr *SwimRing) registerRPCHandlers(handleHTTP bool) error {
	addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:"+sr.config.InternalPort)
	if err != nil {
		return err
	}

	conn, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}

	sr.node.RegisterRPCHandlers()
	sr.kvs.RegisterRPCHandlers()

	if handleHTTP {
		rpc.HandleHTTP()
	}
	rpc.Accept(conn)

	return nil
}
