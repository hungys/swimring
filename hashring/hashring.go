package hashring

import (
	"fmt"
	"sync"

	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("hashring")

// HashRing stores strings on a consistent hash ring. HashRing internally uses
// a Red-Black Tree to achieve O(log N) lookup and insertion time.
type HashRing struct {
	sync.RWMutex

	hashfunc      func(string) int
	replicaPoints int

	serverSet map[string]struct{}
	tree      *redBlackTree
}

// NewHashRing instantiates and returns a new HashRing.
func NewHashRing(hashfunc func([]byte) uint32, replicaPoints int) *HashRing {
	r := &HashRing{
		replicaPoints: replicaPoints,
		hashfunc: func(str string) int {
			return int(hashfunc([]byte(str)))
		},
	}

	r.serverSet = make(map[string]struct{})
	r.tree = &redBlackTree{}
	return r
}

// AddServer adds a server and its replicas onto the HashRing.
func (r *HashRing) AddServer(address string) bool {
	r.Lock()
	ok := r.addServerNoLock(address)
	r.Unlock()
	return ok
}

func (r *HashRing) addServerNoLock(address string) bool {
	if _, ok := r.serverSet[address]; ok {
		return false
	}

	r.addVirtualNodesNoLock(address)
	logger.Noticef("Server %s added to ring", address)
	return true
}

func (r *HashRing) addVirtualNodesNoLock(server string) {
	r.serverSet[server] = struct{}{}
	for i := 0; i < r.replicaPoints; i++ {
		address := fmt.Sprintf("%s%v", server, i)
		key := r.hashfunc(address)
		r.tree.Insert(key, server)
		logger.Debugf("Virtual node %d added for %s", key, server)
	}
}

// RemoveServer removes a server and its replicas from the HashRing.
func (r *HashRing) RemoveServer(address string) bool {
	r.Lock()
	ok := r.removeServerNoLock(address)
	r.Unlock()
	return ok
}

func (r *HashRing) removeServerNoLock(address string) bool {
	if _, ok := r.serverSet[address]; !ok {
		return false
	}

	r.removeVirtualNodesNoLock(address)
	logger.Noticef("Server %s removed from ring", address)
	return true
}

func (r *HashRing) removeVirtualNodesNoLock(server string) {
	delete(r.serverSet, server)
	for i := 0; i < r.replicaPoints; i++ {
		address := fmt.Sprintf("%s%v", server, i)
		key := r.hashfunc(address)
		r.tree.Delete(key)
		logger.Debugf("Virtual node %d removed for %s", key, server)
	}
}

// AddRemoveServers adds and removes servers and all replicas associated to those
// servers to and from the HashRing. Returns whether the HashRing has changed.
func (r *HashRing) AddRemoveServers(add []string, remove []string) bool {
	r.Lock()
	result := r.addRemoveServersNoLock(add, remove)
	r.Unlock()
	return result
}

func (r *HashRing) addRemoveServersNoLock(add []string, remove []string) bool {
	changed := false

	for _, server := range add {
		if r.addServerNoLock(server) {
			changed = true
		}
	}

	for _, server := range remove {
		if r.removeServerNoLock(server) {
			changed = true
		}
	}

	return changed
}

func (r *HashRing) copyServersNoLock() []string {
	var servers []string
	for server := range r.serverSet {
		servers = append(servers, server)
	}
	return servers
}

// Lookup returns the owner of the given key and whether the HashRing contains
// the key at all.
func (r *HashRing) Lookup(key string) (string, bool) {
	strs := r.LookupN(key, 1)
	if len(strs) == 0 {
		return "", false
	}

	logger.Debugf("Lookup(%s)=%s", key, strs[0])
	return strs[0], true
}

// LookupN returns the N servers that own the given key. Duplicates in the form
// of virtual nodes are skipped to maintain a list of unique servers. If there
// are less servers than N, we simply return all existing servers.
func (r *HashRing) LookupN(key string, n int) []string {
	r.RLock()
	servers := r.lookupNNoLock(key, n)
	r.RUnlock()

	logger.Debugf("LookupN(%s) = %v", key, servers)
	return servers
}

func (r *HashRing) lookupNNoLock(key string, n int) []string {
	if n >= len(r.serverSet) {
		return r.copyServersNoLock()
	}

	hash := r.hashfunc(key)
	unique := make(map[string]struct{})

	r.tree.LookupNUniqueAt(n, hash, unique)
	if len(unique) < n {
		r.tree.LookupNUniqueAt(n, 0, unique)
	}

	var servers []string
	for server := range unique {
		servers = append(servers, server)
	}
	return servers
}
