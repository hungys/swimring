package membership

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/rpc"
	"sort"
	"sync"
	"time"

	"github.com/dgryski/go-farm"
)

type memberlist struct {
	node  *Node
	local *Member

	members struct {
		list       []*Member
		byAddress  map[string]*Member
		rpcClients map[string]*rpc.Client
		checksum   uint32
		sync.RWMutex
	}

	sync.Mutex
}

type memberlistIter struct {
	m            *memberlist
	currentIndex int
	currentRound int
}

func newMemberlist(n *Node) *memberlist {
	m := &memberlist{
		node: n,
	}

	m.members.byAddress = make(map[string]*Member)
	m.members.rpcClients = make(map[string]*rpc.Client)

	return m
}

func newMemberlistIter(m *memberlist) *memberlistIter {
	iter := &memberlistIter{
		m:            m,
		currentIndex: -1,
		currentRound: 0,
	}

	iter.m.Shuffle()

	return iter
}

// Next returns the next pingable member in the member list, if it
// visits all members but none are pingable returns nil, false.
func (i *memberlistIter) Next() (*Member, bool) {
	numOfMembers := i.m.NumMembers()
	visited := make(map[string]bool)

	for len(visited) < numOfMembers {
		i.currentIndex++

		if i.currentIndex >= i.m.NumMembers() {
			i.currentIndex = 0
			i.currentRound++
			i.m.Shuffle()
		}

		member := i.m.MemberAt(i.currentIndex)
		visited[member.Address] = true

		if i.m.Pingable(*member) {
			return member, true
		}
	}

	return nil, false
}

// Checksum returns the checksum of the memberlist.
func (m *memberlist) Checksum() uint32 {
	m.members.Lock()
	checksum := m.members.checksum
	m.members.Unlock()

	return checksum
}

// ComputeChecksum computes the checksum of the memberlist.
func (m *memberlist) ComputeChecksum() {
	m.members.Lock()
	checksum := farm.Fingerprint32([]byte(m.genChecksumString()))
	m.members.checksum = checksum
	m.members.Unlock()
}

func (m *memberlist) genChecksumString() string {
	var strings sort.StringSlice

	for _, member := range m.members.list {
		s := fmt.Sprintf("%s,%s,%v", member.Address, member.Status, member.Incarnation)
		strings = append(strings, s)
	}

	strings.Sort()

	buffer := bytes.NewBuffer([]byte{})
	for _, str := range strings {
		buffer.WriteString(str)
		buffer.WriteString("|")
	}

	return buffer.String()
}

// Member returns the member at a specific address.
func (m *memberlist) Member(address string) (*Member, bool) {
	m.members.RLock()
	member, ok := m.members.byAddress[address]
	m.members.RUnlock()

	return member, ok
}

// MemberClient returns the RPC client of the member at a specific address,
// and it will dial to RPC server if client is not in rpcClients map.
func (m *memberlist) MemberClient(address string) (*rpc.Client, error) {
	m.members.Lock()
	client, ok := m.members.rpcClients[address]
	m.members.Unlock()

	if ok {
		return client, nil
	}

	logger.Debugf("Dialing to RPC server: %s", address)
	client, err := rpc.Dial("tcp", address)
	if err == nil {
		logger.Debugf("RPC connection established: %s", address)
		m.members.Lock()
		m.members.rpcClients[address] = client
		m.members.Unlock()
	} else {
		logger.Debugf("Cannot connect to RPC server: %s", address)
	}

	return client, err
}

// CloseMemberClient removes the client instance of the member at a specific address.
func (m *memberlist) CloseMemberClient(address string) {
	m.members.Lock()
	delete(m.members.rpcClients, address)
	m.members.Unlock()
}

// MemberAt returns the i-th member in the list.
func (m *memberlist) MemberAt(i int) *Member {
	m.members.RLock()
	member := m.members.list[i]
	m.members.RUnlock()

	return member
}

// NumMembers returns the number of members in the memberlist.
func (m *memberlist) NumMembers() int {
	m.members.RLock()
	n := len(m.members.list)
	m.members.RUnlock()

	return n
}

// NumPingableMembers returns the number of pingable members in the memberlist.
func (m *memberlist) NumPingableMembers() (n int) {
	m.members.Lock()
	for _, member := range m.members.list {
		if m.Pingable(*member) {
			n++
		}
	}
	m.members.Unlock()

	return
}

// Members returns all the members in the memberlist.
func (m *memberlist) Members() (members []Member) {
	m.members.RLock()
	for _, member := range m.members.list {
		members = append(members, *member)
	}
	m.members.RUnlock()

	return
}

// Pingable returns whether or not a member is pingable.
func (m *memberlist) Pingable(member Member) bool {
	return member.Address != m.local.Address && member.isReachable()
}

// RandomPingableMembers returns the number of pingable members in the memberlist.
func (m *memberlist) RandomPingableMembers(n int, excluding map[string]bool) []*Member {
	var members []*Member

	m.members.RLock()
	for _, member := range m.members.list {
		if m.Pingable(*member) && !excluding[member.Address] {
			members = append(members, member)
		}
	}
	m.members.RUnlock()

	members = shuffle(members)

	if n > len(members) {
		return members
	}
	return members[:n]
}

// Reincarnate sets the status of the node to Alive and updates the incarnation
// number. It adds the change to the disseminator as well.
func (m *memberlist) Reincarnate() []Change {
	return m.MarkAlive(m.node.Address(), time.Now().Unix())
}

// MarkAlive sets the status of the node at specific address to Alive and
// updates the incarnation number. It adds the change to the disseminator as well.
func (m *memberlist) MarkAlive(address string, incarnation int64) []Change {
	return m.MakeChange(address, incarnation, Alive)
}

// MarkSuspect sets the status of the node at specific address to Suspect and
// updates the incarnation number. It adds the change to the disseminator as well.
func (m *memberlist) MarkSuspect(address string, incarnation int64) []Change {
	return m.MakeChange(address, incarnation, Suspect)
}

// MarkFaulty sets the status of the node at specific address to Faulty and
// updates the incarnation number. It adds the change to the disseminator as well.
func (m *memberlist) MarkFaulty(address string, incarnation int64) []Change {
	return m.MakeChange(address, incarnation, Faulty)
}

// MakeChange makes a change to the memberlist.
func (m *memberlist) MakeChange(address string, incarnation int64, status string) []Change {
	if m.local == nil {
		m.local = &Member{
			Address:     m.node.Address(),
			Incarnation: 0,
			Status:      Alive,
		}
	}

	changes := m.Update([]Change{Change{
		Source:            m.local.Address,
		SourceIncarnation: m.local.Incarnation,
		Address:           address,
		Incarnation:       incarnation,
		Status:            status,
	}})

	return changes
}

// Update updates the memberlist with the slice of changes, applying selectively.
func (m *memberlist) Update(changes []Change) (applied []Change) {
	if m.node.Stopped() || len(changes) == 0 {
		return nil
	}

	m.Lock()
	m.members.Lock()

	for _, change := range changes {
		member, ok := m.members.byAddress[change.Address]

		if !ok {
			if m.applyChange(change) {
				applied = append(applied, change)
			}
			continue
		}

		if member.localOverride(m.node.Address(), change) {
			overrideChange := Change{
				Source:            change.Source,
				SourceIncarnation: change.SourceIncarnation,
				Address:           change.Address,
				Incarnation:       time.Now().Unix(),
				Status:            Alive,
			}

			if m.applyChange(overrideChange) {
				applied = append(applied, overrideChange)
			}

			continue
		}

		if member.nonLocalOverride(change) {
			if m.applyChange(change) {
				applied = append(applied, change)
			}
		}
	}

	m.members.Unlock()

	if len(applied) > 0 {
		m.ComputeChecksum()
		m.node.handleChanges(applied)
		m.node.swimring.HandleChanges(applied)
	}

	m.Unlock()
	return applied
}

// AddJoinList adds the list to the membership with the Update function.
// However, as a side effect, Update adds changes to the disseminator as well.
// Since we don't want to disseminate the potentially very large join lists,
// we clear all the changes from the disseminator, except for the one change
// that refers to the make-alive of this node.
func (m *memberlist) AddJoinList(list []Change) {
	applied := m.Update(list)
	for _, member := range applied {
		if member.Address == m.node.Address() {
			continue
		}
		m.node.disseminator.ClearChange(member.Address)
	}
}

func (m *memberlist) getJoinPosition() int {
	l := len(m.members.list)
	if l == 0 {
		return l
	}
	return rand.Intn(l)
}

func (m *memberlist) applyChange(change Change) bool {
	member, ok := m.members.byAddress[change.Address]

	if !ok {
		member = &Member{
			Address:     change.Address,
			Status:      change.Status,
			Incarnation: change.Incarnation,
		}

		if member.Address == m.node.Address() {
			m.local = member
		}

		m.members.byAddress[change.Address] = member
		i := m.getJoinPosition()
		m.members.list = append(m.members.list[:i], append([]*Member{member}, m.members.list[i:]...)...)

		logger.Noticef("Server %s added to memberlist", member.Address)
	}

	member.Lock()
	member.Status = change.Status
	member.Incarnation = change.Incarnation
	member.Unlock()

	logger.Noticef("%s is marked as %s node", member.Address, change.Status)

	return true
}

// Shuffle shuffles the memberlist.
func (m *memberlist) Shuffle() {
	m.members.Lock()
	m.members.list = shuffle(m.members.list)
	m.members.Unlock()
}
