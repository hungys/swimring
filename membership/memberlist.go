package membership

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/rpc"
	"sort"
	"sync"

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

func (m *memberlist) Checksum() uint32 {
	m.members.Lock()
	checksum := m.members.checksum
	m.members.Unlock()

	return checksum
}

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

func (m *memberlist) Member(address string) (*Member, bool) {
	m.members.RLock()
	member, ok := m.members.byAddress[address]
	m.members.RUnlock()

	return member, ok
}

func (m *memberlist) MemberClient(address string) (*rpc.Client, error) {
	m.members.Lock()
	defer m.members.Unlock()

	client, ok := m.members.rpcClients[address]
	if ok {
		return client, nil
	}

	client, err := rpc.Dial("tcp", address)
	if err == nil {
		m.members.rpcClients[address] = client
	}

	return client, err
}

func (m *memberlist) CloseMemberClient(address string) {
	m.members.Lock()
	delete(m.members.rpcClients, address)
	m.members.Unlock()
}

func (m *memberlist) MemberAt(i int) *Member {
	m.members.RLock()
	member := m.members.list[i]
	m.members.RUnlock()

	return member
}

func (m *memberlist) NumMembers() int {
	m.members.RLock()
	n := len(m.members.list)
	m.members.RUnlock()

	return n
}

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

func (m *memberlist) Members() (members []Member) {
	m.members.RLock()
	for _, member := range m.members.list {
		members = append(members, *member)
	}
	m.members.RUnlock()

	return
}

func (m *memberlist) Pingable(member Member) bool {
	return member.Address != m.local.Address && member.isReachable()
}

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

func (m *memberlist) Reincarnate() []Change {
	return m.MarkAlive(m.node.Address(), 0)
}

func (m *memberlist) MarkAlive(address string, incarnation int64) []Change {
	return m.MakeChange(address, incarnation, Alive)
}

func (m *memberlist) MarkSuspect(address string, incarnation int64) []Change {
	return m.MakeChange(address, incarnation, Suspect)
}

func (m *memberlist) MarkFaulty(address string, incarnation int64) []Change {
	return m.MakeChange(address, incarnation, Faulty)
}

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
				Incarnation:       m.node.Incarnation() + 1,
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
	}

	member.Lock()
	member.Status = change.Status
	member.Incarnation = change.Incarnation
	member.Unlock()

	return true
}

func (m *memberlist) Shuffle() {
	m.members.Lock()
	m.members.list = shuffle(m.members.list)
	m.members.Unlock()
}
