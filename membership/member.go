package membership

import (
	"math/rand"
	"sync"
)

const (
	Alive   = "alive"
	Suspect = "suspect"
	Faulty  = "faulty"
)

type Member struct {
	sync.RWMutex
	Address     string
	Status      string
	Incarnation int64
}

func shuffle(members []*Member) []*Member {
	newMembers := make([]*Member, len(members), cap(members))
	newIndexes := rand.Perm(len(members))

	for o, n := range newIndexes {
		newMembers[n] = members[o]
	}

	return newMembers
}

func (m *Member) nonLocalOverride(change Change) bool {
	if change.Incarnation > m.Incarnation {
		return true
	}

	if change.Incarnation < m.Incarnation {
		return false
	}

	return statePrecedence(change.Status) > statePrecedence(m.Status)
}

func (m *Member) localOverride(local string, change Change) bool {
	if m.Address != local {
		return false
	}
	return change.Status == Faulty || change.Status == Suspect
}

func statePrecedence(s string) int {
	switch s {
	case Alive:
		return 0
	case Suspect:
		return 1
	case Faulty:
		return 2
	default:
		return -1
	}
}

func (m *Member) isReachable() bool {
	return m.Status == Alive || m.Status == Suspect
}

type Change struct {
	Source            string
	SourceIncarnation int64
	Address           string
	Incarnation       int64
	Status            string
}
