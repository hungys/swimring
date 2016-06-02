package membership

import (
	"sync"
	"time"
)

type transitionTimer struct {
	*time.Timer
	state string
}

type stateTransitions struct {
	sync.Mutex

	node    *Node
	timers  map[string]*transitionTimer
	enabled bool
}

func newStateTransitions(n *Node) *stateTransitions {
	return &stateTransitions{
		node:    n,
		timers:  make(map[string]*transitionTimer),
		enabled: true,
	}
}

func (s *stateTransitions) ScheduleSuspectToFaulty(change Change) {
	s.Lock()
	s.schedule(change, Suspect, s.node.suspectTimeout, func() {
		s.node.memberlist.MarkFaulty(change.Address, change.Incarnation)
	})
	s.Unlock()
}

func (s *stateTransitions) schedule(change Change, state string, timeout time.Duration, transition func()) {
	if !s.enabled {
		return
	}

	if s.node.Address() == change.Address {
		return
	}

	if timer, ok := s.timers[change.Address]; ok {
		if timer.state == state {
			return
		}
		timer.Stop()
	}

	timer := time.AfterFunc(timeout, func() {
		transition()
	})

	s.timers[change.Address] = &transitionTimer{
		Timer: timer,
		state: state,
	}
}

func (s *stateTransitions) Cancel(change Change) {
	s.Lock()

	if timer, ok := s.timers[change.Address]; ok {
		timer.Stop()
		delete(s.timers, change.Address)
	}

	s.Unlock()
}

func (s *stateTransitions) Enable() {
	s.Lock()
	s.enabled = true
	s.Unlock()
}

func (s *stateTransitions) Disable() {
	s.Lock()
	s.enabled = false
	s.Unlock()
}
