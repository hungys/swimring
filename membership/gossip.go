package membership

import (
	"sync"
	"time"
)

type gossip struct {
	node *Node

	status struct {
		stopped bool
		sync.RWMutex
	}

	minProtocolPeriod time.Duration

	protocol struct {
		numPeriods int
		lastPeriod time.Time
		lastRate   time.Duration
		sync.RWMutex
	}
}

func newGossip(node *Node, minProtocolPeriod time.Duration) *gossip {
	gossip := &gossip{
		node:              node,
		minProtocolPeriod: minProtocolPeriod,
	}

	gossip.SetStopped(true)

	return gossip
}

// Stopped returns whether or not the gossip sub-protocol is stopped.
func (g *gossip) Stopped() bool {
	g.status.RLock()
	stopped := g.status.stopped
	g.status.RUnlock()

	return stopped
}

// SetStopped sets the gossip sub-protocol to stopped or not stopped.
func (g *gossip) SetStopped(stopped bool) {
	g.status.Lock()
	g.status.stopped = stopped
	g.status.Unlock()
}

// Start start the gossip protocol.
func (g *gossip) Start() {
	if !g.Stopped() {
		return
	}

	g.SetStopped(false)
	g.RunProtocolPeriodLoop()

	logger.Notice("Gossip protocol started")
}

// Stop start the gossip protocol.
func (g *gossip) Stop() {
	if g.Stopped() {
		return
	}

	g.SetStopped(true)

	logger.Notice("Gossip protocol stopped")
}

// ProtocolPeriod run a gossip protocol period.
func (g *gossip) ProtocolPeriod() {
	g.node.pingNextMember()
}

// RunProtocolPeriodLoop run the gossip protocol period loop.
func (g *gossip) RunProtocolPeriodLoop() {
	go func() {
		for !g.Stopped() {
			delay := g.minProtocolPeriod
			g.ProtocolPeriod()
			time.Sleep(delay)
		}
	}()
}
