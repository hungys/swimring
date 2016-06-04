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

func (g *gossip) Stopped() bool {
	g.status.RLock()
	stopped := g.status.stopped
	g.status.RUnlock()

	return stopped
}

func (g *gossip) SetStopped(stopped bool) {
	g.status.Lock()
	g.status.stopped = stopped
	g.status.Unlock()
}

func (g *gossip) Start() {
	if !g.Stopped() {
		return
	}

	g.SetStopped(false)
	g.RunProtocolPeriodLoop()

	logger.Notice("Gossip protocol started")
}

func (g *gossip) Stop() {
	if g.Stopped() {
		return
	}

	g.SetStopped(true)

	logger.Notice("Gossip protocol stopped")
}

func (g *gossip) ProtocolPeriod() {
	g.node.pingNextMember()
}

func (g *gossip) RunProtocolPeriodLoop() {
	go func() {
		for !g.Stopped() {
			delay := g.minProtocolPeriod
			g.ProtocolPeriod()
			time.Sleep(delay)
		}
	}()
}
