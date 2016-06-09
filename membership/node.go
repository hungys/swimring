package membership

import (
	"errors"
	"net/rpc"
	"sync"
	"time"

	"github.com/hungys/swimring/util"
	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("membership")

var (
	// ErrNodeNotReady is returned when a remote request is being handled while the node is not yet ready
	ErrNodeNotReady = errors.New("node is not ready to handle requests")
)

type changeHandler interface {
	HandleChanges(changes []Change)
}

// Options is a configuration struct passed into NewNode constructor.
type Options struct {
	JoinTimeout, SuspectTimeout, PingTimeout, PingRequestTimeout, MinProtocolPeriod time.Duration

	PingRequestSize int
	BootstrapNodes  []string
}

func defaultOptions() *Options {
	opts := &Options{
		JoinTimeout:        1000 * time.Millisecond,
		SuspectTimeout:     5 * time.Second,
		PingTimeout:        1500 * time.Millisecond,
		PingRequestTimeout: 5000 * time.Millisecond,
		MinProtocolPeriod:  200 * time.Millisecond,
		PingRequestSize:    3,
	}

	return opts
}

func mergeDefaultOptions(opts *Options) *Options {
	def := defaultOptions()

	if opts == nil {
		return def
	}

	opts.JoinTimeout = util.SelectDurationOpt(opts.JoinTimeout, def.JoinTimeout)
	opts.SuspectTimeout = util.SelectDurationOpt(opts.SuspectTimeout, def.SuspectTimeout)
	opts.PingTimeout = util.SelectDurationOpt(opts.PingTimeout, def.PingTimeout)
	opts.PingRequestTimeout = util.SelectDurationOpt(opts.PingRequestTimeout, def.PingRequestTimeout)
	opts.MinProtocolPeriod = util.SelectDurationOpt(opts.MinProtocolPeriod, def.MinProtocolPeriod)
	opts.PingRequestSize = util.SelectIntOpt(opts.PingRequestSize, def.PingRequestSize)

	return opts
}

// Node is a SWIM member.
type Node struct {
	address string

	status struct {
		stopped, destroyed, pinging, ready bool
		sync.RWMutex
	}

	swimring         changeHandler
	memberlist       *memberlist
	memberiter       *memberlistIter
	disseminator     *disseminator
	stateTransitions *stateTransitions
	gossip           *gossip
	protocolHandlers *ProtocolHandlers

	joinTimeout, suspectTimeout, pingTimeout, pingRequestTimeout time.Duration

	pingRequestSize int
	bootstrapNodes  []string
}

// NewNode returns a new SWIM node.
func NewNode(swimring changeHandler, address string, opts *Options) *Node {
	opts = mergeDefaultOptions(opts)

	node := &Node{
		address: address,
	}

	node.swimring = swimring
	node.memberlist = newMemberlist(node)
	node.memberiter = newMemberlistIter(node.memberlist)
	node.disseminator = newDisseminator(node)
	node.stateTransitions = newStateTransitions(node)
	node.gossip = newGossip(node, opts.MinProtocolPeriod)
	node.protocolHandlers = NewProtocolHandler(node)

	node.joinTimeout = opts.JoinTimeout
	node.suspectTimeout = opts.SuspectTimeout
	node.pingTimeout = opts.PingTimeout
	node.pingRequestTimeout = opts.PingRequestTimeout
	node.pingRequestSize = opts.PingRequestSize
	node.bootstrapNodes = opts.BootstrapNodes

	return node
}

// Address returns the address of the SWIM node.
func (n *Node) Address() string {
	return n.address
}

// Members returns all the members in Node's memberlist.
func (n *Node) Members() []Member {
	return n.memberlist.Members()
}

// MemberClient returns the RPC client of the member at a specific address,
// and it will dial to RPC server if client is not in rpcClients map.
func (n *Node) MemberClient(address string) (*rpc.Client, error) {
	return n.memberlist.MemberClient(address)
}

// MemberReachable returns whether or not the member is reachable
func (n *Node) MemberReachable(address string) bool {
	member, ok := n.memberlist.Member(address)
	if !ok {
		return false
	}

	return member.isReachable()
}

// Start starts the SWIM protocol and all sub-protocols.
func (n *Node) Start() {
	n.gossip.Start()
	n.stateTransitions.Enable()

	n.status.Lock()
	n.status.stopped = false
	n.status.Unlock()

	logger.Noticef("Local node %s started", n.Address())
}

// Stop stops the SWIM protocol and all sub-protocols.
func (n *Node) Stop() {
	n.gossip.Stop()
	n.stateTransitions.Disable()

	n.status.Lock()
	n.status.stopped = true
	n.status.Unlock()

	logger.Noticef("Local node %s stopped", n.Address())
}

// Stopped returns whether or not the SWIM protocol is currently stopped.
func (n *Node) Stopped() bool {
	n.status.RLock()
	stopped := n.status.stopped
	n.status.RUnlock()

	return stopped
}

// Destroy stops the SWIM protocol and all sub-protocols.
func (n *Node) Destroy() {
	n.status.Lock()
	if n.status.destroyed {
		n.status.Unlock()
		return
	}
	n.status.destroyed = true
	n.status.Unlock()

	n.Stop()

	logger.Noticef("Local node %s destroyed", n.Address())
}

// Destroyed returns whether or not the node has been destroyed.
func (n *Node) Destroyed() bool {
	n.status.RLock()
	destroyed := n.status.destroyed
	n.status.RUnlock()

	return destroyed
}

// Ready returns whether or not the node has bootstrapped and is ready for use.
func (n *Node) Ready() bool {
	n.status.RLock()
	ready := n.status.ready
	n.status.RUnlock()

	return ready
}

// Incarnation returns the incarnation number of the Node.
func (n *Node) Incarnation() int64 {
	if n.memberlist != nil && n.memberlist.local != nil {
		n.memberlist.local.RLock()
		incarnation := n.memberlist.local.Incarnation
		n.memberlist.local.RUnlock()
		return incarnation
	}
	return -1
}

// Bootstrap joins the Node to a cluster.
func (n *Node) Bootstrap() ([]string, error) {
	logger.Notice("Bootstrapping local node...")

	n.memberlist.Reincarnate()
	nodesJoined := n.joinCluster()
	n.gossip.Start()

	n.status.Lock()
	n.status.ready = true
	n.status.Unlock()

	return nodesJoined, nil
}

// RegisterRPCHandlers registers the RPC handlers for internal SWIM protocol.
func (n *Node) RegisterRPCHandlers(server *rpc.Server) error {
	server.RegisterName("Protocol", n.protocolHandlers)
	logger.Info("SWIM protocol RPC handlers registered")
	return nil
}

func (n *Node) handleChanges(changes []Change) {
	for _, change := range changes {
		n.disseminator.RecordChange(change)

		switch change.Status {
		case Alive:
			n.stateTransitions.Cancel(change)
		case Suspect:
			n.stateTransitions.ScheduleSuspectToFaulty(change)
		}
	}
}

func (n *Node) pinging() bool {
	n.status.RLock()
	pinging := n.status.pinging
	n.status.RUnlock()

	return pinging
}

func (n *Node) setPinging(pinging bool) {
	n.status.Lock()
	n.status.pinging = pinging
	n.status.Unlock()
}

func (n *Node) pingNextMember() {
	if n.pinging() {
		return
	}

	member, ok := n.memberiter.Next()
	if !ok {
		return
	}

	n.setPinging(true)
	defer n.setPinging(false)

	res, err := sendDirectPing(n, member.Address, n.pingTimeout)
	if err == nil {
		n.memberlist.Update(res.Changes)
		return
	}

	n.memberlist.CloseMemberClient(member.Address)
	targetReached, _ := sendIndirectPing(n, member.Address, n.pingRequestSize, n.pingRequestTimeout)

	if !targetReached {
		if member.Status != Suspect {
			logger.Errorf("Cannot reach %s, mark it suspect", member.Address)
		}
		n.memberlist.MarkSuspect(member.Address, member.Incarnation)
		return
	}
}

func (n *Node) joinCluster() []string {
	var nodesJoined []string
	var wg sync.WaitGroup

	logger.Infof("Trying to join the cluster...")
	for _, target := range n.bootstrapNodes {
		wg.Add(1)

		go func(target string) {
			defer wg.Done()
			res, err := sendJoin(n, target, n.joinTimeout)

			if err != nil {
				return
			}

			logger.Noticef("Join %s successfully, %d peers found", target, len(res.Membership))
			n.memberlist.AddJoinList(res.Membership)
			nodesJoined = append(nodesJoined, target)
		}(target)
	}

	wg.Wait()

	return nodesJoined
}
