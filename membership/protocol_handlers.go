package membership

import "time"

// ProtocolHandlers defines a set of RPC handlers for SWIM gossip protocol.
type ProtocolHandlers struct {
	node *Node
}

// Ping is the payload of ping and ping response.
type Ping struct {
	Changes           []Change
	Checksum          uint32
	Source            string
	SourceIncarnation int64
}

// PingRequest is the payload of ping request.
type PingRequest struct {
	Source            string
	SourceIncarnation int64
	Target            string
	Checksum          uint32
	Changes           []Change
}

// PingResponse is the payload of the response of ping request.
type PingResponse struct {
	Ok      bool
	Target  string
	Changes []Change
}

// JoinRequest is the payload of join request.
type JoinRequest struct {
	Source      string
	Incarnation int64
	Timeout     time.Duration
}

// JoinResponse is the payload of the response of join request.
type JoinResponse struct {
	Coordinator string
	Membership  []Change
	Checksum    uint32
}

// NewProtocolHandler returns a new ProtocolHandlers.
func NewProtocolHandler(n *Node) *ProtocolHandlers {
	p := &ProtocolHandlers{
		node: n,
	}

	return p
}

// Ping handles the incoming Ping.
func (p *ProtocolHandlers) Ping(req *Ping, resp *Ping) error {
	if !p.node.Ready() {
		return ErrNodeNotReady
	}

	p.node.memberlist.Update(req.Changes)

	changes := p.node.disseminator.IssueAsReceiver(req.Source, req.SourceIncarnation, req.Checksum)

	resp.Checksum = p.node.memberlist.Checksum()
	resp.Changes = changes
	resp.Source = p.node.Address()
	resp.SourceIncarnation = p.node.Incarnation()

	return nil
}

// PingRequest handles the incoming PingRequest. It helps the source node to send
// Ping to the target.
func (p *ProtocolHandlers) PingRequest(req *PingRequest, resp *PingResponse) error {
	if !p.node.Ready() {
		return ErrNodeNotReady
	}

	p.node.memberlist.Update(req.Changes)

	logger.Infof("Handling ping request to %s (from %s)", req.Target, req.Source)

	res, err := sendDirectPing(p.node, req.Target, p.node.pingTimeout)
	pingOk := err == nil

	if pingOk {
		p.node.memberlist.Update(res.Changes)
	}

	changes := p.node.disseminator.IssueAsReceiver(req.Source, req.SourceIncarnation, req.Checksum)

	resp.Target = req.Target
	resp.Ok = pingOk
	resp.Changes = changes

	return nil
}

// Join handles the incoming Join request.
func (p *ProtocolHandlers) Join(req *JoinRequest, resp *JoinResponse) error {
	logger.Infof("Handling join request from %s", req.Source)

	resp.Coordinator = p.node.Address()
	resp.Membership = p.node.disseminator.MembershipAsChanges()
	resp.Checksum = p.node.memberlist.Checksum()

	return nil
}
