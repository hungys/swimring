package membership

import "time"

type ProtocolHandlers struct {
	node *Node
}

type Ping struct {
	Changes           []Change
	Checksum          uint32
	Source            string
	SourceIncarnation int64
}

type PingRequest struct {
	Source            string
	SourceIncarnation int64
	Target            string
	Checksum          uint32
	Changes           []Change
}

type PingResponse struct {
	Ok      bool
	Target  string
	Changes []Change
}

type JoinRequest struct {
	Source      string
	Incarnation int64
	Timeout     time.Duration
}

type JoinResponse struct {
	Coordinator string
	Membership  []Change
	Checksum    uint32
}

func NewProtocolHandler(n *Node) *ProtocolHandlers {
	p := &ProtocolHandlers{
		node: n,
	}

	return p
}

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

func (p *ProtocolHandlers) PingRequest(req *PingRequest, resp *PingResponse) error {
	if !p.node.Ready() {
		return ErrNodeNotReady
	}

	p.node.memberlist.Update(req.Changes)

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

func (p *ProtocolHandlers) Join(req *JoinRequest, resp *JoinResponse) error {
	resp.Coordinator = p.node.Address()
	resp.Membership = p.node.disseminator.MembershipAsChanges()
	resp.Checksum = p.node.memberlist.Checksum()

	return nil
}
