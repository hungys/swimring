package membership

import "sync"

const defaultPFactor int = 15

type pChange struct {
	Change
	p int
}

type disseminator struct {
	node    *Node
	changes map[string]*pChange

	maxP    int
	pFactor int

	sync.RWMutex
}

func newDisseminator(n *Node) *disseminator {
	d := &disseminator{
		node:    n,
		changes: make(map[string]*pChange),
		maxP:    defaultPFactor,
		pFactor: defaultPFactor,
	}

	return d
}

func (d *disseminator) MembershipAsChanges() (changes []Change) {
	d.Lock()

	for _, member := range d.node.memberlist.Members() {
		changes = append(changes, Change{
			Address:           member.Address,
			Incarnation:       member.Incarnation,
			Source:            d.node.Address(),
			SourceIncarnation: d.node.Incarnation(),
			Status:            member.Status,
		})
	}

	d.Unlock()

	return changes
}

func (d *disseminator) IssueAsSender() (changes []Change, bumpPiggybackCounters func()) {
	changes = d.issueChanges()
	return changes, func() {
		d.bumpPiggybackCounters(changes)
	}
}

func (d *disseminator) IssueAsReceiver(senderAddress string, senderIncarnation int64, senderChecksum uint32) (changes []Change) {
	changes = d.filterChangesFromSender(d.issueChanges(), senderAddress, senderIncarnation)

	d.bumpPiggybackCounters(changes)

	if len(changes) > 0 || d.node.memberlist.Checksum() == senderChecksum {
		return changes
	}

	return d.MembershipAsChanges()
}

func (d *disseminator) filterChangesFromSender(cs []Change, source string, incarnation int64) []Change {
	for i := 0; i < len(cs); i++ {
		if incarnation == cs[i].SourceIncarnation && source == cs[i].Source {
			cs[i], cs[len(cs)-1] = cs[len(cs)-1], cs[i]
			cs = cs[:len(cs)-1]
			i--
		}
	}
	return cs
}

func (d *disseminator) bumpPiggybackCounters(changes []Change) {
	d.Lock()
	for _, change := range changes {
		c, ok := d.changes[change.Address]
		if !ok {
			continue
		}

		c.p++
		if c.p >= d.maxP {
			delete(d.changes, c.Address)
		}
	}
	d.Unlock()
}

func (d *disseminator) issueChanges() []Change {
	d.Lock()

	result := []Change{}
	for _, change := range d.changes {
		result = append(result, change.Change)
	}

	d.Unlock()

	return result
}

func (d *disseminator) RecordChange(change Change) {
	d.Lock()
	d.changes[change.Address] = &pChange{change, 0}
	d.Unlock()
}

func (d *disseminator) ClearChange(address string) {
	d.Lock()
	delete(d.changes, address)
	d.Unlock()
}

func (d *disseminator) ClearChanges() {
	d.Lock()
	d.changes = make(map[string]*pChange)
	d.Unlock()
}
