package membership

import (
	"errors"
	"sync"
	"time"
)

func sendDirectPing(node *Node, target string, timeout time.Duration) (*Ping, error) {
	changes, bumpPiggybackCounters := node.disseminator.IssueAsSender()

	res, err := sendPingWithChanges(node, target, changes, timeout)
	if err != nil {
		return res, err
	}

	bumpPiggybackCounters()

	return res, err
}

func sendPingWithChanges(node *Node, target string, changes []Change, timeout time.Duration) (*Ping, error) {
	req := &Ping{
		Checksum:          node.memberlist.Checksum(),
		Changes:           changes,
		Source:            node.Address(),
		SourceIncarnation: node.Incarnation(),
	}

	errCh := make(chan error, 1)
	resp := &Ping{}
	go func() {
		client, err := node.memberlist.MemberClient(target)
		if err != nil {
			errCh <- err
			return
		}

		if client != nil {
			errCh <- client.Call("Protocol.Ping", req, resp)
		}
	}()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(timeout):
		logger.Warningf("Ping to %s timeout", target)
		err = errors.New("ping timeout")
	}

	if err != nil {
		return nil, err
	}

	return resp, err
}

func sendIndirectPing(node *Node, target string, amount int, timeout time.Duration) (reached bool, errs []error) {
	resCh := sendPingRequests(node, target, amount, timeout)

	for result := range resCh {
		switch res := result.(type) {
		case *PingResponse:
			if res.Ok {
				return true, errs
			}
		case error:
			errs = append(errs, res)
		}
	}

	return false, errs
}

func sendPingRequests(node *Node, target string, amount int, timeout time.Duration) <-chan interface{} {
	peers := node.memberlist.RandomPingableMembers(amount, map[string]bool{target: true})

	var wg sync.WaitGroup
	resCh := make(chan interface{}, amount)

	for _, peer := range peers {
		wg.Add(1)

		go func(peer Member) {
			defer wg.Done()

			res, err := sendPingRequest(node, peer.Address, target, timeout)
			if err != nil {
				resCh <- err
				return
			}

			resCh <- res
		}(*peer)
	}

	go func() {
		wg.Wait()
		close(resCh)
	}()

	return resCh
}

func sendPingRequest(node *Node, peer string, target string, timeout time.Duration) (*PingResponse, error) {
	changes, bumpPiggybackCounters := node.disseminator.IssueAsSender()
	req := &PingRequest{
		Source:            node.Address(),
		SourceIncarnation: node.Incarnation(),
		Checksum:          node.memberlist.Checksum(),
		Changes:           changes,
		Target:            target,
	}

	errCh := make(chan error, 1)
	resp := &PingResponse{}
	go func() {
		client, err := node.memberlist.MemberClient(peer)
		if err != nil {
			errCh <- err
			return
		}

		if client != nil {
			err = client.Call("Protocol.PingRequest", req, resp)
			if err != nil {
				errCh <- err
				return
			}
		}

		bumpPiggybackCounters()
		errCh <- nil
	}()

	var err error
	select {
	case err = <-errCh:
		if err == nil {
			node.memberlist.Update(resp.Changes)
		}
		return resp, err
	case <-time.After(timeout):
		logger.Warningf("Ping request to %s timeout", target)
		return nil, errors.New("ping request timeout")
	}
}
