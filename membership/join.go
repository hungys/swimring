package membership

import (
	"errors"
	"time"
)

func sendJoin(node *Node, target string, timeout time.Duration) (*JoinResponse, error) {
	if target == node.Address() {
		logger.Error("Cannot join local node")
		return nil, errors.New("cannot join local node")
	}

	req := &JoinRequest{
		Source:      node.address,
		Incarnation: node.Incarnation(),
		Timeout:     timeout,
	}
	resp := &JoinResponse{}

	errCh := make(chan error, 1)
	go func() {
		client, err := node.memberlist.MemberClient(target)
		if err != nil {
			errCh <- err
			return
		}

		errCh <- client.Call("Protocol.Join", req, resp)
	}()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(timeout):
		logger.Error("Join request timeout")
		err = errors.New("join timeout")
	}

	if err != nil {
		return nil, err
	}

	return resp, err
}
