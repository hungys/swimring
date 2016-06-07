package main

import (
	"errors"
	"math"
	"net/rpc"
	"sync"
	"time"

	"github.com/hungys/swimring/membership"
	"github.com/hungys/swimring/storage"
)

const (
	// ONE is the weakest consistency level.
	// For read request, returns value when the first response arrived.
	// For write request, returns when the first ACK received.
	ONE = "ONE"
	// QUORUM is the moderate consistency level.
	// For read request, returns value when the quorum set of replicas all responded.
	// For write request, returns when the quorum set of replicas all responded ACKs.
	QUORUM = "QUORUM"
	// ALL is the strongest consistency level.
	// For read request, returns value when all replicas responded.
	// For write request, returns when all replicas all responded ACKs.
	ALL = "ALL"
	// GetOp is the name of the service method for Get.
	GetOp = "KVS.Get"
	// PutOp is the name of the service method for Put.
	PutOp = "KVS.Put"
	// DeleteOp is the name of the service method for Delete.
	DeleteOp = "KVS.Delete"
	// StatOp is the name of the service method for Stat.
	StatOp = "KVS.Stat"
)

// RequestCoordinator is the coordinator for all the incoming external request.
type RequestCoordinator struct {
	sr *SwimRing
}

// GetRequest is the payload of Get.
type GetRequest struct {
	Level string
	Key   string
}

// GetResponse is the payload of the response of Get.
type GetResponse struct {
	Key, Value string
}

// PutRequest is the payload of Put.
type PutRequest struct {
	Level      string
	Key, Value string
}

// PutResponse is the payload of the response of Put.
type PutResponse struct{}

// DeleteRequest is the payload of Delete.
type DeleteRequest struct {
	Level string
	Key   string
}

// DeleteResponse is the payload of the response of Delete.
type DeleteResponse struct{}

// StateRequest is the payload of Stat.
type StateRequest struct{}

// StateResponse is the payload of the response of Stat.
type StateResponse struct {
	Nodes []NodeStat
}

// NodeStat stores the information of a Node
type NodeStat struct {
	Address  string
	Status   string
	KeyCount int
}

// NewRequestCoordinator returns a new RequestCoordinator.
func NewRequestCoordinator(sr *SwimRing) *RequestCoordinator {
	rc := &RequestCoordinator{
		sr: sr,
	}

	return rc
}

// Get handles the incoming Get request. It first looks up for the owner replicas
// of the given key, forwards request to all replicas and deals with them according to
// consistency level. Read repair is initiated if necessary.
func (rc *RequestCoordinator) Get(req *GetRequest, resp *GetResponse) error {
	logger.Debugf("Coordinating external request Get(%s, %s)", req.Key, req.Level)

	internalReq := &storage.GetRequest{
		Key: req.Key,
	}

	replicas := rc.sr.ring.LookupN(req.Key, rc.sr.config.KVSReplicaPoints)
	resCh := rc.sendRPCRequests(replicas, GetOp, internalReq)
	resp.Key = req.Key

	ackNeed := rc.numOfRequiredACK(req.Level)
	ackReceived := 0
	ackOk := 0
	latestTimestamp := int64(0)
	latestValue := ""

	var resList []*storage.GetResponse

	for result := range resCh {
		switch res := result.(type) {
		case *storage.GetResponse:
			resList = append(resList, res)

			ackReceived++
			if res.Ok {
				ackOk++
			}

			if res.Ok && res.Value.Timestamp > latestTimestamp {
				latestValue = res.Value.Value
			}

			if ackReceived >= ackNeed {
				go rc.readRepair(resList, req.Key, latestValue, latestTimestamp, ackOk, resCh)

				if ackOk == 0 {
					logger.Debugf("No ACK with Ok received for Get(%s): %s", req.Key, res.Message)
					return errors.New(res.Message)
				}

				resp.Value = latestValue
				return nil
			}
		case error:
			continue
		}
	}

	logger.Errorf("Cannot reach consistency requirements for Get(%s, %s)", req.Key, req.Level)
	return errors.New("cannot reach consistency level")
}

// Put handles the incoming Put request. It first looks up for the owner replicas
// of the given key, forwards request to all replicas and deals with them according to
// consistency level.
func (rc *RequestCoordinator) Put(req *PutRequest, resp *PutResponse) error {
	logger.Debugf("Coordinating external request Put(%s, %s, %s)", req.Key, req.Value, req.Level)

	internalReq := &storage.PutRequest{
		Key:   req.Key,
		Value: req.Value,
	}

	replicas := rc.sr.ring.LookupN(req.Key, rc.sr.config.KVSReplicaPoints)
	resCh := rc.sendRPCRequests(replicas, PutOp, internalReq)

	ackNeed := rc.numOfRequiredACK(req.Level)
	ackReceived := 0
	ackOk := 0

	for result := range resCh {
		switch res := result.(type) {
		case *storage.PutResponse:
			ackReceived++
			if res.Ok {
				ackOk++
			}

			if ackReceived >= ackNeed {
				if ackOk == 0 {
					logger.Debugf("No ACK with Ok received for Put(%s, %s): %s", req.Key, req.Value, res.Message)
					return errors.New(res.Message)
				}
				return nil
			}
		case error:
			continue
		}
	}

	logger.Errorf("Cannot reach consistency requirements for Put(%s, %s, %s)", req.Key, req.Value, req.Level)
	return errors.New("cannot reach consistency level")
}

// Delete handles the incoming Delete request. It first looks up for the owner replicas
// of the given key, forwards request to all replicas and deals with them according to
// consistency level.
func (rc *RequestCoordinator) Delete(req *DeleteRequest, resp *DeleteResponse) error {
	logger.Debugf("Coordinating external request Delete(%s, %s)", req.Key, req.Level)

	internalReq := &storage.DeleteRequest{
		Key: req.Key,
	}

	replicas := rc.sr.ring.LookupN(req.Key, rc.sr.config.KVSReplicaPoints)
	resCh := rc.sendRPCRequests(replicas, DeleteOp, internalReq)

	ackNeed := rc.numOfRequiredACK(req.Level)
	ackReceived := 0
	ackOk := 0

	for result := range resCh {
		switch res := result.(type) {
		case *storage.DeleteResponse:
			ackReceived++
			if res.Ok {
				ackOk++
			}

			if ackReceived >= ackNeed {
				if ackOk == 0 {
					logger.Debugf("No ACK with Ok received for Delete(%s): %s", req.Key, res.Message)
					return errors.New(res.Message)
				}
				return nil
			}
		case error:
			continue
		}
	}

	logger.Errorf("Cannot reach consistency requirements for Delete(%s, %s)", req.Key, req.Level)
	return errors.New("cannot reach consistency level")
}

// Stat handles the incoming Stat request.
func (rc *RequestCoordinator) Stat(req *StateRequest, resp *StateResponse) error {
	logger.Debug("Coordinating external request Stat()")

	internalReq := &storage.StatRequest{}

	members := rc.sr.node.Members()
	resCh := make(chan interface{}, len(members))
	var wg sync.WaitGroup

	for _, member := range members {
		wg.Add(1)

		go func(member membership.Member) {
			defer wg.Done()

			stat := NodeStat{
				Address:  member.Address,
				Status:   member.Status,
				KeyCount: 0,
			}

			res, err := rc.sendRPCRequest(member.Address, StatOp, internalReq)
			if err == nil {
				stat.KeyCount = res.(*storage.StatResponse).Count
			}

			resCh <- stat
		}(member)
	}

	go func() {
		wg.Wait()
		close(resCh)
	}()

	for result := range resCh {
		resp.Nodes = append(resp.Nodes, result.(NodeStat))
	}

	return nil
}

func (rc *RequestCoordinator) sendRPCRequests(replicas []string, op string, req interface{}) <-chan interface{} {
	var wg sync.WaitGroup
	resCh := make(chan interface{}, len(replicas))

	for _, replica := range replicas {
		wg.Add(1)

		go func(address string) {
			defer wg.Done()

			res, err := rc.sendRPCRequest(address, op, req)
			if err != nil {
				resCh <- err
				return
			}

			resCh <- res
		}(replica)
	}

	go func() {
		wg.Wait()
		close(resCh)
	}()

	return resCh
}

func (rc *RequestCoordinator) sendRPCRequest(server string, op string, req interface{}) (interface{}, error) {
	client, err := rpc.Dial("tcp", server)
	if err != nil {
		return nil, err
	}

	var resp interface{}
	switch op {
	case GetOp:
		resp = &storage.GetResponse{}
	case PutOp:
		resp = &storage.PutResponse{}
	case DeleteOp:
		resp = &storage.DeleteResponse{}
	case StatOp:
		resp = &storage.StatResponse{}
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Infof("Sending RPC %s request to %s", op, server)
		errCh <- client.Call(op, req, resp)
	}()

	select {
	case err = <-errCh:
		logger.Debugf("Return from RPC call to %s", server)
	case <-time.After(1500 * time.Millisecond):
		logger.Warningf("RPC request to %s timeout", server)
		err = errors.New("request timeout")
	}

	if err != nil {
		logger.Errorf("RPC request to %s error: %s", server, err.Error())
		return nil, err
	}

	return resp, err
}

func (rc *RequestCoordinator) numOfRequiredACK(level string) int {
	switch level {
	case ONE:
		return 1
	case QUORUM:
		return int(math.Floor(float64(rc.sr.config.KVSReplicaPoints)/2)) + 1
	case ALL:
		return rc.sr.config.KVSReplicaPoints
	}

	return rc.sr.config.KVSReplicaPoints
}

func (rc *RequestCoordinator) readRepair(resList []*storage.GetResponse, key string, value string, timestamp int64, okCount int, resCh <-chan interface{}) {
	latestTimestamp := timestamp
	latestValue := value
	ackOk := okCount

	for result := range resCh {
		switch res := result.(type) {
		case *storage.GetResponse:
			resList = append(resList, res)

			if res.Ok {
				ackOk++
			}

			if res.Ok && res.Value.Timestamp > latestTimestamp {
				latestValue = res.Value.Value
			}
		case error:
			continue
		}
	}

	if ackOk == 0 {
		return
	}

	for _, res := range resList {
		if !res.Ok || res.Value.Value != latestValue {
			logger.Debugf("Initiating read repair for %s: (%s, %s)", res.Node, key, latestValue)
			go rc.sendRPCRequest(res.Node, PutOp, &storage.PutRequest{
				Key:   key,
				Value: latestValue,
			})
		}
	}
}
