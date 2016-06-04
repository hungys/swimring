package main

import (
	"errors"
	"math"
	"net/rpc"
	"sync"

	"github.com/hungys/swimring/storage"
)

const (
	ONE      = "ONE"
	QUORUM   = "QUORUM"
	ALL      = "ALL"
	GetOp    = "KVS.Get"
	PutOp    = "KVS.Put"
	DeleteOp = "KVS.Delete"
)

type RequestCoordinator struct {
	sr *SwimRing
}

type GetRequest struct {
	Level string
	Key   string
}

type GetResponse struct {
	Key, Value string
}

type PutRequest struct {
	Level      string
	Key, Value string
}

type PutResponse struct{}

type DeleteRequest struct {
	Level string
	Key   string
}

type DeleteResponse struct{}

func NewRequestCoordinator(sr *SwimRing) *RequestCoordinator {
	rc := &RequestCoordinator{
		sr: sr,
	}

	return rc
}

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
	latestTimestamp := 0
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
	}

	err = client.Call(op, req, resp)
	if err != nil {
		return nil, err
	}

	return resp, err
}

func (rc *RequestCoordinator) numOfRequiredACK(level string) int {
	switch level {
	case ONE:
		return 1
	case QUORUM:
		return int(math.Floor(float64(rc.sr.config.KVSReplicaPoints) / 2))
	case ALL:
		return rc.sr.config.KVSReplicaPoints
	}

	return rc.sr.config.KVSReplicaPoints
}

func (rc *RequestCoordinator) readRepair(resList []*storage.GetResponse, key string, value string, timestamp int, okCount int, resCh <-chan interface{}) {
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
