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
	logger.Debugf("Coordinating for request Get(%s, %s)", req.Key, req.Level)

	internalReq := &storage.GetRequest{
		Key: req.Key,
	}

	replicas := rc.sr.ring.LookupN(req.Key, rc.sr.config.KVSReplicaPoints)
	resCh := rc.sendRPCRequests(replicas, GetOp, internalReq)
	resp.Key = req.Key

	ackNeed := rc.numOfRequiredACK(req.Level)
	ackReceived := 0
	latestTimestamp := 0
	latestValue := ""

	var resList []*storage.GetResponse

	for result := range resCh {
		switch res := result.(type) {
		case *storage.GetResponse:
			ackReceived++
			resList = append(resList, res)
			if res.Value.Timestamp > latestTimestamp {
				latestValue = res.Value.Value
			}

			if ackReceived >= ackNeed {
				resp.Value = latestValue
				go rc.readRepair(resList, req.Key, latestValue, latestTimestamp, resCh)
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
	logger.Debugf("Coordinating for request Put(%s, %s, %s)", req.Key, req.Value, req.Level)

	internalReq := &storage.PutRequest{
		Key:   req.Key,
		Value: req.Value,
	}

	replicas := rc.sr.ring.LookupN(req.Key, rc.sr.config.KVSReplicaPoints)
	resCh := rc.sendRPCRequests(replicas, PutOp, internalReq)

	ackNeed := rc.numOfRequiredACK(req.Level)
	ackReceived := 0

	for result := range resCh {
		switch result.(type) {
		case *storage.PutResponse:
			ackReceived++
			if ackReceived >= ackNeed {
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
	logger.Debugf("Coordinating for request Delete(%s, %s)", req.Key, req.Level)

	internalReq := &storage.DeleteRequest{
		Key: req.Key,
	}

	replicas := rc.sr.ring.LookupN(req.Key, rc.sr.config.KVSReplicaPoints)
	resCh := rc.sendRPCRequests(replicas, DeleteOp, internalReq)

	ackNeed := rc.numOfRequiredACK(req.Level)
	ackReceived := 0

	for result := range resCh {
		switch result.(type) {
		case *storage.DeleteResponse:
			ackReceived++
			if ackReceived >= ackNeed {
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

func (rc *RequestCoordinator) readRepair(resList []*storage.GetResponse, key string, value string, timestamp int, resCh <-chan interface{}) {
	latestTimestamp := timestamp
	latestValue := value

	for result := range resCh {
		switch res := result.(type) {
		case *storage.GetResponse:
			resList = append(resList, res)
			if res.Value.Timestamp > latestTimestamp {
				latestValue = res.Value.Value
			}
		case error:
			continue
		}
	}

	for _, res := range resList {
		if res.Value.Value != latestValue {
			logger.Debugf("Initiating read repair for %s: (%s, %s)", res.Node, key, latestValue)
			go rc.sendRPCRequest(res.Node, PutOp, &storage.PutRequest{
				Key:   key,
				Value: latestValue,
			})
		}
	}
}
