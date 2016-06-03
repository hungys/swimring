package main

const (
	ONE    = "ONE"
	QUORUM = "QUORUM"
	ALL    = "ALL"
)

type RequestCoordinator struct {
	sr *SwimRing
}

type GetRequest struct {
	level string
	key   string
}

type GetResponse struct {
	key, value string
}

type PutRequest struct {
	level      string
	key, value string
}

type PutResponse struct {
	key, value string
}

type DeleteRequest struct {
	level string
	key   string
}

type DeleteResponse struct{}

func NewRequestCoordinator(sr *SwimRing) *RequestCoordinator {
	rc := &RequestCoordinator{
		sr: sr,
	}

	return rc
}

func (rc *RequestCoordinator) Get(req *GetRequest, resp *GetResponse) error {
	return nil
}

func (rc *RequestCoordinator) Put(req *PutRequest, resp *PutResponse) error {
	return nil
}

func (rc *RequestCoordinator) Delete(req *DeleteRequest, resp *DeleteResponse) error {
	return nil
}
