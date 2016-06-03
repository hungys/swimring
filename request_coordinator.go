package main

type RequestCoordinator struct {
	sr *SwimRing
}

type GetRequest struct {
	key string
}

type GetResponse struct {
	key, value string
}

type PutRequest struct {
	key, value string
}

type PutResponse struct {
	key, value string
}

type DeleteRequest struct {
	key string
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
