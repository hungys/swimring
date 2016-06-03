package storage

type RequestHandlers struct {
	kvs *KVStore
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

func NewRequestHandler(kvs *KVStore) *RequestHandlers {
	rh := &RequestHandlers{
		kvs: kvs,
	}

	return rh
}

func (rh *RequestHandlers) Get(req *GetRequest, resp *GetResponse) error {
	value, err := rh.kvs.Get(req.key)
	if err != nil {
		return err
	}

	resp.key = req.key
	resp.value = value
	return nil
}

func (rh *RequestHandlers) Put(req *PutRequest, resp *PutResponse) error {
	err := rh.kvs.Put(req.key, req.value)
	if err != nil {
		return err
	}

	resp.key = req.key
	resp.value = req.value
	return nil
}

func (rh *RequestHandlers) Delete(req *DeleteRequest, resp *DeleteResponse) error {
	return rh.kvs.Delete(req.key)
}
