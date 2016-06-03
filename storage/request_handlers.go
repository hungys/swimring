package storage

type RequestHandlers struct {
	kvs *KVStore
}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Node  string
	Key   string
	Value KVEntry
}

type PutRequest struct {
	Key, Value string
}

type PutResponse struct{}

type DeleteRequest struct {
	Key string
}

type DeleteResponse struct{}

func NewRequestHandler(kvs *KVStore) *RequestHandlers {
	rh := &RequestHandlers{
		kvs: kvs,
	}

	return rh
}

func (rh *RequestHandlers) Get(req *GetRequest, resp *GetResponse) error {
	value, err := rh.kvs.Get(req.Key)
	if err != nil {
		return err
	}

	resp.Node = rh.kvs.address
	resp.Key = req.Key
	resp.Value = *value
	return nil
}

func (rh *RequestHandlers) Put(req *PutRequest, resp *PutResponse) error {
	return rh.kvs.Put(req.Key, req.Value)
}

func (rh *RequestHandlers) Delete(req *DeleteRequest, resp *DeleteResponse) error {
	return rh.kvs.Delete(req.Key)
}
