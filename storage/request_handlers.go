package storage

type RequestHandlers struct {
	kvs *KVStore
}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Ok      bool
	Message string

	Node  string
	Key   string
	Value KVEntry
}

type PutRequest struct {
	Key, Value string
}

type PutResponse struct {
	Ok      bool
	Message string
}

type DeleteRequest struct {
	Key string
}

type DeleteResponse struct {
	Ok      bool
	Message string
}

func NewRequestHandler(kvs *KVStore) *RequestHandlers {
	rh := &RequestHandlers{
		kvs: kvs,
	}

	return rh
}

func (rh *RequestHandlers) Get(req *GetRequest, resp *GetResponse) error {
	logger.Infof("Handling intrnal request Get(%s)", req.Key)

	value, err := rh.kvs.Get(req.Key)
	resp.Node = rh.kvs.address
	if err != nil {
		resp.Ok = false
		resp.Message = err.Error()
		return nil
	}

	resp.Ok = true
	resp.Key = req.Key
	resp.Value = *value
	return nil
}

func (rh *RequestHandlers) Put(req *PutRequest, resp *PutResponse) error {
	logger.Infof("Handling intrnal request Put(%s, %s)", req.Key, req.Value)

	err := rh.kvs.Put(req.Key, req.Value)
	if err != nil {
		resp.Ok = false
		resp.Message = err.Error()
		return nil
	}

	resp.Ok = true
	return nil
}

func (rh *RequestHandlers) Delete(req *DeleteRequest, resp *DeleteResponse) error {
	logger.Infof("Handling intrnal request Delete(%s)", req.Key)

	err := rh.kvs.Delete(req.Key)
	if err != nil {
		resp.Ok = false
		resp.Message = err.Error()
		return nil
	}

	resp.Ok = true
	return nil
}
