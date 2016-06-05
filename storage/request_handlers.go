package storage

// RequestHandlers defines a set of RPC handlers for internal KVS request.
type RequestHandlers struct {
	kvs *KVStore
}

// GetRequest is the payload of Get.
type GetRequest struct {
	Key string
}

// GetResponse is the payload of the response of Get.
type GetResponse struct {
	Ok      bool
	Message string

	Node  string
	Key   string
	Value KVEntry
}

// PutRequest is the payload of Put.
type PutRequest struct {
	Key, Value string
}

// PutResponse is the payload of the response of Put.
type PutResponse struct {
	Ok      bool
	Message string
}

// DeleteRequest is the payload of Delete.
type DeleteRequest struct {
	Key string
}

// DeleteResponse is the payload of the response of Delete.
type DeleteResponse struct {
	Ok      bool
	Message string
}

// StatRequest is the payload of Stat.
type StatRequest struct{}

// StatResponse is the payload of the response of Stat.
type StatResponse struct {
	Ok    bool
	Count int
}

// NewRequestHandler returns a new RequestHandlers.
func NewRequestHandler(kvs *KVStore) *RequestHandlers {
	rh := &RequestHandlers{
		kvs: kvs,
	}

	return rh
}

// Get handles the incoming Get request.
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

// Put handles the incoming Put request.
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

// Delete handles the incoming Delete request.
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

// Stat handles the incoming Stat request.
func (rh *RequestHandlers) Stat(req *StatRequest, resp *StatResponse) error {
	logger.Info("Handling intrnal request Stat()")

	resp.Ok = true
	resp.Count = rh.kvs.Count()

	return nil
}
