package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	cf := req.Cf
	key := req.Key
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(cf, key)
	if err != nil {
		return nil, err
	}
	res := &kvrpcpb.RawGetResponse{}
	res.Value = val
	if val == nil {
		res.NotFound = true
	} else {
		res.NotFound = false
	}
	return res, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Cf:    req.Cf,
		Key:   req.Key,
		Value: req.Value,
	}
	modify := storage.Modify{
		Data: put,
	}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		return nil, nil
	}
	return &kvrpcpb.RawPutResponse{}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	del := storage.Delete{
		Cf:  req.Cf,
		Key: req.Key,
	}
	modify := storage.Modify{
		Data: del,
	}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		return nil, nil
	}
	return &kvrpcpb.RawDeleteResponse{}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	var kvs []*kvrpcpb.KvPair
	dbit := reader.IterCF(req.Cf)
	dbit.Seek(req.StartKey)
	if !dbit.Valid() {
		return nil, nil
	}
	for i := 0; i < int(req.Limit); i++ {
		val, err := dbit.Item().Value()
		if err != nil {
			return nil, err
		}
		kvp := kvrpcpb.KvPair{
			Key:   dbit.Item().Key(),
			Value: val,
		}
		kvs = append(kvs, &kvp)
		dbit.Next()
		if !dbit.Valid() {
			break
		}
	}
	dbit.Close()
	res := &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}
	return res, nil
}
