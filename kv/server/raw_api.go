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
	response := &kvrpcpb.RawGetResponse{}

	var reader storage.StorageReader
	defer reader.Close()

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	if len(value) == 0 {
		response.NotFound = true
	}

	response.Value = value
	return nil, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	response := &kvrpcpb.RawPutResponse{}
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	}

	if err := server.storage.Write(req.Context, batch); err != nil {
		return nil, err
	}
	return response, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	response := &kvrpcpb.RawDeleteResponse{}
	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	}

	if err := server.storage.Write(req.Context, batch); err != nil {
		return nil, err
	}
	return response, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	response := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return nil, err
	}

	iterator := reader.IterCF(req.Cf)
	defer iterator.Close()

	var pairs []*kvrpcpb.KvPair
	limit := req.Limit
	for iterator.Seek(req.StartKey); iterator.Valid() && limit > 0; iterator.Next() {
		item := iterator.Item()
		value, err := item.Value()
		if err != nil {
			return response, err
		}
		pairs = append(pairs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		})
		limit--
	}
	response.Kvs = pairs

	return response, nil
}
