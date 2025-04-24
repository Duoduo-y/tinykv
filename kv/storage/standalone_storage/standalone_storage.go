package standalone_storage

import (
	"path"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines //engine_util对badger进行了封装
	conf   *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvPath := path.Join(conf.DBPath, "kv")
	raftPath := path.Join(conf.DBPath, "raft")
	kvDB := engine_util.CreateDB(kvPath, conf.Raft)
	raftDB := engine_util.CreateDB(raftPath, conf.Raft)
	return &StandAloneStorage{
		engine: engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath),
		conf:   conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.engine.Close()

	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandAloneStorageReader(s.engine.Kv.NewTransaction(false)), nil
}

// Modify 本质上代表着Put和Delete两种操作，通过下面的函数可以看到，它是通过断言来区分Put还是Delete的。一个Modify对应一个kv
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	var err error
	err = nil
	for _, m := range batch {
		k, v, cf := m.Key(), m.Value(), m.Cf()
		if _, ok := m.Data.(storage.Put); ok {
			err = engine_util.PutCF(s.engine.Kv, cf, k, v)
		} else if _, ok := m.Data.(storage.Delete); ok {
			err = engine_util.DeleteCF(s.engine.Kv, cf, k)
		}
	}
	return err
}

// 实现StorageReader接口
type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorageReader(badgertxn *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		txn: badgertxn,
	}
}

// When the key doesn't exist, return nil for the value
// 没有key，那么err也是nil！！！不能直接返回val,err（ErrKeyNotFound）
func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}
func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	// txn:= r.db.NewTransaction(false)
	dbit := engine_util.NewCFIterator(cf, r.txn)
	return dbit
}
func (r *StandAloneStorageReader) Close() {
	defer r.txn.Discard()
}
