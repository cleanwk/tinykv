package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandaloneReader struct {
	txn *badger.Txn
}

func NewStandaloneReader(txn *badger.Txn) *StandaloneReader {
	return &StandaloneReader{
		txn: txn,
	}
}

//GetCF 返回的是cfWithKey 对应的val
func (s *StandaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		err = nil
	}
	return val, err
}
func (s *StandaloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandaloneReader) Close() {
	_ = s.txn.Commit()
}
