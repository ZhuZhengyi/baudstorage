package raftopt

import (
	"fmt"
	"github.com/tecbot/gorocksdb"
)

//#cgo CFLAGS:-I/usr/local/include
//#cgo LDFLAGS:-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy
import "C"

type RocksDBStore struct {
	dir string
	db  *gorocksdb.DB
}

func NewRocksDBStore(dir string) (store *RocksDBStore) {
	store = &RocksDBStore{dir: dir}
	err := store.Open()
	if err != nil {
		panic(fmt.Sprintf("Failed to Open rocksDB! err:%v", err.Error()))
	}
	return store
}

func (rs *RocksDBStore) Open() error {
	basedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
	basedTableOptions.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(basedTableOptions)
	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, rs.dir)

	if err != nil {
		err = fmt.Errorf("action[openRocksDB],err:%v", err)
		return err
	}
	rs.db = db

	return nil

}

func (rs *RocksDBStore) Delete(key string) (interface{}, error) {
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	defer wb.Clear()
	if err := rs.db.Delete(wo, []byte(key)); err != nil {
		err = fmt.Errorf("action[deleteFromRocksDB],err:%v", err)
		return nil, err
	}

	return nil, nil

}

func (rs *RocksDBStore) Put(key string, value []byte) (interface{}, error) {
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	wb.Put([]byte(key), value)
	if err := rs.db.Write(wo, wb); err != nil {
		err = fmt.Errorf("action[putToRocksDB],err:%v", err)
		return nil, err
	}

	return nil, nil
}

func (rs *RocksDBStore) Get(key string) (value []byte, err error) {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	if value, err = rs.db.GetBytes(ro, []byte(key)); err != nil {
		err = fmt.Errorf("action[getFromRocksDB],err:%v", err)
		return nil, err
	}

	return value, nil
}

func (rs *RocksDBStore) Snapshot() *gorocksdb.Snapshot {
	return rs.db.NewSnapshot()
}

func (rs *RocksDBStore) ReleaseSnapshot(snapshot *gorocksdb.Snapshot) {
	rs.db.ReleaseSnapshot(snapshot)
}

func (rs *RocksDBStore) Iterator(snapshot *gorocksdb.Snapshot) *gorocksdb.Iterator {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	ro.SetSnapshot(snapshot)

	return rs.db.NewIterator(ro)
}

func (rs *RocksDBStore) BatchPut(kvArr []*Kv) (interface{}, error) {
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	for _, kv := range kvArr {
		wb.Put([]byte(kv.K), kv.V)
	}

	if err := rs.db.Write(wo, wb); err != nil {
		err = fmt.Errorf("action[batchPutToRocksDB],err:%v", err)
		return nil, err
	}
	return nil, nil
}
