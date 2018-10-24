package main

import (
	"context"
	"io/ioutil"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/sirupsen/logrus"
)

type TikvClient struct {
	url   string
	store kv.Storage
}

func Dial(url string) (*TikvClient, error) {
	logrus.SetOutput(ioutil.Discard)
	store, err := tikv.Driver{}.Open(url)
	if err != nil {
		return nil, err
	}
	return &TikvClient{store: store, url: url}, nil
}

func (cli *TikvClient) Get(key []byte) ([]byte, error) {
	txn, err := cli.store.Begin()
	if err != nil {
		return nil, err
	}

	val, err := txn.Get(kv.Key(key))
	if err != nil {
		return nil, err
	}

	return val, nil
}

func (cli *TikvClient) Set(key []byte, val []byte) error {
	txn, err := cli.store.Begin()
	if err != nil {
		return err
	}
	err = txn.Set(kv.Key(key), val)
	if err != nil {
		return err
	}

	err = txn.Commit(context.TODO())
	if err != nil {
		return err
	}
	return nil
}

func (cli *TikvClient) Scan(begin []byte, limit int64, each func(key, val []byte) bool) (int64, error) {
	txn, err := cli.store.Begin()
	if err != nil {
		return 0, err
	}

	iter, err := txn.Seek(kv.Key(begin))
	if err != nil {
		return 0, err
	}
	total := limit
	for iter.Valid() && limit != 0 {
		if !each([]byte(iter.Key()), iter.Value()) {
			break
		}
		if err := iter.Next(); err != nil {
			return total - limit, err
		}
		limit--
	}
	return total - limit, nil
}

func (cli *TikvClient) Delete(key []byte) error {
	txn, err := cli.store.Begin()
	if err != nil {
		return err
	}
	if err := txn.Delete(kv.Key(key)); err != nil {
		return err
	}
	if err := txn.Commit(context.TODO()); err != nil {
		return err
	}
	return nil
}
