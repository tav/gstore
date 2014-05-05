package store

import (
	"bytes"
)

type StoreServer struct {
	BaseServer
}
type GetItem struct {
	Key, KeyRange []byte
}
type PutItem struct {
	Key, Value, KeyRange []byte
}
type IterItems struct {
	Start, End, KeyRange []byte
}

type Item struct {
	key   []byte
	value []byte
}

// multiPut?
// multiGet?
// time-window batching of commit log? i.e. Put and Del

func (s *StoreServer) Get(getI *GetItem, value *[]byte) (err error) {
	// assumes the client knows the start key for the memtable...it must do otherwise how does it know to look in this StoreServer??
	// get the memTable
	rangeMeta, err := s.getKeyRange(getI.KeyRange)
	// shouldn't we return a reference to the data from memDB?
	val, err := rangeMeta.memDB.Get(getI.Key)
	value = &val
	return
}

func (s *StoreServer) Put(putI *PutItem, ret *int) (err error) {
	// TODO: snappy compression of values (at least large ones)
	kr, err := s.getKeyRange(putI.KeyRange)
	if err != nil {
		return err
	}
	op := Op{typ: PutOp, key: putI.Key, value: putI.Value}
	var ops []Op
	err = kr.Write(append(ops, op))
	*ret = 1 // error states are they taken care of below?
	return err
}

func (s *StoreServer) Delete(delI *GetItem, ret *int) (err error) {
	// rangeMeta, err := s.getKeyRange(delI.KeyRange)
	kr, err := s.getKeyRange(delI.KeyRange)
	var ops []Op
	op := Op{typ: DeleteOp, key: delI.Key}
	err = kr.Write(append(ops, op))
	*ret = 1 //error states?
	return
}

func (s *StoreServer) Find(rangeI *IterItems, items *[]Item) (err error) {
	// note: between fill calls - read consistency is not guaranteed
	// that is the responsibility of the client

	// get the memTable
	rangeMeta, err := s.getKeyRange(rangeI.KeyRange)
	// get the values between the start and end keys
	for iter := rangeMeta.memDB.Find(rangeI.Start); bytes.Compare(iter.Key(), rangeI.End) != 1; iter.Next() {
		// items should get streamed back to the client and this loop should stop filling items until they are streamed off
		// => writing directly to a network channel with a limited buffer
		// this is an accepted change request but isn't scheduled to happen in any particular release
		// http://code.google.com/p/go/issues/detail?id=6569
		*items = append(*items, Item{iter.Key(), iter.Value()})
	}
	return
}
