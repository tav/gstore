package store

import ()

type AddressServer struct {
	BaseServer
}
type Range struct {
	StartKey, EndKey []byte
}

// 1. API, levels and recursion
// 2.

func (s *AddressServer) NewRange(startKey []byte, ret *int) (err error) {
	return nil
}

func (s *AddressServer) RangeForKey(key []byte, keyRange *Range) (err error) {
	// levels
	keyRange = &Range{[]byte{}, []byte{}}
	return nil
}

func (s *AddressServer) ServeRange(startKey []byte, ret *int) (err error) {
	// attempt to change the range record to point to this node_id
	_ = startKey
	return nil
}

func (s *AddressServer) getRootTablet() bool {
	// AddressServer
	// request tablet
	// if failed to get it:
	//    acquireRootLease()
	return true
}

func (s *AddressServer) acquireRootLease() {
	// acquire the root lease or create it
	l, err := GetLease("root")
	if err != nil {
		l, err = NewLease("root", RootLease, "", rootLeaseLength, s)
		if err != nil {
			return
		}
	}
	go l.contest()
}

// start method
// s.getRootTablet()
