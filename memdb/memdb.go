// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package memdb provides a memory-backed implementation of the db.DB
// interface.
//
// A MemDB's memory consumption increases monotonically, even if keys are
// deleted or values are updated with shorter slices. Callers of the package
// are responsible for explicitly compacting a MemDB into a separate DB
// (whether in-memory or on-disk) when appropriate.
package memdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	//"fmt"
	"math/rand"
	"sync"
)

// maxHeight is the maximum height of a MemDB's skiplist.
const maxHeight = 12

// A MemDB's skiplist consists of a number of nodes, and each node is
// represented by a variable number of ints: a key-offset, a value-offset, and
// between 1 and maxHeight next nodes. The key-offset and value-offset encode
// the node's key/value pair and are offsets into a MemDB's kvData slice.
// The remaining ints, for the next nodes in the skiplist's linked lists, are
// offsets into a MemDB's nodeData slice.
//
// The fXxx constants represent how to find the Xxx field of a node in the
// nodeData. For example, given an int 30 representing a node, and given
// nodeData[30:36] that looked like [60, 71, 82, 83, 84, 85], then
// nodeData[30 + fKey] = 60 would be the node's key-offset,
// nodeData[30 + fVal] = 71 would be the node's value-offset, and
// nodeData[30 + fNxt + 0] = 82 would be the next node at the height-0 list,
// nodeData[30 + fNxt + 1] = 83 would be the next node at the height-1 list,
// and so on. A node's height is implied by the skiplist construction: a node
// of height x appears in the height-h list iff 0 <= h && h < x.
const (
	fKey = iota
	fVal
	fNxt
)

const (
	// zeroNode represents the end of a linked list.
	zeroNode = 0
	// headNode represents the start of the linked list. It is equal to -fNxt
	// so that the next nodes at height-h are at nodeData[h].
	// The head node is an artificial node and has no key or value.
	headNode = -fNxt
)

// A node's key-offset and value-offset fields are offsets into a MemDB's
// kvData slice that stores varint-prefixed strings: the node's key and value.
// A negative offset means a zero-length string, whether explicitly set to
// empty or implicitly set by deletion.
const (
	kvOffsetEmptySlice  = -1
	kvOffsetDeletedNode = -2
)

var ErrNotFound = errors.New("ampstore: not found")

// It is safe to call Get, Set, Delete and Find concurrently.
type MemDB struct {
	Mutex sync.RWMutex
	// height is the number of such lists, which can increase over time.
	height int
	// kvData is an append-only buffer that holds varint-prefixed strings.
	kvData []byte
	// nodeData is an append-only buffer that holds a node's fields.
	nodeData []int
	// indicates that this memdb has been changes since last compaction
	Dirty bool
}

func (m *MemDB) compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// load loads a []byte from m.kvData.
func (m *MemDB) loadBytes(kvOffset int) (b []byte) {
	if kvOffset < 0 {
		return nil
	}
	bLen, n := binary.Uvarint(m.kvData[kvOffset:])
	i, j := kvOffset+n, kvOffset+n+int(bLen)

	return m.kvData[i:j:j]
}

// save saves a []byte to m.kvData.
func (m *MemDB) saveBytes(b []byte) (kvOffset int) {
	if len(b) == 0 {
		return kvOffsetEmptySlice
	}
	kvOffset = len(m.kvData)
	var buf [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(buf[:], uint64(len(b)))
	m.kvData = append(m.kvData, buf[:length]...)
	m.kvData = append(m.kvData, b...)
	return kvOffset
}

// findNode returns the first node n whose key is >= the given key (or nil if
// there is no such node) and whether n's key equals key. The search is based
// solely on the contents of a node's key. Whether or not that key was
// previously deleted from the MemDB is not relevant.
//
// If prev is non-nil, it also sets the first m.height elements of prev to the
// preceding node at each height.
func (m *MemDB) findNode(key []byte, prev *[maxHeight]int) (n int, exactMatch bool) {
	for h, p := m.height-1, headNode; h >= 0; h-- {
		// Walk the skiplist at height h until we find either a zero node
		// or one whose key is >= the given key.
		n = m.nodeData[p+fNxt+h]
		for {
			if n == zeroNode {
				exactMatch = false
				break
			}
			kOff := m.nodeData[n+fKey]
			if c := m.compare(m.loadBytes(kOff), key); c >= 0 {
				exactMatch = c == 0
				break
			}
			p, n = n, m.nodeData[n+fNxt+h]
		}
		if prev != nil {
			(*prev)[h] = p
		}
	}
	return n, exactMatch
}

// Get implements DB.Get, as documented in the leveldb/db package.
func (m *MemDB) Get(key []byte) (value []byte, err error) {
	m.Mutex.RLock()
	defer m.Mutex.RUnlock()
	n, exactMatch := m.findNode(key, nil)
	vOff := m.nodeData[n+fVal]
	if !exactMatch || vOff == kvOffsetDeletedNode {
		return nil, ErrNotFound
	}
	return m.loadBytes(vOff), nil
}

// Set implements DB.Set, as documented in the leveldb/db package.
func (m *MemDB) Set(key, value []byte) error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	if m.Dirty == false {
		m.Dirty = true
	}
	// Find the node, and its predecessors at all heights.
	var prev [maxHeight]int
	n, exactMatch := m.findNode(key, &prev)
	if exactMatch {
		m.nodeData[n+fVal] = m.saveBytes(value)
		return nil
	}
	// Choose the new node's height, branching with 25% probability.
	h := 1
	for h < maxHeight && rand.Intn(4) == 0 {
		h++
	}
	// Raise the skiplist's height to the node's height, if necessary.
	if m.height < h {
		for i := m.height; i < h; i++ {
			prev[i] = headNode
		}
		m.height = h
	}
	// Insert the new node.
	var x [fNxt + maxHeight]int
	n1 := len(m.nodeData)
	x[fKey] = m.saveBytes(key)
	x[fVal] = m.saveBytes(value)
	for i := 0; i < h; i++ {
		j := prev[i] + fNxt + i
		x[fNxt+i] = m.nodeData[j]
		m.nodeData[j] = n1
	}
	m.nodeData = append(m.nodeData, x[:fNxt+h]...)
	return nil
}

// Delete implements DB.Delete, as documented in the leveldb/db package.
func (m *MemDB) Delete(key []byte) error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	if m.Dirty == false {
		m.Dirty = true
	}
	n, exactMatch := m.findNode(key, nil)
	if !exactMatch || m.nodeData[n+fVal] == kvOffsetDeletedNode {
		return ErrNotFound
	}
	m.nodeData[n+fVal] = kvOffsetDeletedNode
	return nil
}

// Find implements DB.Find, as documented in the leveldb/db package.
func (m *MemDB) Find(key []byte) *Iterator {
	m.Mutex.RLock()
	defer m.Mutex.RUnlock()
	n, _ := m.findNode(key, nil)
	for n != zeroNode && m.nodeData[n+fVal] == kvOffsetDeletedNode {
		n = m.nodeData[n+fNxt]
	}
	t := &Iterator{
		m:           m,
		restartNode: n,
	}
	t.fill()
	// The iterator is positioned at the first node >= key. The iterator API
	// requires that the caller the Next first, so we set t.i0 to -1.
	t.i0 = -1
	return t
}

// Close implements DB.Close, as documented in the leveldb/db package.
func (m *MemDB) Close() error {
	return nil
}

// ApproximateMemoryUsage returns the approximate memory usage of the MemDB.
func (m *MemDB) ApproximateMemoryUsage() int {
	m.Mutex.RLock()
	defer m.Mutex.RUnlock()
	return len(m.kvData)
}

// Empty returns whether the MemDB has no key/value pairs.
func (m *MemDB) Empty() bool {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	return len(m.nodeData) == maxHeight
}

// New returns a new MemDB.
func New() *MemDB {
	return &MemDB{
		height: 1,
		kvData: make([]byte, 0, 4096),
		// The first maxHeight values of nodeData are the next nodes after the
		// head node at each possible height. Their initial value is zeroNode.
		nodeData: make([]int, maxHeight, 256),
	}
}
func Fresh() *MemDB {
	return &MemDB{
		height: 1,
		kvData: make([]byte, 0, 4096),
		// The first maxHeight values of nodeData are the next nodes after the
		// head node at each possible height. Their initial value is zeroNode.
		nodeData: make([]int, 0, 256),
	}
}

// iterator is a MemDB iterator that buffers upcoming results, so that it does
// not have to acquire the MemDB's mutex on each Next call.
type Iterator struct {
	m *MemDB
	// restartNode is the node to start refilling the buffer from.
	restartNode int
	// i0 is the current iterator position with respect to buf. A value of -1
	// means that the iterator is at the start, end or both of the iteration.
	// i1 is the number of buffered entries.
	// Invariant: -1 <= i0 && i0 < i1 && i1 <= len(buf).
	i0, i1 int
	// buf buffers up to 32 key/value pairs.
	buf [32][2][]byte
}

// fill fills the iterator's buffer with key/value pairs from the MemDB.
//
// Precondition: t.m.mutex is locked for reading.
func (t *Iterator) fill() {
	i, n := 0, t.restartNode
	for i < len(t.buf) && n != zeroNode {
		if t.m.nodeData[n+fVal] != kvOffsetDeletedNode {
			t.buf[i][fKey] = t.m.loadBytes(t.m.nodeData[n+fKey])
			t.buf[i][fVal] = t.m.loadBytes(t.m.nodeData[n+fVal])
			i++
		}
		n = t.m.nodeData[n+fNxt]
	}
	if i == zeroNode {
		// There were no non-deleted nodes on or after t.restartNode.
		// The iterator is exhausted.
		t.i0 = -1
	} else {
		t.i0 = 0
	}
	t.i1 = i
	t.restartNode = n
}

// Next implements Iterator.Next, as documented in the leveldb/db package.
func (t *Iterator) Next() bool {
	t.i0++
	if t.i0 < t.i1 {
		return true
	}
	if t.restartNode == zeroNode {
		t.i0 = -1
		t.i1 = 0
		return false
	}
	t.m.Mutex.RLock()
	defer t.m.Mutex.RUnlock()
	t.fill()
	return true
}

// Key implements Iterator.Key, as documented in the leveldb/db package.
func (t *Iterator) Key() []byte {
	if t.i0 < 0 {
		return nil
	}
	return t.buf[t.i0][fKey]
}

// Value implements Iterator.Value, as documented in the leveldb/db package.
func (t *Iterator) Value() []byte {
	if t.i0 < 0 {
		return nil
	}
	return t.buf[t.i0][fVal]
}

// Close implements Iterator.Close, as documented in the leveldb/db package.
func (t *Iterator) Close() error {
	return nil
}
