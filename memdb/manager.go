package memdb

import (
	"bytes"
	"encoding/binary"
	//"fmt"
	"io"
)

type MemTableManager struct {
	// this table may be quite large
	// e.g. if a server has 100GB Ram and we have a 1m memDB size, there would be 100,000 memDBs on the node
	// we may also want to get data from a bunch of close keyRanges
	// therefore we might want to use a mem table for the nodes key range map
	// compare efficiency of map with memDB
	keyRangeMap map[string]*MemDB
}

func NewMemTableManager() *MemTableManager {
	return &MemTableManager{}
}

func (mm *MemTableManager) Snapshot(keyrange []byte) {
	_ = keyrange
}

func (mm *MemTableManager) Load(keyrange []byte) {
	// get the snapshot for the keyrange
	// get the commit log
	// apply the commit log and begin
	_ = keyrange
}

// use an iterator through the entire memtable and re-construct new node_data and kv_data using Set
func Compact(m *MemDB) *MemDB {
	m.Mutex.RLock()
	defer m.Mutex.RUnlock()
	newMemDB := New()
	// iterator's fill method automatically skips deleted nodes
	t := m.Find([]byte{})
	for t.Next() {
		newMemDB.Set(t.Key(), t.Value())
	}
	newMemDB.Dirty = false
	return newMemDB
}

func Load(file io.Reader) (*MemDB, error) {
	buf := new(bytes.Buffer)
	buf.ReadFrom(file)
	contents := buf.Bytes()
	memDB := Fresh()
	height, bytesRead := binary.Uvarint(contents)
	memDB.height = int(height)
	nodeDataLen, numBytes := binary.Uvarint(contents[bytesRead:])
	bytesRead += numBytes
	endNodeData := bytesRead + int(nodeDataLen)
	var nodeValue uint64
	for bytesRead < endNodeData {
		nodeValue, numBytes = binary.Uvarint(contents[bytesRead:])
		memDB.nodeData = append(memDB.nodeData, int(nodeValue))
		bytesRead += numBytes
	}
	kvLen, numBytes := binary.Uvarint(contents[bytesRead:])
	bytesRead += numBytes
	memDB.kvData = contents[bytesRead : bytesRead+int(kvLen)]
	return memDB, nil
}

func Save(m *MemDB, file io.Writer) error {
	if m.Dirty {
		m = Compact(m)
	}
	var snapshot []byte
	var buf [binary.MaxVarintLen64]byte
	var intLen int
	intLen = binary.PutUvarint(buf[:], uint64(m.height))
	snapshot = append(snapshot, buf[:intLen]...)
	var nodeDataSnap []byte
	nodeDataLen := 0
	for _, node := range m.nodeData {
		// 32 bit int should be sufficient here
		intLen = binary.PutUvarint(buf[:], uint64(node))
		nodeDataLen += intLen
		nodeDataSnap = append(nodeDataSnap, buf[:intLen]...)
	}
	intLen = binary.PutUvarint(buf[:], uint64(nodeDataLen))
	snapshot = append(snapshot, buf[:intLen]...)
	snapshot = append(snapshot, nodeDataSnap...)
	// add crc checks
	kvLen := binary.PutUvarint(buf[:], uint64(len(m.kvData)))
	snapshot = append(snapshot, buf[:kvLen]...)
	snapshot = append(snapshot, m.kvData...)
	length, err := file.Write(snapshot)
	_ = length
	return err
}
