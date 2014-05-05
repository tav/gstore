package store

import (
	"github.com/salfield/dynamodb"
	"github.com/tav/ampstore/memdb"
	"sync"
	"time"
)

const (
	RootLease           = 0
	ServerLease         = 1
	renewIntervals      = 3
	rootLeaseLength     = 7 * time.Second
	compactAfterCommits = 100
	compactAfterBytes   = 300 * 1024
	minSnapshotInterval = 60 * 3 * time.Second // rate limiting
	PutOp               = 0
	DeleteOp            = 1
)

type KeyRangeMap map[string]*KeyRangeMeta

type BaseServer struct {
	dbClient *dynamodb.Client
	/* protect in mutex */
	address     string // ip:port
	startTime   time.Time
	keyRangeMap KeyRangeMap
	/* protect in mutex */
	commitTable *dynamodb.Table
	rangeTable  *dynamodb.Table
	mutex       sync.RWMutex
	termChan    chan int
}

type Server interface {
	client() *dynamodb.Client
	kill()
	name() string
	getTermChan() chan int
}

type Lease struct {
	Key         string `ddb:",HASH"` // ip:port:start_time or "root"
	Type        int    // RootLease or ServerLease
	Holder      string
	Contested   bool
	Period      time.Duration
	timer       *time.Timer
	renewTicker *time.Ticker
	server      Server
	table       *dynamodb.Table
	tempL       *Lease
}

type KeyRangeMeta struct {
	KeyRange        string `ddb:"KeyRangeMeta,HASH"`
	SnapshotAddress string
	SnapCommitID    string
	LastCommitID    string
	CNodeIDs        []string // list of Nodes for commits between the SnapCommitSeq and LastCommitSeq
	SplitTo         []KeyRangeMeta
	Level           int16

	partialID           string
	bytesSinceCompact   int64
	commitsSinceCompact int64
	compactionActive    bool
	compactQueue        [][]byte

	snapshotPending    bool // if we compact but are not ready to Snapshot
	snapshotTime       *time.Time
	snapshottingActive bool

	memDB  *memdb.MemDB
	server *BaseServer
	tempK  *KeyRangeMeta
	lock   sync.RWMutex
}

type Commit struct {
	UUID     string `ddb:"Commit,HASH"` // of form: KeyRange + CommitSeq + NodeID + part_id (limited to 2048 bytes)
	Ops      []byte // a list of ops
	PrevUUID string // limited to 2048 bytes
	Parts    uint16 // for commits over 64k the files need to be multipart - accomodates upto 4GB commits
}

type Op struct {
	key   []byte
	value []byte
	typ   byte
}
