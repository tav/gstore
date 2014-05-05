package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/salfield/dynamodb"
	"github.com/tav/ampstore/memdb"
	"github.com/tav/golly/prime"
	"launchpad.net/goamz/s3"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"time"
)

func NewLease(key string, Type int, holder string, period time.Duration, server Server) (*Lease, error) {
	l := &Lease{
		Key:       key,
		Type:      Type,
		Holder:    holder,
		Contested: false,
		Period:    period,
		server:    server,
		table:     server.client().Table("Lease"),
		tempL:     &Lease{Key: key},
	}
	err := l.table.Add(l)
	if err != nil {
		// failed to add lease to database
		return nil, err
		// if error LeaseExists
		//   return nil
		// l.expire()
	}
	return l, err
}

func GetLease(key string) (*Lease, error) {
	l := &Lease{
		Key: key,
	}
	err := l.table.Get(l, true)
	// do something different if the error is "doesNotExist"?
	return l, err
}

func (l *Lease) acquire() {
	l.Holder = l.server.name()
	l.Contested = false
	// replace the lease assuming it hasn't already been replaced
	err := l.table.PutIf(l, l.tempL)
	if err != nil {
		return
	}
	l.Contested = false
	l.hold()
}

func (l *Lease) contest() {
	// get the lease and contest it
	err := l.getLeaseData(true)
	if err != nil || l.tempL.Contested {
		return
	}
	if l.tempL.Holder == "" && l.tempL.Type == RootLease {
		// the rootLease is unclaimed
		l.acquire()
	}

	l.Contested = true
	holder := l.tempL.Holder
	err = l.table.PutIf(l, l.tempL)
	if err != nil {
		return
	}
	// then wait for the lease time
	time.Sleep(l.Period)
	//err := l.getLeaseData()
	if l.tempL.Contested && l.tempL.Holder == holder {
		// it hasn't been successfully renewed in the lease time
		if l.Type == ServerLease {
			// this is a safeguard, it should have deleted itself if the server was shutdown cleanly
			l.delete()
		} else {
			// we want to acquire the root lease
			l.acquire()
		}
	}
}

func (l *Lease) delete() {
	err := l.table.Delete(l)
	if err != nil {
		log.Printf("failed to delete lease for %s", l.Key)
	}
}

func (l *Lease) hold() {
	l.timer = time.NewTimer(l.Period)
	l.renewTicker = time.NewTicker(l.Period / renewIntervals)
Renew:
	for {
		select {
		case <-l.renewTicker.C:
			l.renew()
		case <-l.timer.C:
			l.expire()
			break Renew
		}
	}
}

func (l *Lease) getLeaseData(firstTry bool) error {
	err := l.table.Get(l.tempL, true)
	if err != nil && firstTry {
		jitter, err := prime.Between(1000, 30000)
		if err != nil {
			return err
		}
		time.Sleep(time.Duration(jitter) * time.Microsecond)
		l.getLeaseData(false)
	}
	return err
}

func (l *Lease) renew() {
	now := time.Now()
	err := l.getLeaseData(true)
	if err != nil {
		l.expire()
	}
	if l.Holder == l.tempL.Holder {
		// the lease hasn't been grabbed while we were stuck garbage collecting etc
		if l.tempL.Contested {
			// put back the existing lease so long as the contested version hasn't changed again in the meantime
			err := l.table.PutIf(l, l.tempL)
			if err != nil {
				// if the error is due to oldLease already having been succesfully contested (changed)
				l.expire()
				return
				// if the error is due to some other fault try again to renew?
				// l.renew()
			}

		}
		// we have successfully renewed the lease
		remaining := l.Period - time.Since(now)
		if remaining < 0 {
			l.expire()
		}
		active := l.timer.Reset(remaining)
		if !active { // timer has already been stopped or expired
			l.expire()
		}
	} else {
		l.expire()
	}
}

func (l *Lease) expire() {
	// lease is invalid
	l.timer.Stop()
	l.renewTicker.Stop()
	// close channels?
	l.timer = nil
	l.renewTicker = nil

	if l.Type == ServerLease {
		// if its a server lease delete it and restart the server
		l.delete()
		l.server.kill()
	} else {
		// its the root lease
		// make it possible to acquire without waiting for the contesting time
		l.Holder = ""
		l.table.PutIf(l, l.tempL)
		// try and re-acquire
		l.getLeaseData(true)
		if l.tempL.Holder == "" && !l.tempL.Contested {
			// clobber the existing lease data
			err := l.table.Get(l, true)
			if err == nil {
				l.contest()
			}
		}
	}
}

func NewBaseServer(dbClient *dynamodb.Client, address string) BaseServer {
	return BaseServer{
		dbClient:    dbClient,
		address:     address,
		startTime:   time.Now(),
		keyRangeMap: KeyRangeMap{},
		commitTable: dbClient.Table("Commit"),
		rangeTable:  dbClient.Table("KeyRangeMeta"),
	}
}

func (s *BaseServer) client() *dynamodb.Client {
	return s.dbClient
}

func (s *BaseServer) name() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return fmt.Sprintf("%s:%s", s.address, s.startTime)
}

func (s *BaseServer) start(period time.Duration) {
	l, err := NewLease(s.name(), ServerLease, s.name(), period, s)
	if err != nil {
		s.kill()
		return
	}
	// since we created the lease and its unique we don't need to contest it, we can just hold it
	go l.hold()
}
func (s *BaseServer) shutdown() {
	channel := s.getTermChan()
	channel <- 1
}
func (s *BaseServer) getTermChan() chan int {
	return s.termChan
}
func (s *BaseServer) kill() {
	// lock the keyrange data
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.startTime = time.Time{}
	s.keyRangeMap = KeyRangeMap{}
	// should we now:
	// a) shutdown the server and restart it
	s.shutdown()
	// b) acquire a new ServerLease
	// and after starting up again, should we:
	// a) continue to serve requests and attempt to update addres entries on receiving requests
	// b) use a disk cache to actively update address entries to the new timestamp
}

func (s *BaseServer) load(keyRange []byte) {
	// get the snapshot for the keyrange
	// get the commit log
	// apply the commit log and begin
	s.mutex.Lock()
	defer s.mutex.Unlock()
	k := s.rangeTable.Get(keyRange)
	// load the snapshot
	// if k.SnapCommitID != k.LastCommitID {
	// catchup
	// compress and snapshot
	//}
	// serve
	// k.snapshotTime =
}

func (s *BaseServer) getKeyRange(keyRange []byte) (keyRangeMeta *KeyRangeMeta, err error) {
	// if keyRange not in keyRangeMap
	//     do address lookup
	//     if keyRange exists
	//         if this server's IP:port was last responsible
	//             load from disk and sync to dynamo
	//         else
	//             check if lease is live
	//             if live
	//                 tell the client who should be responsible
	//             else
	//                 ask the parent range to make someone responsible
	//                 tell client who is now responsible
	//     else keyRange doesn't exist (usually because the endKey has been changed due to splitting)
	//         return an error to the client requesting a new address lookup
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	keyRangeMeta = s.keyRangeMap[string(keyRange)]
	if keyRangeMeta == nil {
		err = errors.New("KeyRange not available")
		return
	}
	return
}

func (k *KeyRangeMeta) Write(ops []Op) (err error) {
	// Commit Item schema: C=startKey + , key=binary, val=binary, val_type=string, method [SET, DEL]
	if err != nil {
		// should be a typed error for client to respond accordingly?
		return err
	}
	var opData []byte
	var buf [binary.MaxVarintLen64]byte
	for _, op := range ops {
		opData = append(opData, op.typ)
		keyLength := binary.PutUvarint(buf[:], uint64(len(op.key)))
		opData = append(opData, buf[:keyLength]...)
		opData = append(opData, op.key...)
		if op.typ == PutOp {
			valLength := binary.PutUvarint(buf[:], uint64(len(op.value)))
			opData = append(opData, buf[:valLength]...)
			opData = append(opData, op.value...)
		}
	}
	err = k.server.rangeTable.Get(k, true)
	// must initialise k.tempK to avoid nil-pointer dereference
	*k.tempK = *k
	k.tempK.newCommitID()
	// Is the 64KB limit in decimal or binary?
	// limit must include attribute names and values
	// need to limit startkey length
	// 65536 - (2048 * 2) -   2   -    20  = 61418
	// 64kB  - max UUIDlen - parts - keylengths
	lenOpData := len(opData)

	parts := uint16(1)
	if lenOpData > 61418 {
		parts = uint16(lenOpData / 61418)
	}
	for i := uint16(1); i <= parts; i++ {
		err = k.server.commitTable.Put(Commit{
			UUID:     fmt.Sprintf("%s-%i", k.tempK.partialID, i),
			Ops:      opData[:61418],
			PrevUUID: k.LastCommitID,
			Parts:    parts,
		})
		opData = opData[61418:]
	}
	if err != nil {
		log.Panicf("%v ", err)
	}
	// TODO: modify k but check the old value
	err = k.server.rangeTable.PutIf(k.tempK, k)
	k.LastCommitID = k.tempK.LastCommitID
	k.CNodeIDs = k.tempK.CNodeIDs
	for _, op := range ops {
		if op.typ == PutOp {
			k.memDB.Set(op.key, op.value)
		} else if op.typ == DeleteOp {
			k.memDB.Delete(op.key)
		}
	}
	if k.commitsSinceCompact >= compactAfterCommits || k.bytesSinceCompact >= compactAfterBytes {
		// TODO: snapshot should be called on keyrangeMeta?
		if k.compactionActive == False {
			k.compactionActive = True
			go k.compact()
		}
	}
	return
}
func (k *KeyRangeMeta) compact() (err error) {
	// copy and compact the memdb
	newMemdb := k.memDB.Compact()
	for n, commit := range k.compactQueue {
		if n == last {
			k.mutex.lock()
			defer k.mutex.unlock()
			// lock keyrange/compactQueue => don't allow new writes
			// defer unlock
		}
		apply(newMemdb, k.compactQueue)
	}
	// swap memdb pointers
	k.memDB = newMemdb
	// stop queuing
	k.compactionActive = False
	k.compactQueue = [][]byte{}
	// reset compaction counters
	k.bytesSinceCompact = 0
	k.commitsSinceCompact = 0
	if time.Since(k.snapshotTime) > snapshotAfterSeconds {
		k.snapshot(newMemdb)
	} else {
		k.snapshotPending = True

	}
	return nil
}
func (k *KeyRangeMeta) newCommitID() (err error) {
	var lastCommitSeq uint64
	if k.LastCommitID == "" {
		lastCommitSeq = 0
	} else {
		lastCommitSeq, err = strconv.ParseUint(strings.Split(k.LastCommitID, "-")[1], 10, 64)
		if err != nil {
			return err
		}
	}
	if !stringInSlice(k.server.name(), k.CNodeIDs) {
		k.CNodeIDs = append(k.CNodeIDs, k.server.name())
	}
	k.partialID = fmt.Sprintf("%s-%i-%s", k.KeyRange, lastCommitSeq+1, k.server.name())
	k.LastCommitID = fmt.Sprintf("%s-%i", k.partialID, 1)
	return nil
}

func (k *KeyRangeMeta) snapshot() (err error) {
	k.lock()
	defer k.lock.unlock()
}

///////////////////////////////////////
// StartUp code
///////////////////////////////////////
func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func CheckTables(dbClient *dynamodb.Client) {
	tables, err := dbClient.ListTables(4, "")
	if err != nil {
		log.Fatal(err)
	}
	tableNames := tables.TableNames
	if len(tableNames) > 3 {
		log.Fatal("There should only be three tables")
	}
	if stringInSlice("Commit", tableNames) == false {
		// Commit Item schema: C=HASH_key, []ops(val=binary, val_type=string, method [SET, DEL])
		dbClient.CreateTable("Commit", &Commit{}, 10, 10, nil, nil)
	}
	if stringInSlice("KeyRangeMeta", tableNames) == false {
		dbClient.CreateTable("KeyRangeMeta", &KeyRangeMeta{}, 10, 10, nil, nil)
	}
	if stringInSlice("Lease", tableNames) == false {
		dbClient.CreateTable("Lease", &Lease{}, 10, 10, nil, nil)
	}
	for {
		commitTable, _ := dbClient.DescribeTable("Commit")
		commitMetaTable, _ := dbClient.DescribeTable("KeyRangeMeta")
		LeaseTable, _ := dbClient.DescribeTable("Lease")
		if commitTable.TableStatus == "ACTIVE" && commitMetaTable.TableStatus == "ACTIVE" && LeaseTable.TableStatus == "ACTIVE" {
			break
		} else {
			log.Printf("creating tables\n")
		}
	}
	tables, err = dbClient.ListTables(4, "")
	if err != nil {
		log.Fatal(err)
	}
	if len(tables.TableNames) != 3 {
		log.Fatal("There should be exactly three tables")
	}
}

func StartServer(address string, server Server) chan int {
	RPC := rpc.NewServer()
	RPC.Register(server)
	listener, e := net.Listen("tcp", address)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go RPC.Accept(listener)
	return server.getTermChan()
}
