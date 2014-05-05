// Public Domain (-) 2013 The Ampstore Authors.
// See the Ampstore UNLICENSE file for details.

package ampstore

import (
	"fmt"
	"github.com/tav/golly/dynamodb"
	// "reflect"
	"time"
)

type DB struct {
	name    string
	client  *dynamodb.Client
	txn     bool
	pending []byte
}

// max hash key 2048 bytes
// max range key 1024 bytes
// items size 64kb
// query/scan results set 1mb
// batch write item 25ops and 1mb
// batch get 100ops and 1mb

// User:tom/Message:27197

// Item

//     RootKey "User:tom"
//     PathKey "\x01Message:27197"
//     Data    toJSON({"content": "hi"})

//     RootKey "User:tom:root"
//     PathKey "\x00"
//     Data    toJSON({"name": "Tom Salfield", "home": "London"})

func (db *DB) Init() error {
	// Check if tables exists.
	// Else create tables.
	// db.client.Call("CreateTable", db.name+"-indexes")
	db.client.Call("CreateTable", dynamodb.Map{
		"AttributeDefinitions": []dynamodb.Map{
			{"AttributeName": "", "AttributeType": ""},
			{"AttributeName": "", "AttributeType": ""},
		},
		"KeySchema": []dynamodb.Map{
			{"AttributeName": "R", "KeyType": "HASH"},
			{"AttributeName": "K", "KeyType": "RANGE"},
		},
	})
	// db.client.Call("CreateTable", db.name+"-roots")
	// db.client.Call("CreateTable", db.name+"-counters")
	return nil
}

func (db *DB) Get(key *Key, value interface{}) error {
	// err := db.client.Call("GetItem", `{"TableName": $Name-items, ...}`)
	// if err != nil {
	// 	return err
	// }
	// decodeInto(value)
	return nil
}

// `name: tav: user/23`
// `name: tom: user/12`

func (db *DB) MultiGet(keys []*Key, value interface{}) error {
	return nil
}

func (db *DB) MultiDelete(keys []*Key) error {
	return nil
}

func (db *DB) MultiPut(keys []*Key, value interface{}) error {
	return nil
}

// type Map map[string]interface{}

// func (db *DB) makeRequest(method string, payload []byte) (resp Map, err error) {
// 	payload, err = db.client.RawRequest(method, payload)
// 	// fmt.Println("RESP PAYLOAD: ", string(payload))
// 	if err != nil {
// 		return
// 	}
// 	resp = Map{}
// 	err = json.Unmarshal(payload, &resp)
// 	return
// }

func (db *DB) Put(key *Key, value interface{}) error {
	// if db.txn {

	// }
	return nil
}

func (db *DB) Delete(key *Key) error {
	return nil
}

type Query struct {
	ops []*op
}

// type Query chan op

type op struct {
}

// q := db.Query()
// q = q.Filter("foo =", "blah").Filter("m = ", 23)
// q = q.Filter("x =", 23).Filter("tag IN", []string{"foo", "bar"})
// q.WithCursor(c).Limit(10)
// q.Limit(20)

// for result := range q.Run() {
//    result.Cursor()
// }

// Ancestor

func (q *Query) Clone() *Query {
	ops := make([]*op, len(q.ops))
	copy(ops, q.ops)
	return &Query{ops: ops}
}

func (q *Query) Filter() *Query {
	return q
}

func (q *Query) Get(value interface{}) ([]*Key, error) {
	return nil, nil
}

func (q *Query) KeysOnly() *Query {
	return q
}

func (q *Query) Limit(n int) *Query {
	return q
}

func (q *Query) WithCursor(c []byte) *Query {
	return q
}

func (db *DB) Push(value interface{}) error {
	return nil
}

func (db *DB) PushIf(value interface{}) error {
	return nil
}

func (db *DB) Query() *Query {
	return &Query{}
}

// func (db *DB) Counter(name string) *Counter {
// 	return *Counter{
// 		db:   db,
// 		name: name,
// 	}
// }

func (db *DB) Transact(handler func(db *DB) error) error {
	newDB := &DB{
		client: db.client,
		txn:    true,
	}
	i := 0
	for {
		err := handler(newDB)
		if err == nil {
			return nil
		}
		i++
		if i < 5 {
			continue
		}
		return err
	}
}

func (db *DB) Incr(key *Key) error {
	return nil
}

func (db *DB) Decr(key *Key) error {
	return nil
}

func (db *DB) Add(key *Key, n int) error {
	return nil
}

func (db *DB) AutoID(kind string) *Key {
	return &Key{
		Kind:  kind,
		IntID: -1,
	}
}

func (db *DB) Key(kind, id string) *Key {
	return &Key{
		Kind:     kind,
		StringID: id,
	}
}

func (db *DB) Subtract(key *Key, n int) error {
	return nil
}

// type Order struct {
// 	CustomerID string `db:"noindex"`
// }

// db.Put(db.Key("User", "tom"), somePointer)
// db.Put(db.AutoID("Message"), somePointer)

func Dial(name string, client *dynamodb.Client) *DB {
	return &DB{
		name:   name,
		client: client,
	}
}

func main() {
	// client := dynamodb.Dial(dynamodb.USWest1, dynamodb.Auth("access", "secret"), nil)
	db := Dial("wikifactory", nil)

	// db.Init()
	// db.Transact(func(db *DB) error {
	// 	o := &Order{}
	// 	err := db.Get("foo", o)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	if o.CustomerID < 1000 {
	// 		w := &Winner{Customer: o.CustomerID}
	// 		db.Put("winner", w)
	// 	}
	// 	return nil
	// })

	// iter := q.Run(c)
	// for {
	// 	var x Widget
	// 	key, err := iter.Next(&x)
	// 	if err == datastore.Done {
	// 		break
	// 	}
	// 	if err != nil {
	// 		return
	// 	}
	// }

	// widgets := []*Widget{}
	// keys, err := q.Get(&widgets)
	// if err != nil {
	// 	return
	// }

	// var widget *Widget
	// for result := range q.Run() {
	// 	err := result.To(widget)
	// 	if err != nil {
	// 		return
	// 	}
	// }

	// results, err = q.GetResultsAs(widget)
	// if err != nil {
	// 	return
	// }
	// for result := range results {
	// 	widget, ok = result.(*Widget)
	// 	if !ok {
	// 		return
	// 	}
	// }

	// results, err = q.Run()
	// if err != nil {
	// 	return
	// }
	// for result := range results {
	// 	widget, ok = result.(*Widget)
	// 	if !ok {
	// 		return
	// 	}
	// }

	// k := db.Key("User", "tom", "Widget", "foo")
	// k.Parent().Equals(db.Key("User", "tom")) // true

	// type Widget struct {
	// 	Name      string `db:"key"`
	// 	Dimension int
	// }

	// type Widget2 struct {
	// 	ID        int `db:"auto"`
	// 	Name      string
	// 	Dimension int
	// }

}

// TODO(tom): It would be advantageous to minimise latency
// by supporting projected queries with dynamic fields, e.g.
//
//    query.Values("attr1", "attr2", "etc.").Run()
//
// These fields could be stored as additional properties on
// the index item -- with a minimal hit on the first read,
// if they are not already present.
//
// Issues to watch out for:
//
// - Transactions
//
// - Too large a hit on our assumptions about index rows,
//   i.e. limits on batchget on DynamoDB rows.
