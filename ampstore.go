// Public Domain (-) 2013 The Ampstore Authors.
// See the Ampstore UNLICENSE file for details.

// package ampstore
package main

import (
	"github.com/tav/golly/dynamodb"
	"reflect"
)

type DB struct {
	name    string
	client  *dynamodb.Client
	txn     bool
	pending []byte
}

func (db *DB) Init() error {
	// Check if tables exists.
	// Else create tables.
	// db.client.Call("CreateTable", db.name+"-indexes")
	// db.client.Call("CreateTable", db.name+"-items")
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

func (db *DB) Put(key *Key, value interface{}) error {
	if db.txn {

	}
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

type Cursor []byte

func (q *Query) WithCursor(c Cursor) *Query {
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

func (db *DB) Transact(handler func(*DB) error) error {
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

type Counter struct {
	DB   *DB
	Name string
}

func (c *Counter) Incr() error {
	return nil
}

func (c *Counter) Decr() error {
	return nil
}

func (c *Counter) Add(n int) error {
	return nil
}

func (c *Counter) Subtract(n int) error {
	return nil
}

type Order struct {
	CustomerID string `db:"noindex"`
}

var registry = map[string]reflect.Type{}

func Register(name string, example interface{}) {
	registry[name] = reflect.TypeOf(example)
}

func New(name string, client *dynamodb.Client) *DB {
	return &DB{
		name:   name,
		client: client,
	}
}

type Key struct {
	Kind     string
	StringID string
	IntID    int64
	Parent   *Key
}

// db.Put(db.Key("User", "tom"), somePointer)
// db.Put(db.AutoID("Message"), somePointer)

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

func (k *Key) WithParent(p *Key) *Key {
	k.Parent = p
	return k
}

func (k *Key) Equals(o *Key) bool {
	if k.Kind == o.Kind && k.IntID == o.IntID && k.StringID == o.StringID {
		if k.Parent == nil {
			if o.Parent == nil {
				return true
			}
			return false
		}
		if o.Parent == nil {
			return false
		}
		return k.Parent.Equals(o.Parent)
	}
	return false
}

func (k *Key) Equals2(o *Key) bool {
	for k != nil && o != nil {
		if k.Kind != o.Kind || k.StringID != o.StringID || k.IntID != o.IntID {
			return false
		}
		k, o = k.Parent, o.Parent
	}
	return k == o
}

func (k *Key) Incomplete() bool {
	if k.IntID == -1 || k.IntID == 0 {
		return true
	}
	return k.StringID == ""
}

func (k *Key) Root() *Key {
	for k.Parent != nil {
		k = k.Parent
	}
	return k
}

func main() {
	client := dynamodb.Dial(dynamodb.USWest1, dynamodb.Auth("access", "secret"), nil)
	db := New("wikifactory", client)
	db.Init()
	// db.Counter("units-sold").Add(20)
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

type Keyer interface {
	Key() string
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
