// Public Domain (-) 2013 The Protostore Authors.
// See the Protostore UNLICENSE file for details.

// package protostore
package main

import (
	"github.com/tav/golly/dynamodb"
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
	db.client.Call("CreateTable", db.name+"-indexes")
	db.client.Call("CreateTable", db.name+"-items")
	db.client.Call("CreateTable", db.name+"-roots")
	db.client.Call("CreateTable", db.name+"-counters")
}

func (db *DB) Get(key string) *Response {
	err := db.client.Call("GetItem", `{"TableName": $Name-items, ...}`)
	if err != nil {
		return err
	}
	decodeInto(value)
	return nil
}

func (db *DB) Put(key string, value interface{}) error {
	if db.txn {

	}
}

func (db *DB) Delete(key string) error {
}

type Query struct {
}

func (db *DB) Query() *Query {
	return &Query{}
}

func (db *DB) Counter(name string) *Counter {
	return *Counter{
		db:   db,
		name: name,
	}
}

func (db *DB) Transact(handler func(*protostore.DB) error) error {
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
}

func (c *Counter) Decr() error {
}

func (c *Counter) Add(n int) error {
}

func (c *Counter) Subtract(n int) error {
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
		Name:   name,
		Client: client,
	}
}

func main() {
	client := dynamodb.Dial(dynamodb.USWest1, dynamodb.Auth("access", "secret"), nil)
	db := protostore.New("wikifactory", client)
	db.Init()
	db.Counter("units-sold").Add(20)
	db.Transact(func(db *protostore.DB) error {
		o := &Order{}
		err := db.Get("foo", o)
		if err != nil {
			return err
		}
		if o.CustomerID < 1000 {
			w := &Winner{Customer: o.CustomerID}
			db.Put("winner", w)
		}
		return nil
	})
}
