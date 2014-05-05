package store

import (
	"github.com/salfield/dynamodb"
	"log"
	"net/rpc"
	"os"
	"testing"
)

func TestClient(t *testing.T) {
	log.SetOutput(os.Stderr)
	auth := dynamodb.Auth("your-access-key", "your-secret-key")
	endpoint := dynamodb.EndPoint("DynamoDB Local", "home", "localhost:8000", false)
	dbClient := dynamodb.Dial(endpoint, auth, nil)
	address := "127.0.0.1:1234"
	ssTermChan := StartServer(address, &StoreServer{NewBaseServer(dbClient, address)})
	//dbClient.UpdateTable("CommitMeta", 30, 20, nil)

	client, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	k := []byte("hello")
	v := []byte("world")
	start := []byte(":")
	putArgs := &PutItem{k, v, start}
	var ret int
	err = client.Call("StoreServer.Put", putArgs, &ret)
	if err != nil {
		log.Fatalf("Error Putting key: %v\n", err) //string(newV))
	}
	log.Printf("Item is put")

	var newV []byte
	getArgs := &GetItem{[]byte("hello"), start}
	if err != nil {
		log.Fatalf("Error Putting key: %v\n", err) //string(newV))
	}
	err = client.Call("StoreServer.Get", getArgs, &newV)
	log.Printf("test%v\n", err) //string(newV))
	// assert
	_ = ssTermChan
	/*
		select {
		case <-ssTermChan:
			log.Printf("store server shutdown")
		case <-asTermChan:
			log.Printf("address server shutdown")
		} */
}
