package main

import (
	"github.com/salfield/dynamodb"
	"github.com/tav/ampstore/store"
	"log"
	"os"
)

func StartServers(dbClient *dynamodb.Client) {
	store.CheckTables(dbClient)
	// setup the address Server
	address := "127.0.0.1:1235"
	asTermChan := store.StartServer(address, &store.AddressServer{store.NewBaseServer(dbClient, address)})
	address = "127.0.0.1:1234"
	ssTermChan := store.StartServer(address, &store.StoreServer{store.NewBaseServer(dbClient, address)})
	select {
	case <-ssTermChan:
		log.Printf("store server shutdown")
	case <-asTermChan:
		log.Printf("address server shutdown")
	}
}

func main() {
	log.SetOutput(os.Stderr)
	auth := dynamodb.Auth("your-access-key", "your-secret-key")
	endpoint := dynamodb.EndPoint("DynamoDB Local", "home", "localhost:8000", false)
	dbClient := dynamodb.Dial(endpoint, auth, nil)
	StartServers(dbClient)
}
