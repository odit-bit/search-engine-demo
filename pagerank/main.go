package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/odit-bit/indexstore"
	"github.com/odit-bit/linkstore"
)

func main() {
	linkstoreAddress := os.Getenv("LINKSTORE_SERVER_ADDRESS")
	indexstoreAddress := os.Getenv("INDEXSTORE_SERVER_ADDRESS")
	if linkstoreAddress == "" || indexstoreAddress == "" {
		log.Fatal("grpc server address is nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	graphAPI, err := linkstore.ConnectGraph(linkstoreAddress)
	if err != nil {
		log.Fatal("failed connect to graph server")
	}

	indexAPI, err := indexstore.ConnectIndex(indexstoreAddress)
	if err != nil {
		log.Fatal("failed connect to index server")
	}

	//instance with default config
	srv := New(graphAPI, indexAPI)
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT)

	go func() {
		<-sigC
		cancel()
	}()

	if err := srv.Run(ctx); err != nil {
		log.Println(err)
	}

	log.Println("[crawler service exit]")

}
