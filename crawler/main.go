package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/odit-bit/indexstore"
	"github.com/odit-bit/linkstore"
	"github.com/odit-bit/se/crawler/linkcrawler"
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

	// crawler service
	cr := linkcrawler.New(graphAPI, indexAPI)
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT)

	go func() {
		<-sigC
		cancel()
	}()

	if err := cr.Run(ctx); err != nil {
		log.Println(err)
	}

	log.Println("[crawler service exit]")

}
