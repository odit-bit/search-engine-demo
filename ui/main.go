package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/odit-bit/indexstore"
	"github.com/odit-bit/linkstore"
	"github.com/odit-bit/se/ui/frontend"
)

func main() {
	var linkstoreAddress = os.Getenv("LINKSTORE_SERVER_ADDRESS")
	var indexstoreAddress = os.Getenv("INDEXSTORE_SERVER_ADDRESS")
	if linkstoreAddress == "" || indexstoreAddress == "" {
		log.Fatal("grpc server address is nil")
	}

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	//conect to graph server
	graphAPI, err := linkstore.ConnectGraph(linkstoreAddress)
	if err != nil {
		log.Fatal("failed connect to graph server")
	}
	//connect to index server
	indexAPI, err := indexstore.ConnectIndex(indexstoreAddress)
	if err != nil {
		log.Fatal("failed connect to index server")
	}

	// create frontend instance to server html for user
	ui := frontend.NewDefault(graphAPI, indexAPI)

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT)

	go func() {
		<-sigC
		cancel()
	}()

	if err := ui.Run(ctx); err != nil {
		log.Println(err)
	}

}
