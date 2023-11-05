package main

import (
	"log"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/odit-bit/linkstore"
	postgregraph "github.com/odit-bit/se/graph/linkpostgre"
)

func main() {
	dsn := os.Getenv("DSN")
	if dsn == "" {
		log.Println("DSN var is nil")
		return
	}
	dbConn, err := connectPG(dsn)
	if err != nil {
		log.Fatal(err)
	}
	db := postgregraph.New(dbConn)

	srv := linkstore.Server{
		Port:    8181,
		Handler: db,
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}

	// linkServer := linkstore.NewServer(db)

	// grpcServer := grpc.NewServer()
	// api.RegisterLinkGraphServer(grpcServer, linkServer)

	// listen, err := net.Listen("tcp", ":8181")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// log.Println("listen on :", listen.Addr().String())

	// ctx, cancel := context.WithCancel(context.TODO())
	// defer cancel()

	// sigC := make(chan os.Signal, 1)
	// signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)

	// var wg sync.WaitGroup
	// //server setup
	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	grpcServer.Serve(listen)

	// }()

	// select {
	// case <-ctx.Done():
	// case <-sigC:
	// 	cancel()
	// }

	// grpcServer.GracefulStop()

	// wg.Wait()
	// fmt.Println("rpc server shutdown")
}

func connectPG(dsn string) (*sqlx.DB, error) {
	//IMPORT !!
	// _ "github.com/jackc/pgx/v5/stdlib"

	// DSN format
	// "host=localhost dbname=postgres password=test user=postgres"

	db, err := sqlx.Connect("pgx", dsn)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return db, nil
}
