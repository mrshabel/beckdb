package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	beck "github.com/mrshabel/beckdb"
)

type Server struct {
	db *beck.BeckDB
	ln net.Listener
}

func main() {
	// get configs
	dataDir := flag.String("dir", "", "Data Directory as a valid path")
	syncOnWrite := flag.Bool("sync", false, "Persist each write to disk immediately?")
	readOnly := flag.Bool("read-only", false, "Run db in read-only mode?")
	address := flag.String("addr", "127.0.0.1:6379", "Server address")

	flag.Parse()
	if *dataDir == "" {
		fmt.Println("-dir is required")
		flag.Usage()
		os.Exit(1)
	}

	// setup db
	srv := &Server{}
	db, err := beck.Open(&beck.Config{DataDir: *dataDir, SyncOnWrite: *syncOnWrite, ReadOnly: *readOnly})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	srv.db = db

	// start server and handle connections
	go shutdown(srv)

	ln, err := net.Listen("tcp", *address)
	if err != nil {
		log.Fatal(err)
	}
	srv.ln = ln
	log.Printf("server started successfully on %s\n", *address)

	// accept client connections
	for {
		conn, err := srv.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				log.Println("server stopped accepting connections")
				return
			}
			log.Println("server connection accept error: ", err)
			continue
		}

		go handleConn(conn, srv)
	}
}

func handleConn(conn net.Conn, srv *Server) {
	log.Printf("connection received from client %s\n", conn.RemoteAddr().String())
	defer func() {
		log.Printf("connection closed from client %s\n", conn.RemoteAddr().String())
		conn.Close()
	}()

	// read connection data with the resp parser
	for {
		resp := NewResp(conn)
		data, err := resp.Read()
		if err != nil {
			fmt.Println("Error reading request: ", err)
			return
		}

		// input data should be an array for all commands implemented
		if data.typ != Array {
			resp.WriteError("ERR invalid request payload. expected array")
			continue
		}
		if len(data.array) == 0 {
			resp.WriteError("Err invalid request payload. expected non-empty array")
			continue
		}

		// extract command and args. command is the first entry of the array
		command := strings.ToUpper(data.array[0].bulkStr)
		args := data.array[1:]

		// process request
		res := srv.handleCommand(HandlerCommand(command), args)
		resp.Write(res)
	}
}

func shutdown(srv *Server) {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// wait for shutdown notification
	<-ctx.Done()

	log.Println("shutting down server")
	if err := srv.db.Close(); err != nil {
		log.Printf("error closing database: %v\n", err)
	}

	if err := srv.ln.Close(); err != nil {
		log.Printf("error closing server listener: %v\n", err)
	}
}
