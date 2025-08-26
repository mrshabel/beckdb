package main

import (
	"fmt"
	"log"
	"net"
	"strings"
)

func main() {
	// start server and handle connections
	ln, err := net.Listen("tcp", "127.0.0.1:6379")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("server started successfully")

	// accept client connections
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}

		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
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
		handler, ok := handlers[HandlerCommand(command)]
		if !ok {
			fmt.Println("command handler not found: ", command)
			resp.WriteError("Err invalid command type")
			continue
		}
		resp.Write(handler(args))
	}
}
