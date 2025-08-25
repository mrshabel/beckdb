package main

import (
	"fmt"
	"log"
	"net"
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
			fmt.Println(err)
			return
		}

		log.Printf("client data read: %v\n", *data)
		// write pong response
		conn.Write([]byte("+OK\r\n"))
	}
}
