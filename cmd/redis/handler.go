package main

import (
	"errors"
	"fmt"

	beck "github.com/mrshabel/beckdb"
)

// resp command handlers
type HandlerCommand string

const (
	Ping HandlerCommand = "PING"
	Set  HandlerCommand = "SET"
	Get  HandlerCommand = "GET"
	Del  HandlerCommand = "DEL"
	HSet HandlerCommand = "HSET"
	HGet HandlerCommand = "HGET"
	HDel HandlerCommand = "HDEL"
)

// resp ack and response
var (
	AckVal      Value = Value{typ: SimpleString, str: "Ok"}
	NullVal     Value = Value{typ: Null}
	HSetCreated Value = Value{typ: Integer, num: 1}
	HSetUpdated Value = Value{typ: Integer, num: 0}
	HSetNoOp    Value = Value{typ: Integer, num: 0}
)

// HandlerFunc is the function to execute. only the args received will be passed to it.
type HandlerFunc func([]Value) Value

func (s *Server) ping(args []Value) Value {
	return Value{typ: SimpleString, str: "PONG"}
}

// set stores a key value pair. args should be of length 2, key followed by value
func (s *Server) set(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: Error, str: "Err wrong number of arguments for 'SET' command"}
	}

	key := args[0].bulkStr
	val := args[1].bulkStr

	// write to db
	if err := s.db.Put(key, []byte(val)); err != nil {
		return Value{typ: Error, str: err.Error()}
	}

	// ack operation
	return AckVal
}

// get retrieves the value associated with a given key
func (s *Server) get(args []Value) Value {
	if len(args) < 1 {
		return Value{typ: Error, str: "Err wrong number of arguments for 'GET' command"}
	}

	key := args[0].bulkStr

	// retrieve value
	val, err := s.db.Get(key)
	if err != nil {
		return NullVal
	}

	return Value{typ: BulkString, bulkStr: string(val)}
}

// del deletes an entry with a given key
func (s *Server) del(args []Value) Value {
	if len(args) < 1 {
		return Value{typ: Error, str: "Err wrong number of arguments for 'GET' command"}
	}

	key := args[0].bulkStr

	// retrieve value
	if err := s.db.Delete(key); err != nil {
		return NullVal
	}

	return AckVal
}

// hSet implements the redis HSET command for storing a hashmap entry.
// args will typically be: hash field value[field value ...] (user1 name shabel)
// this implementation is limited to a single field and value for now
func (s *Server) hSet(args []Value) Value {
	if len(args) < 3 {
		return Value{typ: Error, str: "Err wrong number of arguments for 'HSET' command"}
	}

	hashStr := args[0].bulkStr
	field := args[1].bulkStr
	value := args[2].bulkStr

	// compose key as hash:field
	key := getHashKey(hashStr, field)

	// write to db
	if err := s.db.Put(key, []byte(value)); err != nil {
		return Value{typ: Error, str: err.Error()}
	}

	return HSetCreated
}

// hGet implements the redis HGET command where the args are of the form:
// hash field (user1 name)
func (s *Server) hGet(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: Error, str: "Err wrong number of arguments for 'HGET' command"}
	}

	hashStr := args[0].bulkStr
	field := args[1].bulkStr
	key := getHashKey(hashStr, field)

	val, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, beck.ErrKeyNotFound) {
			return NullVal
		}

		return Value{typ: Error, str: "Err " + err.Error()}
	}

	return Value{typ: BulkString, bulkStr: string(val)}
}

// hDel implements the redis HDEL command where the args are of the form:
// hash field. Only a single key deletion is supported now
func (s *Server) hDel(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: Error, str: "Err wrong number of arguments for 'HGET' command"}
	}

	hashStr := args[0].bulkStr
	field := args[1].bulkStr
	key := getHashKey(hashStr, field)

	if err := s.db.Delete(key); err != nil {
		return HSetNoOp
	}

	return HSetCreated
}

// handleCommand acts as the route handler for the request
func (s *Server) handleCommand(command HandlerCommand, args []Value) Value {
	switch command {
	case Ping:
		return s.ping(args)
	case Set:
		return s.set(args)
	case Get:
		return s.get(args)
	case Del:
		return s.del(args)
	case HSet:
		return s.hSet(args)
	case HGet:
		return s.hGet(args)
	case HDel:
		return s.hDel(args)
	default:
		fmt.Println("command handler not found: ", command)
		return Value{typ: Error, str: "Err invalid command type"}
	}
}

func getHashKey(hashStr, field string) string {
	return fmt.Sprintf("%s:%s", hashStr, field)
}
