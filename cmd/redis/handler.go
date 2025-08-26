package main

import "fmt"

// resp command handlers
type HandlerCommand string

const (
	Ping HandlerCommand = "PING"
	Set  HandlerCommand = "SET"
	Get  HandlerCommand = "GET"
	HSet HandlerCommand = "HSET"
	HGet HandlerCommand = "HGET"
)

// resp ack and response
var (
	AckVal  Value = Value{typ: SimpleString, str: "Ok"}
	NullVal Value = Value{typ: Null}
)

// HandlerFunc is the function to execute. only the args received will be passed to it.
type HandlerFunc func([]Value) Value

// handlers are registered here
var handlers = map[HandlerCommand]HandlerFunc{
	Ping: ping,
	Set:  set,
	Get:  get,
	HSet: hSet,
	HGet: hGet,
}

func ping(args []Value) Value {
	return Value{typ: SimpleString, str: "PONG"}
}

// set stores a key value pair. args should be of length 2, key followed by value
func set(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: Error, str: "Err wrong number of arguments for 'SET' command"}
	}

	key := args[0].bulkStr
	val := args[1].bulkStr
	fmt.Printf("key %s value %s\n", key, val)

	// ack operation
	return AckVal
}

// get retrieves the value associated with a given key
func get(args []Value) Value {
	if len(args) < 1 {
		return Value{typ: Error, str: "Err wrong number of arguments for 'GET' command"}
	}

	// retrieve value
	key := args[0].bulkStr
	if key == "" {
		return NullVal
	}
	value := "not set"

	return Value{typ: BulkString, bulkStr: value}
}

// hSet implements the redis HSET command for storing a hashmap entry.
// args will typically be: hash field value[field value ...] (user1 name shabel)
// this implementation is limited to a single field and value for now
func hSet(args []Value) Value {
	if len(args) < 3 {
		return Value{typ: Error, str: "Err wrong number of arguments for 'HSET' command"}
	}

	key := args[0].bulkStr
	field1 := args[1].bulkStr
	val1 := args[2].bulkStr

	fmt.Printf("hash: %s, field1: %s, val1: %s\n", key, field1, val1)
	return AckVal
}

// hGet implements the redis HGET command where the args are of the form:
// hash field (user1 name)
func hGet(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: Error, str: "Err wrong number of arguments for 'HGET' command"}
	}

	key := args[0].bulkStr
	field := args[1].bulkStr

	// check if key exists
	if key == "" {
		return NullVal
	}
	value := "not implemented"

	fmt.Printf("key: %s, field: %s, value: %s", key, field, value)

	return Value{typ: BulkString, bulkStr: value}

}
