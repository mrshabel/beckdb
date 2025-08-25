package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

type DataType byte

const (
	String     DataType = '+'
	Error      DataType = '-'
	Integer    DataType = ':'
	Boolean    DataType = '#'
	Array      DataType = '*'
	BulkString DataType = '$'
)

type Token byte

const (
	CR   Token = '\r'
	LF   Token = '\n'
	CRLF Token = CR + LF
)

// errors
var (
	ErrExpectCRLF = errors.New("Err: protocol error. expected CRLF token")
)

// Value holds commands and arguments
// TODO: optimize by recording value as a byte slice and casting type based on value
type Value struct {
	// data type of the value
	typ string
	// string value
	str string
	// number value
	num int
	// bulk string value
	bulkStr string
	// all values received from the array
	array []Value
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(reader io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(reader)}
}

// readLine reads the input stream until the first occurrence of a CRLF token
func (r *Resp) readLine() (line []byte, length int, err error) {
	// read full input stream up to the LF token (\n), from which we can
	// then verify that the CRLF token is the last occurrence read
	line, err = r.reader.ReadBytes(byte(LF))
	if err != nil {
		return nil, 0, err
	}

	if len(line) < 2 || line[len(line)-2] != byte(CR) {
		return nil, 0, ErrExpectCRLF
	}

	// strip crlf token but return full line size
	return line[:len(line)-2], len(line), nil
}

func (r *Resp) readInteger() (num, n int, err error) {
	// read the record line
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	val, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(val), n, nil
}

func (r *Resp) Read() (*Value, error) {
	// get data type
	t, err := r.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	// process data. accepted types are array and bulk strings
	switch DataType(t) {
	case Array:
		return r.readArray()
	case BulkString:
		return r.readBulkString()
	default:
		return nil, fmt.Errorf("Err: protocol error. unknown type %v", string(t))
	}
}

func (r *Resp) readArray() (*Value, error) {
	val := &Value{typ: "array"}

	// get array length
	arrLen, _, err := r.readInteger()
	if err != nil {
		return nil, err
	}

	// process each line recursively
	val.array = make([]Value, arrLen)
	for idx := range arrLen {
		cur, err := r.Read()
		if err != nil {
			return nil, err
		}

		// record parsed value in resp array
		val.array[idx] = *cur
	}
	return val, nil
}

func (r *Resp) readBulkString() (*Value, error) {
	val := &Value{typ: "bulkString"}

	// get string length
	strLen, _, err := r.readInteger()
	if err != nil {
		return nil, err
	}

	// collect string from current reader
	bulkString := make([]byte, strLen)
	r.reader.Read(bulkString)

	val.bulkStr = string(bulkString)

	// consume crlf token
	if _, _, err = r.readLine(); err != nil {
		return nil, err
	}

	return val, nil
}
