// reference: https://redis.io/docs/latest/develop/reference/protocol-spec/#resp-versions
package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

type DataType string

const (
	Array        DataType = "array"
	SimpleString DataType = "string"
	BulkString   DataType = "bulkString"
	Null         DataType = "null"
	Error        DataType = "error"
)

// resp single byte prefix for data type
type Prefix byte

const (
	PrefixSimpleString Prefix = '+'
	PrefixError        Prefix = '-'
	PrefixInteger      Prefix = ':'
	PrefixBoolean      Prefix = '#'
	PrefixArray        Prefix = '*'
	PrefixBulkString   Prefix = '$'
)

type Token byte

const (
	CR Token = '\r'
	LF Token = '\n'
)

var CRLF = []byte{byte(CR), byte(LF)}

// errors
var (
	ErrExpectCRLF = errors.New("err: protocol error. expected CRLF token")
)

type Resp struct {
	reader *bufio.Reader
	writer io.Writer
}

func NewResp(rw io.ReadWriter) *Resp {
	return &Resp{
		reader: bufio.NewReader(rw),
		writer: rw,
	}
}

// Read extracts the value from the resp instance
func (r *Resp) Read() (*Value, error) {
	// get data type
	t, err := r.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	// process data. accepted types are array and bulk strings
	switch Prefix(t) {
	case PrefixArray:
		return r.readArray()
	case PrefixBulkString:
		return r.readBulkString()
	default:
		return nil, fmt.Errorf("err: protocol error. unknown type %v", string(t))
	}
}

// Write writes the resp value to the underlying writer. this may typically be a response
func (r *Resp) Write(v Value) error {
	data := v.Marshal()
	_, err := r.writer.Write(data)
	return err
}

// WriteError sends an error reply to the client
func (r *Resp) WriteError(msg string) error {
	return r.Write(Value{typ: Error, str: msg})
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

func (r *Resp) readArray() (*Value, error) {
	val := &Value{typ: Array}

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
	val := &Value{typ: BulkString}

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

// Value holds commands and arguments
// TODO: optimize by recording value as a byte slice and casting type based on value
type Value struct {
	// data type of the value
	typ DataType
	// string value
	str string
	// number value
	num int
	// bulk string value
	bulkStr string
	// all values received from the array
	array []Value
}

// convert value into resp bytes. all marshalled data is suffixed with CRLF token
// <prefix sign - data - crlf>
func (v *Value) Marshal() []byte {
	switch v.typ {
	case SimpleString:
		return v.marshalSimpleString()
	case BulkString:
		return v.marshalBulkString()
	case Array:
		return v.marshalArray()
	case Null:
		return v.marshalNullBulkString()
	case Error:
		return v.marshalError()
	default:
		return []byte{}
	}
}

func (v *Value) marshalSimpleString() []byte {
	var data []byte
	// sign, followed by string then crlf
	data = append(data, byte(PrefixSimpleString))
	data = append(data, v.str...)
	data = append(data, CRLF...)
	return data
}

func (v *Value) marshalBulkString() []byte {
	var data []byte
	length := strconv.Itoa(len(v.bulkStr))
	// sign, length, crlf, bulk string then crlf
	data = append(data, byte(PrefixBulkString))
	data = append(data, []byte(length)...)
	data = append(data, CRLF...)
	data = append(data, []byte(v.bulkStr)...)
	data = append(data, CRLF...)

	return data
}

func (v *Value) marshalArray() []byte {
	var data []byte
	length := strconv.Itoa(len(v.array))
	// sign, length of array, crlf, elements1...elementN
	data = append(data, byte(PrefixArray))
	data = append(data, []byte(length)...)
	data = append(data, CRLF...)

	// append elements one by one
	for _, val := range v.array {
		// since type is unknown, the parent marshal function is called
		data = append(data, val.Marshal()...)
	}

	return data
}

// marshal a resp empty bulk string
func (v *Value) marshalNullBulkString() []byte {
	// sign, followed by-1 then crlf
	return []byte("$-1\r\n")
}

// marshal a resp simple error
func (v *Value) marshalError() []byte {
	var data []byte
	// sign, followed by error string then crlf
	data = append(data, byte(PrefixError))
	data = append(data, v.str...)
	data = append(data, CRLF...)
	return data
}

type Writer struct {
	writer io.Writer
}
