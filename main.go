package main

import (
	"fmt"
	"net"
	"strings"
)

const BufferSize = 128
const MaxReadIterations = 128
const MaxArgumentSize = 128

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if nil != err {
		fmt.Println("failed to start server with error.", err)
	}

	defer func(l net.Listener) {
		err := l.Close()
		if err != nil {
			fmt.Println("error while closing listener.", err)
		}
	}(l)

	for {
		conn, err := l.Accept()
		if nil != err {
			fmt.Println("failed while accepting connection.", err)
		}
		go handleConnection(conn)
	}
}

type DataBuffer struct {
	Buffer      []byte
	Size        int
	ReadPointer int
}

func handleConnection(conn net.Conn) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			fmt.Println("error while closing connection.", err)
		}
	}(conn)

	lastProcessed := true
	dataBuffer := DataBuffer{Buffer: make([]byte, BufferSize)}
	for {
		if lastProcessed {
			dataBuffer.ReadPointer = 0
			dataBuffer.Size = 0
			done := readDataIntoBuffer(conn, &dataBuffer, false)
			if done {
				return
			}
		}

		commandArray, err := readCommand(conn, &dataBuffer)
		if err {
			return
		}

		if processCommand(conn, commandArray) {
			return
		}

		if dataBuffer.ReadPointer < dataBuffer.Size {
			lastProcessed = false
			dataBuffer.Buffer = dataBuffer.Buffer[dataBuffer.ReadPointer:]
			dataBuffer.Size -= dataBuffer.ReadPointer
			dataBuffer.ReadPointer = 0
		}
	}
}

// read more data into buffer
func readDataIntoBuffer(conn net.Conn, dataBuffer *DataBuffer, appendToBuffer bool) bool {
	tempBuffer := dataBuffer.Buffer
	if appendToBuffer {
		tempBuffer = make([]byte, BufferSize)
	}

	size, err := conn.Read(tempBuffer)
	if nil != err {
		fmt.Println("error while reading from connection.", err)
		return true
	}

	if size > 0 && appendToBuffer {
		dataBuffer.Buffer = append(dataBuffer.Buffer, tempBuffer...)
	}

	dataBuffer.Size += size
	return false
}

// process/reply to command
func processCommand(conn net.Conn, cmdArray []string) bool {
	if strings.ToLower(cmdArray[0]) == "ping" {
		_, err := conn.Write([]byte("+PONG\r\n"))
		if nil != err {
			fmt.Println("error while pinging.", err)
			return true
		}
	} else {
		_, err := conn.Write([]byte("-1\r\n"))
		if nil != err {
			fmt.Println("error while pinging.", err)
			return true
		}
	}
	return false
}

// return one command and it's arguments
func readCommand(conn net.Conn, dataBuffer *DataBuffer) ([]string, bool) {
	arraySize, err := getDataSize(conn, dataBuffer)
	if err || 0 == arraySize || arraySize > MaxArgumentSize {
		return nil, true
	}

	cmdArray := make([]string, arraySize)
	for arrayCounter := 0; arrayCounter < arraySize; arrayCounter++ {
		argSize, getDataSizeErr := getDataSize(conn, dataBuffer)
		if getDataSizeErr || 0 == argSize {
			return nil, true
		}

		for iter := 0; iter <= MaxReadIterations; iter++ {
			if dataBuffer.Size >= dataBuffer.ReadPointer+argSize+2 {
				break
			}
			err = readDataIntoBuffer(conn, dataBuffer, true)
			if err {
				return nil, true
			}
		}
		cmdArray[arrayCounter] = string((dataBuffer.Buffer)[dataBuffer.ReadPointer : dataBuffer.ReadPointer+argSize])
		dataBuffer.ReadPointer += argSize + 2
	}
	return cmdArray, false
}

// calculate the size of next datatype at readPointer
func getDataSize(conn net.Conn, dataBuffer *DataBuffer) (int, bool) {
	argSize, nextPointer, comp := parseDataSize((dataBuffer.Buffer)[dataBuffer.ReadPointer:], dataBuffer.Size)
	if comp {
		dataBuffer.ReadPointer += nextPointer
	} else {
		for iter := 0; iter < MaxReadIterations; iter++ {
			tempBuffer := make([]byte, BufferSize)
			size, err := conn.Read(tempBuffer)
			if nil != err {
				fmt.Println("error while reading from connection.", err)
				return 0, true
			}
			dataBuffer.Size += size
			dataBuffer.Buffer = append(dataBuffer.Buffer, tempBuffer...)
			argSize, nextPointer, comp = parseDataSize((dataBuffer.Buffer)[dataBuffer.ReadPointer:], dataBuffer.Size)
			if comp {
				dataBuffer.ReadPointer += nextPointer
				break
			}
		}
	}
	return argSize, false
}

// takes byte array(Buffer) and Size of Buffer to read
// returns number parsed, offset to end of number in byte array, whether the Buffer is enough to parse number
func parseDataSize(b []byte, sz int) (int, int, bool) {
	if sz < 2 {
		return 0, 0, false
	}
	if '+' != b[0] && '-' != b[0] && ':' != b[0] && '*' != b[0] && '$' != b[0] {
		return 0, 0, true
	}

	offset := 1
	number := 0
	complete := false
	for ; offset < sz; offset++ {
		if '\r' == b[offset] {
			complete = true
			break
		}
		number = (number * 10) + int(b[offset]-'0')
	}
	if offset+1 >= sz || '\n' != b[offset+1] {
		return number, offset, false
	}
	return number, offset + 2, complete
}
