package main

import (
	"fmt"
	"net"
	"strings"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if nil != err {
		fmt.Println("failed to start server with error.", err)
	}

	defer func(l net.Listener) {
		err := l.Close()
		if err != nil {
			fmt.Println("error while closing listener", err)
		}
	}(l)

	for {
		conn, err := l.Accept()
		if nil != err {
			fmt.Println("failed while accepting connection. ", err)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			fmt.Println("error while closing connection", err)
		}
	}(conn)

	b := make([]byte, 128)
	for {
		size, err := conn.Read(b)
		if nil != err {
			fmt.Println("error while reading from connection.", err)
			return
		}
		if 0 == size {
			break
		}

		if strings.Contains(strings.ToLower(string(b)), "ping") {
			_, err = conn.Write([]byte("+PONG\r\n"))
			if nil != err {
				fmt.Println("error while pinging.", err)
				return
			}
		} else {
			return
		}
	}
}
