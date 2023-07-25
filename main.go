package redisClone

import (
	"fmt"
	"net"
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
}
