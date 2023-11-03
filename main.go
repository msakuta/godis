package main

import (
	"fmt"
	"net"
	"strings"
)

const port = "3737"

func main() {
	fmt.Println("Listening on ", port)
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("e: ", err)
		return
	}

	conns := make(map[string]chan string)
	data := make(map[string]string)
	for {
		conn, err := lis.Accept()
		if err != nil {
			fmt.Println("e: ", err)
			return
		}
		addr := conn.RemoteAddr().String()
		fmt.Println("Connected ", addr)
		ch := make(chan string)
		conns[addr] = ch
		fmt.Printf("Now we have %d connections\n", len(conns))
		go reader(addr, conn, conns, data)
		go writer(conn, ch)
	}
}

func reader(addr string, conn net.Conn, conns map[string]chan string, data map[string]string) {
	for i := 1; i < 30; i++ {
		buf := make([]uint8, 128)
		_, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Read error: ", err)
			return
		}
		fmt.Printf("Client sent: %s\n", buf)

		cmdline := string(buf)
		cmd := strings.Fields(cmdline)
		switch cmd[0] {
		case "GET":
			fmt.Printf("Client get: %s\n", cmd[1])
			value, ok := data[cmd[1]]
			if ok {
				conn.Write([]byte(value))
			} else {
				conn.Write([]byte("$-1\r\n"))
			}
		case "SET":
			fmt.Printf("Client set: %s\n", cmd[1:])
			data[cmd[1]] = cmd[2]
			conn.Write([]byte("+OK\r\n"))
		}
	}
}

func writer(conn net.Conn, a chan string) {
	for true {
		value := <-a
		bytes := []byte(value)
		conn.Write(bytes)
	}
}
