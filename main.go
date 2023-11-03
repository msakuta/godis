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
	subscribers := make(map[string]chan string)
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
		go reader(addr, conn, conns, data, &subscribers)
		go writer(conn, ch)
	}
}

func reader(addr string, conn net.Conn, conns map[string]chan string, data map[string]string, subscribers *map[string]chan string) {
	for {
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
		case "PUBLISH":
			fmt.Printf("Client publish[%d]: %s\n", len(cmd), cmd[1:])
			if len(cmd) < 4 {
				conn.Write([]byte("-PUBLISH requires 2 args\r\n"))
				continue
			}
			sub, ok := (*subscribers)[cmd[1]]
			if ok {
				sub <- cmd[2]
			} else {
				fmt.Printf("WARNING: a topic %s doesn't have a subscriber\n", cmd[1])
			}
		case "SUBSCRIBE":
			fmt.Printf("Client subscribe[%d]: %s\n", len(cmd), cmd[1:])
			if len(cmd) < 3 {
				conn.Write([]byte("-SUBSCRIBE requires 1 arg\r\n"))
				continue
			}
			ch := make(chan string)
			(*subscribers)[cmd[1]] = ch
			conn.Write([]byte("+OK\r\n"))
			fmt.Printf("The topic %s has %d subscribers\n", cmd[1], len(*subscribers))
			subscribeLoop(conn, ch)
			delete(*subscribers, cmd[1])
		}
	}
}

// Subscribe to the topic and respond. The commands are ignored except unsubscribe.
func subscribeLoop(conn net.Conn, ch chan string) {
	unsub := make(chan struct{})
	go (func() {
		buf := make([]uint8, 128)
		for {
			_, err := conn.Read(buf)
			if err != nil {
				fmt.Printf("Read error in waiting subscription: %s\n", err)
				continue
			}
			cmd := strings.Fields(string(buf))
			switch cmd[0] {
			case "UNSUBSCRIBE":
				fmt.Printf("Client unsubscribed\n")
				unsub <- struct{}{}
				return
			default:
				fmt.Printf("Unrecognized command: %s\n", cmd)
			}
		}
	})()

	for {
		select {
		case s := <-ch:
			conn.Write([]byte(fmt.Sprintf("%s\r\n", s)))
		case <-unsub:
			conn.Write([]byte("+OK\r\n"))
			return
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
