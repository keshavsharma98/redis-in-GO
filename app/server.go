package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	db := make(map[string]string)
	mu := sync.Mutex{}

	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleRead(conn, &db, &mu)
	}
}

func handleRead(conn net.Conn, db *map[string]string, mu *sync.Mutex) {
	defer conn.Close()

	readBytes := make([]byte, 1024)
	for {
		n, err := conn.Read(readBytes)
		if err != nil {
			fmt.Println("Error while reading the data: ", err.Error())
			return
		}
		receivedData := strings.TrimSpace(string(readBytes[:n]))
		// fmt.Println("Received data:", receivedData)
		c, a := parseRedisCmd(receivedData)
		if c == "ping" {
			_, err := conn.Write([]byte("+PONG\r\n"))
			if err != nil {
				fmt.Println("Error while writing the data: ", err.Error())
			}
		} else if c == "echo" {
			_, err := conn.Write([]byte("+" + a + "\r\n"))
			if err != nil {
				fmt.Println("Error while writing the data: ", err.Error())
			}
		} else if c == "set" {
			status := set(a, db, mu)
			_, err := conn.Write([]byte("+" + status + "\r\n"))
			if err != nil {
				fmt.Println("Error while writing the data: ", err.Error())
			}
		} else if c == "get" {
			v := get(a, db, mu)
			_, err := conn.Write([]byte(v))
			if err != nil {
				fmt.Println("Error while writing the data: ", err.Error())
			}
		} else {
			_, err := conn.Write([]byte("+command not found\r\n"))
			if err != nil {
				fmt.Println("Error while writing the data: ", err.Error())
			}
		}
	}
}

func parseRedisCmd(c string) (string, string) {
	parts := strings.Split(c, "\r\n")
	if len(parts) < 2 {
		return "", ""
	}
	cmd := strings.ToLower(parts[2])

	var args strings.Builder
	for i, e := range parts[3:] {
		if string(e[0]) != "$" {
			args.WriteString(e)
			if i != len(parts[3:])-1 {
				args.WriteByte(' ')
			}
		}
	}
	return cmd, args.String()
}

func set(pair string, db *map[string]string, mu *sync.Mutex) string {
	mu.Lock()
	defer mu.Unlock()
	p := strings.Split(pair, " ")
	if len(p) > 2 {
		if p[2] != "px" {
			return "invalid argument px"
		}
		(*db)[p[0]] = p[1]
		go expireDeletion(p[3], p[0], db, mu)
		return "OK"

	} else {
		(*db)[p[0]] = p[1]
	}
	return "OK"
}

func get(k string, db *map[string]string, mu *sync.Mutex) string {
	mu.Lock()
	defer mu.Unlock()
	v, ok := (*db)[k]
	if !ok {
		return "$-1\r\n"
	}
	return ("+" + v + "\r\n")
}

func expireDeletion(t, key string, db *map[string]string, mu *sync.Mutex) {
	dur, _ := strconv.Atoi(t)

	time.AfterFunc(time.Millisecond*time.Duration(dur), func() {
		mu.Lock()
		defer mu.Unlock()
		delete(*db, key)
	})
}
