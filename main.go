package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net"
	"time"
)

func listen(addr string, port int, out chan string) {
	bind := fmt.Sprintf("%s:%d", addr, port)
	log.Printf("Listening on %s", bind)
	l, err := net.Listen("tcp", bind)
	if err != nil {
		log.Fatalf("Error listening: %v", err)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatalf("Error accepting: %v", err)
		}
		go handleLog(conn, out)
	}
}

func connect(target string) net.Conn {
	for {
		conn, err := net.DialTimeout("tcp", target, 2*time.Second)
		if err == nil {
			return conn
		}
		log.Printf("Unable to connect to %s: %v", target, err)
		time.Sleep(2 * time.Second)
	}
}

func handleLog(conn net.Conn, out chan string) {
	log.Printf("New connection from %s", conn.RemoteAddr())
	defer conn.Close()

	buf := make([]byte, 4096)
	var stringbuf string
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Printf("Error reading: %v", err)
			break
		}

		lastNewlineIndex := bytes.LastIndexByte(buf[:n], byte('\n'))
		if lastNewlineIndex != -1 {
			//Newline, truncate and send
			stringbuf += string(buf[:lastNewlineIndex+1])
			out <- stringbuf
			stringbuf = string(buf[lastNewlineIndex+1 : n])
		} else {
			//No Newline, append to buffer
			stringbuf += string(buf[:n])
		}
	}
	if stringbuf != "" {
		out <- stringbuf
	}
}

func transmit(outputChan chan string, target string) {
	conn := connect(target)
	var s string
	for {
		s = <-outputChan
		for {
			n, err := conn.Write([]byte(s))
			if err != nil || n == 0 {
				log.Printf("Error writing: %v", err)
				conn = connect(target)
				continue
			}
			if n != len(s) {
				log.Fatalf("Error writing: %d != %d", n, len(s))
			}
			break
		}
	}
}

func receive(addr string, port int, target string, connections int, statsInterval time.Duration) {
	outputChan := make(chan string, connections)
	go listen(addr, port, outputChan)
	for i := 0; i < connections; i++ {
		go transmit(outputChan, target)
	}
	select {}
}

func main() {
	var port int
	var addr string
	var target string
	var connections int
	var interval time.Duration
	flag.StringVar(&addr, "addr", "0.0.0.0", "Address to listen on")
	flag.IntVar(&port, "port", 9000, "Port to listen on")
	flag.StringVar(&target, "target", "127.0.0.1:9999", "Address to proxy to")
	flag.IntVar(&connections, "connections", 16, "Number of outbound connections to make")
	flag.DurationVar(&interval, "interval", time.Second, "Interval between stats")
	flag.Parse()
	receive(addr, port, target, connections, interval)
}
