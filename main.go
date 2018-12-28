package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

const bufferSize int = 16384

var bufPool = sync.Pool{
	New: func() interface{} {
		buf := new(bytes.Buffer)
		buf.Grow(bufferSize * 2)
		return buf
	},
}

func listen(addr string, port int, out chan *bytes.Buffer) error {
	bind := fmt.Sprintf("%s:%d", addr, port)
	log.Printf("Listening on %s", bind)
	l, err := net.Listen("tcp", bind)
	if err != nil {
		return err
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go handleLog(conn, out)
	}
}

func handleLog(conn net.Conn, out chan *bytes.Buffer) {
	log.Printf("New connection from %s", conn.RemoteAddr())
	defer conn.Close()

	buf := make([]byte, bufferSize)
	var stringbuf *bytes.Buffer
	stringbuf = bufPool.Get().(*bytes.Buffer)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Printf("Error reading: %v", err)
			break
		}

		lastNewlineIndex := bytes.LastIndexByte(buf[:n], byte('\n'))
		if lastNewlineIndex != -1 {
			//Newline, truncate and send
			stringbuf.Write(buf[:lastNewlineIndex+1])
			out <- stringbuf
			stringbuf = bufPool.Get().(*bytes.Buffer)
			stringbuf.Write(buf[lastNewlineIndex+1 : n])
		} else {
			//No Newline, append to buffer
			stringbuf.Write(buf[:n])
		}
	}
	if stringbuf.Len() > 0 {
		out <- stringbuf
	}
}

//connect tries to connect to a target with exponential backoff
func connect(target string, worker int) net.Conn {
	delay := 2 * time.Second
	for {
		//log.Printf("Worker %d: Opening connection to %v", worker, target)
		conn, err := net.DialTimeout("tcp", target, 5*time.Second)
		if err == nil {
			log.Printf("Worker %d: connected to %s", worker, target)
			return conn
		}
		log.Printf("Worker %d: Unable connect to %s: %v", worker, target, err)
		time.Sleep(delay)
		delay *= 2
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
	}
}

func transmit(worker int, outputChan chan *bytes.Buffer, target string) {
	var b *bytes.Buffer

	var conn net.Conn
	conn = connect(target, worker)

	for b = range outputChan {
		n, err := conn.Write(b.Bytes())
		if err != nil || n == 0 {
			log.Printf("Worker %d: Error writing: %v. n=%d, len=%d", worker, err, n, b.Len())
			//requeue this message
			outputChan <- b
			//reconnect
			conn.Close()
			conn = connect(target, worker)
			continue
		}
		//Message succesfully sent.. but...
		//Only return small buffers to the pool
		if b.Cap() <= 1024*1024 {
			b.Reset()
			bufPool.Put(b)
		}
	}
}

func receive(addr string, port int, targets []string, connections int) {
	outputChan := make(chan *bytes.Buffer, connections*len(targets)*2)
	for i := 0; i < connections*len(targets); i++ {
		targetIdx := i % len(targets)
		go transmit(i+1, outputChan, targets[targetIdx])
	}
	err := listen(addr, port, outputChan)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	var port int
	var addr string
	var target string
	var connections int
	flag.StringVar(&addr, "addr", "0.0.0.0", "Address to listen on")
	flag.IntVar(&port, "port", 9000, "Port to listen on")
	flag.StringVar(&target, "target", "127.0.0.1:9999", "Address to proxy to. separate multiple with comma")
	flag.IntVar(&connections, "connections", 16, "Number of outbound connections to make to each target")
	flag.Parse()
	targets := strings.Split(target, ",")
	receive(addr, port, targets, connections)
}
