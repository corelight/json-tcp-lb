package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"testing"
	"time"
)

func listenTestConnection(t *testing.T, l net.Listener, connections int) (int, error) {
	resultChan := make(chan int, connections)

	var lines int
	for i := 0; i < connections; i++ {
		conn, err := l.Accept()
		if err != nil {
			return 0, err
		}
		log.Printf("Accepted connection %d of %d", i+1, connections)
		go handleTestConnection(t, conn, resultChan)
	}
	for i := 0; i < connections; i++ {
		lines += <-resultChan
	}
	return lines, nil
}

func handleTestConnection(t *testing.T, conn net.Conn, resultChan chan int) {
	var lines int
	log.Printf("New connection from %s", conn.RemoteAddr())
	defer conn.Close()
	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Printf("Error reading: %v", err)
			break
		}
		lines += bytes.Count(buf[:n], []byte("\n"))
	}
	log.Printf("Connection got %d lines", lines)
	resultChan <- lines
}

const logLine = `{"_path":"conn","_system_name":"HQ","_write_ts":"2018-11-28T04:50:48.848281Z","ts":"2018-11-28T04:50:38.834880Z","uid":"CX6jut3BmNwFdYkgrk","id.orig_h":"fc00::165","id.orig_p":44206,"id.resp_h":"fc00::1","id.resp_p":53,"proto":"udp","service":"dns","duration":0.004537,"orig_bytes":55,"resp_bytes":55,"conn_state":"SF","local_orig":false,"local_resp":false,"missed_bytes":0,"history":"Dd","orig_pkts":1,"orig_ip_bytes":103,"resp_pkts":1,"resp_ip_bytes":103,"tunnel_parents":[],"corelight_shunted":false,"orig_l2_addr":"ac:1f:6b:00:81:9a","resp_l2_addr":"b4:75:0e:08:08:c1"}`

func spew(t *testing.T, port int, lines int) {
	line := []byte(logLine + "\n")
	target := fmt.Sprintf("localhost:%d", port)
	conn, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		log.Printf("Unable to connect to %s: %v", target, err)
		t.Fatal(err)
	}
	defer conn.Close()
	log.Printf("Spewing %d lines to port %d", lines, port)
	for i := 0; i < lines; i++ {
		_, err := conn.Write(line)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDirect(t *testing.T) {
	connections := 8
	linesPerConnnection := 10000
	expected := connections * linesPerConnnection
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	log.Printf("Listening for tests on %d", port)
	defer l.Close()
	for i := 0; i < connections; i++ {
		go spew(t, port, linesPerConnnection)
	}
	lines, err := listenTestConnection(t, l, connections)
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("Got %d lines total", lines)
	if lines != expected {
		t.Errorf("Expected %d lines, got %d", expected, lines)
	}
}

func TestProxy(t *testing.T) {
	connections := 8
	linesPerConnnection := 10000
	expected := connections * linesPerConnnection

	// Setup downstream listener
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	log.Printf("Listening for tests on %d", port)
	defer l.Close()
	target := fmt.Sprintf("localhost:%d", port)
	targets := []string{target}

	// Setup proxy listener
	ctx, cancel := context.WithCancel(context.Background())
	pl, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	proxyPort := pl.Addr().(*net.TCPAddr).Port
	log.Printf("Listening for proxy on %d", proxyPort)
	defer pl.Close()
	//

	go proxy(ctx, pl, targets, 8)

	//Spew everything and then close the proxy listener
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < connections; i++ {
			wg.Add(1)
			go func() {
				spew(t, proxyPort, linesPerConnnection)
				log.Printf("Spew done")
				wg.Done()
			}()
		}
		wg.Wait()
		log.Printf("All spew done")
		cancel()
	}()

	lines, err := listenTestConnection(t, l, connections)
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("Got %d lines total", lines)
	if lines != expected {
		t.Errorf("Expected %d lines, got %d", expected, lines)
	}
}
