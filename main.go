package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
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

func receive(conn net.Conn, out chan *bytes.Buffer) {
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

type Worker struct {
	id            int
	targets       []string //The list of all available targets
	target        string   //The currently used target
	targetIdx     int      //The index of the default target this worker should be using
	conn          net.Conn
	lastReconnect time.Time
}

func (w Worker) String() string {
	return fmt.Sprintf("worker-%02d", w.id)
}

func (w Worker) isConnectedToPrimary() bool {
	return w.target == w.targets[w.targetIdx]
}

//connect tries to connect to a target with exponential backoff
func (w *Worker) ConnectWithRetries(ctx context.Context) error {
	rand.Seed(time.Now().UnixNano())
	delay := 2 * time.Second
	targetIdx := w.targetIdx //Leave the desired one alone
	for {
		w.target = w.targets[targetIdx]
		//log.Printf("Worker %d: Opening connection to %v", w.id, w.target)
		conn, err := net.DialTimeout("tcp", w.target, 5*time.Second)
		if err == nil {
			log.Printf("Worker %d: connected to %s", w.id, w.target)
			w.conn = conn
			w.lastReconnect = time.Now()
			return nil
		}
		log.Printf("Worker %d: Unable connect to %s: %v", w.id, w.target, err)
		//The context is done
		if ctx.Err() != nil {
			return err
		}
		time.Sleep(delay)
		delay *= 2
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
		//After a failure, move onto a random target
		targetIdx = rand.Intn(len(w.targets))
	}
}

func (w *Worker) ConnectIfNeeded(ctx context.Context) error {
	if w.conn == nil {
		return w.ConnectWithRetries(ctx)
	}
	//If not connected to the desired target, try reconnecting if it's been 5 minutes
	if !w.isConnectedToPrimary() && time.Since(w.lastReconnect) > 5*time.Minute {
		return w.ConnectWithRetries(ctx)
	}
	return nil
}
func (w *Worker) Close() {
	if w.conn != nil {
		w.conn.Close()
		w.conn = nil
	}
}

func (w *Worker) Reconnect(ctx context.Context) {
	w.Close()
	w.ConnectWithRetries(ctx)
}
func (w *Worker) Write(b []byte) (int, error) {
	w.conn.SetDeadline(time.Now().Add(30 * time.Second))
	n, err := w.conn.Write(b)
	return n, err
}
func (w *Worker) WriteWithRetries(ctx context.Context, b []byte) (int, error) {
	for {
		w.ConnectIfNeeded(ctx)
		n, err := w.Write(b)
		if err == nil {
			return n, err
		}
		log.Printf("Worker %d: Error writing to %s: %v. n=%d, len=%d", w.id, w.target, err, n, len(b))
		w.Close()
	}
}

func transmit(ctx context.Context, worker int, outputChan chan *bytes.Buffer, targets []string, target int) {
	var b *bytes.Buffer

	w := &Worker{
		id:        worker,
		targets:   targets,
		targetIdx: target,
	}
	err := w.ConnectWithRetries(ctx)
	//Only happens if we are exiting during startup
	if err != nil {
		return
	}
	var exit bool

	doneChan := ctx.Done()

	idleCount := 0
	timer := time.NewTicker(1 * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			idleCount++
			//Exit if we are done and have not received any logs to write in 5 ticks.
			if exit && idleCount >= 5 {
				w.Close()
				return
			}
		case <-doneChan:
			log.Printf("Worker %d: draining records and exiting...", worker)
			exit = true
			doneChan = nil
		case b = <-outputChan:
			idleCount = 0
			//This will retry forever and will not fail
			w.WriteWithRetries(context.TODO(), b.Bytes())
			//Message succesfully sent.. but...
			//Only return small buffers to the pool
			if b.Cap() <= 1024*1024 {
				b.Reset()
				bufPool.Put(b)
			}
		}
	}
}
func proxy(ctx context.Context, l net.Listener, targets []string, connections int) error {
	outputChan := make(chan *bytes.Buffer, connections*len(targets)*2)
	var wg sync.WaitGroup
	for i := 0; i < connections*len(targets); i++ {
		wg.Add(1)
		go func(idx int) {
			targetIdx := idx % len(targets)
			transmit(ctx, idx+1, outputChan, targets, targetIdx)
			log.Printf("Worker %d done", idx+1)
			wg.Done()
		}(i)
	}
	go func() {
		<-ctx.Done()
		l.Close()
	}()
	var err error
	for {
		conn, err := l.Accept()
		if err != nil {
			break
		}
		go receive(conn, outputChan)
	}
	//Wait for all workers to exit
	wg.Wait()
	return err
}

func listenAndProxy(addr string, port int, targets []string, connections int) error {
	ctx, cancel := context.WithCancel(context.Background())
	bind := fmt.Sprintf("%s:%d", addr, port)
	log.Printf("Listening on %s", bind)

	l, err := net.Listen("tcp", bind)
	if err != nil {
		return err
	}
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Printf("Received signal %s, exiting", sig)
		cancel()
	}()

	return proxy(ctx, l, targets, connections)
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
	err := listenAndProxy(addr, port, targets, connections)
	if err != nil {
		log.Fatal(err)
	}
}
