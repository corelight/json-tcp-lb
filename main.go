package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
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

type Config struct {
	// Basic Settings
	Addr        string
	Port        int
	Connections int
	Targets     []string

	// Listen using TLS
	CertFile string
	KeyFile  string

	// Connect using TLS
	TargetTLS           bool
	TargetTLSSkipVerify bool
}

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
			if errors.Is(err, net.ErrClosed) {
				log.Printf("Closed inbound connection from %s", conn.RemoteAddr())
			} else {
				log.Printf("Error reading from %s: %v", conn.RemoteAddr(), err)
			}
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
	cfg           Config
}

func (w Worker) String() string {
	return fmt.Sprintf("worker-%02d", w.id)
}

func (w Worker) isConnectedToPrimary() bool {
	return w.target == w.targets[w.targetIdx]
}
func (w *Worker) Connect(ctx context.Context, target string) (net.Conn, error) {
	var conn net.Conn
	var err error
	if !w.cfg.TargetTLS {
		conn, err = net.DialTimeout("tcp", target, 5*time.Second)
	} else {
		conf := &tls.Config{}
		if w.cfg.TargetTLSSkipVerify {
			conf.InsecureSkipVerify = true
		}
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", target, conf)
	}
	return conn, err
}

// ConnectWithRetries tries to connect to a target with exponential backoff
func (w *Worker) ConnectWithRetries(ctx context.Context) error {
	rand.Seed(time.Now().UnixNano())
	delay := 2 * time.Second
	targetIdx := w.targetIdx //Leave the desired one alone
	for {
		w.target = w.targets[targetIdx]
		conn, err := w.Connect(ctx, w.target)
		//log.Printf("Worker %d: Opening connection to %v", w.id, w.target)
		if err == nil {
			w.Close()
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
		log.Printf("Worker %d: attempting to reconnect to primary target", w.id)
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

func transmit(ctx context.Context, cfg Config, worker int, outputChan chan *bytes.Buffer, targets []string, target int) {
	var b *bytes.Buffer

	w := &Worker{
		cfg:       cfg,
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
func proxy(ctx context.Context, l net.Listener, cfg Config) error {
	numTargets := len(cfg.Targets)
	outputChan := make(chan *bytes.Buffer, cfg.Connections*numTargets*2)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Connections*numTargets; i++ {
		wg.Add(1)
		go func(idx int) {
			targetIdx := idx % numTargets
			transmit(ctx, cfg, idx+1, outputChan, cfg.Targets, targetIdx)
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
		go func() {
			<-ctx.Done()
			conn.Close()
		}()
		go receive(conn, outputChan)
	}
	//Wait for all workers to exit
	wg.Wait()
	return err
}

func listen(cfg Config) (net.Listener, error) {
	bind := fmt.Sprintf("%s:%d", cfg.Addr, cfg.Port)

	var l net.Listener
	var err error

	if cfg.CertFile == "" || cfg.KeyFile == "" {
		log.Printf("Listening on %s", bind)
		l, err = net.Listen("tcp", bind)
	} else {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			log.Fatal(err)
		}
		config := &tls.Config{Certificates: []tls.Certificate{cert}}

		log.Printf("listening on %s using TLS", bind)
		l, err = tls.Listen("tcp", bind, config)
	}
	return l, err
}

func listenAndProxy(cfg Config) error {
	ctx, cancel := context.WithCancel(context.Background())
	l, err := listen(cfg)
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

	return proxy(ctx, l, cfg)
}

func main() {
	var cfg Config
	var targets string
	flag.StringVar(&cfg.Addr, "addr", "0.0.0.0", "Address to listen on")
	flag.IntVar(&cfg.Port, "port", 9000, "Port to listen on")
	flag.StringVar(&targets, "target", "127.0.0.1:9999", "Address to proxy to. separate multiple with comma")
	flag.BoolVar(&cfg.TargetTLS, "tls-target", false, "Connect to the targets using TLS")
	flag.BoolVar(&cfg.TargetTLSSkipVerify, "tls-target-skip-verify", false, "Accepts any certificate presented by the target")
	flag.IntVar(&cfg.Connections, "connections", 4, "Number of outbound connections to make to each target")
	flag.StringVar(&cfg.CertFile, "tls-cert", "", "TLS Certificate PEM file.  Configuring this enables TLS")
	flag.StringVar(&cfg.KeyFile, "tls-key", "", "TLS Certificate Key PEM file")
	flag.Parse()
	cfg.Targets = strings.Split(targets, ",")
	err := listenAndProxy(cfg)
	if err != nil {
		log.Fatal(err)
	}
}
