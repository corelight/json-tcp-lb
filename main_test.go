package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// generateTestCerts generates a self signed key pair
func generateTestCerts(t *testing.T) (string, string) {
	// Mostly from https://go.dev/src/crypto/tls/generate_cert.go
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	notBefore := time.Now().Add(-5 * time.Minute)
	notAfter := notBefore.Add(10 * time.Minute)
	serialNumber := big.NewInt(42)

	keyUsage := x509.KeyUsageDigitalSignature
	keyUsage |= x509.KeyUsageKeyEncipherment

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Acme Co"},
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage:              keyUsage,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	template.IPAddresses = append(template.IPAddresses, net.ParseIP("127.0.0.1"))

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}

	certDir := t.TempDir()

	certPath := filepath.Join(certDir, "cert.pem")
	keyPath := filepath.Join(certDir, "key.pem")

	certOut, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("Failed to open %v for writing: %v", certPath, err)
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		t.Fatalf("Failed to write data to %v: %v", certPath, err)
	}
	if err := certOut.Close(); err != nil {
		t.Fatalf("Error closing %v: %v", certPath, err)
	}
	t.Logf("wrote %v\n", certPath)

	keyOut, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatalf("Failed to open %v for writing: %v", keyPath, err)
	}
	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("Unable to marshal private key: %v", err)
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes}); err != nil {
		t.Fatalf("Failed to write data to %v: %v", keyPath, err)
	}
	if err := keyOut.Close(); err != nil {
		t.Fatalf("Error closing %v: %v", keyPath, err)
	}
	t.Logf("wrote %v", keyPath)

	return certPath, keyPath
}

func listenTestConnection(t *testing.T, l net.Listener, connections int) (int, error) {
	resultChan := make(chan int, connections)

	var lines int
	for i := 0; i < connections; i++ {
		conn, err := l.Accept()
		if err != nil {
			return 0, err
		}
		t.Logf("Accepted connection %d of %d", i+1, connections)
		go handleTestConnection(t, conn, resultChan)
	}
	for i := 0; i < connections; i++ {
		lines += <-resultChan
	}
	return lines, nil
}

func handleTestConnection(t *testing.T, conn net.Conn, resultChan chan int) {
	var lines int
	t.Logf("New connection from %s", conn.RemoteAddr())
	defer conn.Close()
	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				t.Logf("Error reading: %v", err)
			}
			break
		}
		lines += bytes.Count(buf[:n], []byte("\n"))
	}
	t.Logf("Connection got %d lines", lines)
	resultChan <- lines
}

const logLine = `{"_path":"conn","_system_name":"HQ","_write_ts":"2018-11-28T04:50:48.848281Z","ts":"2018-11-28T04:50:38.834880Z","uid":"CX6jut3BmNwFdYkgrk","id.orig_h":"fc00::165","id.orig_p":44206,"id.resp_h":"fc00::1","id.resp_p":53,"proto":"udp","service":"dns","duration":0.004537,"orig_bytes":55,"resp_bytes":55,"conn_state":"SF","local_orig":false,"local_resp":false,"missed_bytes":0,"history":"Dd","orig_pkts":1,"orig_ip_bytes":103,"resp_pkts":1,"resp_ip_bytes":103,"tunnel_parents":[],"corelight_shunted":false,"orig_l2_addr":"ac:1f:6b:00:81:9a","resp_l2_addr":"b4:75:0e:08:08:c1"}`

func connectToProxy(cfg Config) (net.Conn, error) {
	var conn net.Conn
	var err error
	target := fmt.Sprintf("127.0.0.1:%d", cfg.Port)
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		conn, err = net.DialTimeout("tcp", target, 5*time.Second)
	} else {
		//TODO: fixme. this needs to be tested properly
		conf := &tls.Config{
			InsecureSkipVerify: true,
		}
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", target, conf)
	}
	return conn, err
}

func spew(t *testing.T, cfg Config, lines int) error {
	line := []byte(logLine + "\n")
	conn, err := connectToProxy(cfg)
	if err != nil {
		return fmt.Errorf("Spew: %w", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	t.Logf("Spewing %d lines to port %d", lines, cfg.Port)
	for i := 0; i < lines; i++ {
		_, err := conn.Write(line)
		if err != nil {
			return fmt.Errorf("Spew failed: %w", err)
		}
	}
	return nil
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
	t.Logf("Listening for tests on %d", port)
	defer l.Close()
	cfg := Config{
		Port: port,
	}
	for i := 0; i < connections; i++ {
		go spew(t, cfg, linesPerConnnection)
	}
	lines, err := listenTestConnection(t, l, connections)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Got %d lines total", lines)
	if lines != expected {
		t.Errorf("Expected %d lines, got %d", expected, lines)
	}
}

func testProxy(t *testing.T, cfg Config) {
	connections := 8
	linesPerConnnection := 10000
	expected := connections * linesPerConnnection

	// Common settings for all tests
	cfg.Connections = connections
	cfg.Addr = ""
	cfg.Port = 0 //Select dynamically

	// Setup downstream listener
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	t.Logf("Listening for tests on %d", port)
	defer l.Close()
	target := fmt.Sprintf("localhost:%d", port)
	cfg.Targets = []string{target}

	// Setup proxy listener
	ctx, cancel := context.WithCancel(context.Background())
	pl, err := listen(cfg)
	proxyPort := pl.Addr().(*net.TCPAddr).Port
	t.Logf("Listening for proxy on %d", proxyPort)
	cfg.Port = proxyPort
	defer pl.Close()
	//

	go proxy(ctx, pl, cfg)

	// Spew everything and then close the proxy listener
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < connections; i++ {
			wg.Add(1)
			go func() {
				err := spew(t, cfg, linesPerConnnection)
				if err == nil {
					t.Logf("Spew done")
				} else {
					t.Logf("spew: %v", err)
				}
				wg.Done()
			}()
		}
		wg.Wait()
		t.Logf("All spew done")
		time.Sleep(1 * time.Second)
		cancel()
	}()

	lines, err := listenTestConnection(t, l, connections)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Got %d lines total", lines)
	if lines != expected {
		t.Errorf("Expected %d lines, got %d", expected, lines)
	}
}

func TestProxyPlainText(t *testing.T) {
	// All default settings
	cfg := Config{}
	testProxy(t, cfg)
}

func TestProxyTLS(t *testing.T) {
	cert, key := generateTestCerts(t)
	cfg := Config{
		CertFile: cert,
		KeyFile:  key,
	}
	testProxy(t, cfg)
}
