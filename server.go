package main

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"log"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/internal/testdata"
)

func main() {
	tlsConf := generateTLSConfig()

	listener, err := quic.ListenAddr(
		"10.0.0.1:6121",
		tlsConf,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Server listening on :6121")

	for {
		conn, err := listener.Accept(context.Background())
		if err != nil {
			log.Println(err)
			continue
		}

		go handleConn(conn)
	}
}

func handleConn(conn *quic.Conn) {
	log.Println("New connection")

	stream, err := conn.AcceptStream(context.Background())
	if err != nil {
		log.Println(err)
		return
	}

	handleStream(stream)
}

func handleStream(stream *quic.Stream) {
	defer stream.Close()

	for {
		// =========================
		// read frame length
		// =========================
		lenBuf := make([]byte, 4)

		_, err := io.ReadFull(stream, lenBuf)
		if err != nil {
			if err == io.EOF {
				return
			}

			log.Println("length read error:", err)
			return
		}

		size := binary.BigEndian.Uint32(lenBuf)

		// protection
		if size == 0 || size > 10*1024*1024 {
			log.Println("invalid packet size:", size)
			return
		}

		// =========================
		// read payload
		// =========================
		buf := make([]byte, size)

		_, err = io.ReadFull(stream, buf)
		if err != nil {
			log.Println("payload read error:", err)
			return
		}

		// =========================
		// echo back
		// =========================
		_, err = stream.Write(lenBuf)
		if err != nil {
			log.Println("length write error:", err)
			return
		}

		_, err = stream.Write(buf)
		if err != nil {
			log.Println("payload write error:", err)
			return
		}
	}
}

func generateTLSConfig() *tls.Config {
	certFile, keyFile := testdata.GetCertificatePaths()

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatal(err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"quic-test"},
	}
}