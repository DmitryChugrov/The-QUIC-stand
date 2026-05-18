package main

import (
	"sort"
	"syscall"
	"context"
	"crypto/tls"
	"encoding/binary"
	"flag"
	"io"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
)

func main() {
	// ===============================
	// Config
	// ===============================
	addr := flag.String("addr", "10.0.0.1:6121", "server address")
	numPackets := flag.Int("n", 1000, "number of packets")
	packetSize := flag.Int("size", 1400, "packet payload size")
	interval := flag.Duration("interval", 50*time.Microsecond, "delay between packets")

	flag.Parse()

	log.Printf("Target: %s", *addr)
	log.Printf("Packets: %d", *numPackets)
	log.Printf("Packet size: %d", *packetSize)
	log.Printf("Interval: %s", interval.String())

	startCPU := getCPUTime()

	// ===============================
	// TLS - Config
	// ===============================
	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"quic-test"},
	}

	// ===============================
	// Qlog  connection
	// ===============================
	quicConf := &quic.Config{
		Tracer: qlog.DefaultConnectionTracer,
	}

	// ===============================
	// handshake timing
	// ===============================
	clientStart := time.Now()
	
	handshakeStart := time.Now()

	conn, err := quic.DialAddr(
		context.Background(),
		*addr,
		tlsConf,
		quicConf,
	)
	if err != nil {
		log.Fatal(err)
	}
	//defer conn.CloseWithError(0, "")

	handshakeLatency := time.Since(handshakeStart)

	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	// ===============================
	// Metrics
	// ===============================
	var sent uint64
	var received uint64
	var totalRTT int64
	var appBytes uint64
	var wireBytes uint64

	sentTimes := sync.Map{}

	var rttValues []time.Duration
	var rttMu sync.Mutex

	var firstByteTime time.Duration
	firstByteOnce := sync.Once{}

	start := time.Now()

	// ===============================
	// Receiver
	// ===============================
	go func() {
		for {
			lenBuf := make([]byte, 4)

			_, err := io.ReadFull(stream, lenBuf)
			if err != nil {
				return
			}

			size := binary.BigEndian.Uint32(lenBuf)

			buf := make([]byte, size)

			_, err = io.ReadFull(stream, buf)
			if err != nil {
				return
			}

			seq := binary.BigEndian.Uint32(buf[:4])

			if val, ok := sentTimes.Load(seq); ok {
				sentTime := val.(time.Time)
				rtt := time.Since(sentTime)

				firstByteOnce.Do(func() {
					firstByteTime = time.Since(clientStart)
				})

				rttMu.Lock()
				rttValues = append(rttValues, rtt)
				rttMu.Unlock()

				atomic.AddInt64(&totalRTT, int64(rtt))
				sentTimes.Delete(seq)
				atomic.AddUint64(&received, 1)
			}
		}
	}()

	// ===============================
	// Sender
	// ===============================
	for i := 0; i < *numPackets; i++ {
		packet := make([]byte, *packetSize)
		atomic.AddUint64(&appBytes, 
		uint64(len(packet)+4),
		)
		
		const protocolOverhead = 64

		atomic.AddUint64(&wireBytes, uint64(len(packet)+protocolOverhead),)

		binary.BigEndian.PutUint32(packet[:4], uint32(i))
		sentTimes.Store(uint32(i), time.Now())

		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(packet)))

		_, err := stream.Write(lenBuf)
		if err != nil {
			log.Println(err)
			continue
		}

		_, err = stream.Write(packet)
		if err != nil {
			log.Println(err)
			continue
		}

		atomic.AddUint64(&sent, 1)

		if *interval > 0 {
			time.Sleep(*interval)
		}
	}

	//stream.Close()

	//timeout := time.After(10 * time.Second)
	
	// graceful stream close
	streamCloseStart := time.Now()

	err = stream.Close()

	streamCloseLatency := time.Since(streamCloseStart)
	
	if err != nil {
		log.Println("stream close error:", err)
	}

	
	timeout := time.After(10 * time.Second)

	for {
		if atomic.LoadUint64(&received) >= atomic.LoadUint64(&sent) {
			break
		}

		select {
		case <-timeout:
			log.Println("Timeout waiting for responses")
			goto DONE
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

DONE:
	err = conn.CloseWithError(0, "")
	if err != nil {
		log.Println("conn close error:", err)
	}

	
	time.Sleep(3 * time.Second)

	duration := time.Since(start)

	sentVal := atomic.LoadUint64(&sent)
	recvVal := atomic.LoadUint64(&received)
	
	rttMu.Lock()
	sort.Slice(rttValues, func(i, j int) bool {
	return rttValues[i] < rttValues[j]
})
	
	percentile := func(p float64) time.Duration {
	if len(rttValues) == 0 {
		return 0
	}

	idx := int(float64(len(rttValues)-1) * p)
	return rttValues[idx]
}
	p50 := percentile(0.50)
	p95 := percentile(0.95)
	p99 := percentile(0.99)
	rttMu.Unlock()
	// ===============================
	//  RTT stats
	// ===============================
	var meanRTT float64
	var jitterDuration time.Duration
	
	if len(rttValues) > 0 {
		var sum time.Duration
		for _, v := range rttValues {
			sum += v
		}
		meanRTT = float64(sum) / float64(len(rttValues)) / 1e9

		

		if len(rttValues) > 1 {
		for i := 1; i < len(rttValues); i++ {
		d := rttValues[i] - rttValues[i-1]

		if d < 0 {
			d = -d
		}

		jitterDuration +=
			(d - jitterDuration) / 16
	}
}
	}

	// ===============================
	// throughput metrics
	// ===============================
	throughputMbps := float64(atomic.LoadUint64(&wireBytes)*8) /duration.Seconds() / 1e6

goodputMbps := float64(atomic.LoadUint64(&appBytes)*8) / duration.Seconds() / 1e6

	pps := float64(recvVal) / duration.Seconds()
	//loss := 0.0
	//if sentVal > 0 {
	//	loss = float64(sentVal-recvVal) / float64(sentVal) * 100
	//}

	// ===============================
	// CPU / memory (proxy energy)
	// ===============================
	endCPU := getCPUTime()
	cpuUsed := endCPU - startCPU

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// ===============================
	// OUTPUT
	// ===============================
	log.Println("=== CLIENT STATS ===")
	log.Printf("Sent: %d", sentVal)
	log.Printf("Received: %d", recvVal)
	log.Printf("Duration: %.3f s", duration.Seconds())

	log.Printf("Throughput: %.2f Mbps", throughputMbps)
	log.Printf("Goodput: %.2f Mbps", goodputMbps)
	log.Printf("Packet rate: %.2f pps", pps)

	//log.Printf("Packet Loss: %.2f%%", loss)

	log.Printf("Mean RTT: %.6f s", meanRTT)
	log.Printf("RTT p50: %.3f ms", p50.Seconds()*1000)
	log.Printf("RTT p95: %.3f ms", p95.Seconds()*1000)
	log.Printf("RTT p99: %.3f ms", p99.Seconds()*1000)
	log.Printf(
	"Jitter: %.3f ms",
	jitterDuration.Seconds()*1000,
)

	log.Printf("TTFB: %.6f s", firstByteTime.Seconds())

	log.Printf("Handshake latency: %.6f s", handshakeLatency.Seconds())

	log.Printf(
	"CPU time: %.6f s",
	cpuUsed.Seconds(),
)
	log.Printf("Memory alloc (MB): %.2f", float64(m.Alloc)/1024/1024)
	log.Printf(
	"Stream close latency: %.6f s",
	streamCloseLatency.Seconds(),
)
}

// ===============================
// CPU helper
// ===============================
func getCPUTime() time.Duration {
    var r syscall.Rusage

    err := syscall.Getrusage(syscall.RUSAGE_SELF, &r)
    if err != nil {
        return 0
    }

    user :=
        time.Duration(r.Utime.Sec)*time.Second +
            time.Duration(r.Utime.Usec)*time.Microsecond

    system :=
        time.Duration(r.Stime.Sec)*time.Second +
            time.Duration(r.Stime.Usec)*time.Microsecond

    return user + system
}