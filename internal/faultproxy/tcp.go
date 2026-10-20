package faultproxy

import (
	"errors"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"paymentledger/internal/faults"
)

// TCPStats counts injected faults.
type TCPStats struct {
	Accepted  int64 `json:"accepted"`
	Refused   int64 `json:"refused_during_blackout"`
	Resets    int64 `json:"resets"`
	Blackouts int64 `json:"blackouts"`
}

// TCPProxy forwards connections to a target (PostgreSQL) and can add
// latency, reset connections at random, or black out entirely (all
// connections reset, new ones refused) to simulate a failover.
type TCPProxy struct {
	target string
	ln     net.Listener

	latencyNs     atomic.Int64
	resetRate     atomic.Uint64 // float64 bits
	blackoutUntil atomic.Int64  // unix nanos

	mu    sync.Mutex
	rng   *rand.Rand
	conns map[*tcpPair]struct{}

	accepted, refused, resets, blackouts atomic.Int64
	closed                               atomic.Bool
}

type tcpPair struct{ client, server net.Conn }

func (p *tcpPair) kill() {
	for _, c := range []net.Conn{p.client, p.server} {
		if tc, ok := c.(*net.TCPConn); ok {
			tc.SetLinger(0) //nolint:errcheck
		}
		c.Close()
	}
}

func NewTCP(listenAddr, target string, seed uint64) (*TCPProxy, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	p := &TCPProxy{target: target, ln: ln, rng: rand.New(rand.NewPCG(seed, seed^0x5bd1e995)), conns: map[*tcpPair]struct{}{}}
	go p.acceptLoop()
	go p.resetLoop()
	return p, nil
}

func (p *TCPProxy) Addr() string { return p.ln.Addr().String() }

func (p *TCPProxy) SetFaults(f faults.TCP) {
	p.latencyNs.Store(int64(f.LatencyMs * float64(time.Millisecond)))
	p.resetRate.Store(math.Float64bits(f.ResetRatePerSec))
}

// Blackout resets every connection and refuses new ones for d.
func (p *TCPProxy) Blackout(d time.Duration) {
	p.blackouts.Add(1)
	p.blackoutUntil.Store(time.Now().Add(d).UnixNano())
	p.mu.Lock()
	for c := range p.conns {
		c.kill()
		p.resets.Add(1)
	}
	p.mu.Unlock()
}

func (p *TCPProxy) Stats() TCPStats {
	return TCPStats{Accepted: p.accepted.Load(), Refused: p.refused.Load(), Resets: p.resets.Load(), Blackouts: p.blackouts.Load()}
}

func (p *TCPProxy) Close() error {
	p.closed.Store(true)
	err := p.ln.Close()
	p.mu.Lock()
	for c := range p.conns {
		c.kill()
	}
	p.mu.Unlock()
	return err
}

func (p *TCPProxy) acceptLoop() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			if p.closed.Load() || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		if time.Now().UnixNano() < p.blackoutUntil.Load() {
			p.refused.Add(1)
			if tc, ok := c.(*net.TCPConn); ok {
				tc.SetLinger(0) //nolint:errcheck
			}
			c.Close()
			continue
		}
		go p.handle(c)
	}
}

func (p *TCPProxy) handle(client net.Conn) {
	server, err := net.DialTimeout("tcp", p.target, 5*time.Second)
	if err != nil {
		client.Close()
		return
	}
	p.accepted.Add(1)
	pair := &tcpPair{client: client, server: server}
	p.mu.Lock()
	p.conns[pair] = struct{}{}
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.conns, pair)
		p.mu.Unlock()
		client.Close()
		server.Close()
	}()

	done := make(chan struct{}, 2)
	go func() { io.Copy(server, client); done <- struct{}{} }() //nolint:errcheck
	go func() { p.copyWithLatency(client, server); done <- struct{}{} }()
	<-done
}

func (p *TCPProxy) copyWithLatency(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if d := p.latencyNs.Load(); d > 0 {
				time.Sleep(time.Duration(d))
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *TCPProxy) resetLoop() {
	const tick = 100 * time.Millisecond
	t := time.NewTicker(tick)
	defer t.Stop()
	for range t.C {
		if p.closed.Load() {
			return
		}
		rate := math.Float64frombits(p.resetRate.Load())
		if rate <= 0 {
			continue
		}
		prob := 1 - math.Exp(-rate*tick.Seconds())
		p.mu.Lock()
		for c := range p.conns {
			if p.rng.Float64() < prob {
				c.kill()
				p.resets.Add(1)
			}
		}
		p.mu.Unlock()
	}
}
