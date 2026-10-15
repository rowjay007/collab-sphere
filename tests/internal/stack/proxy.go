package stack

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// tcpProxy forwards connections to target. While cut it has closed every
// connection and listens on nothing, so dials are refused.
type tcpProxy struct {
	t      *testing.T
	addr   string
	target string

	mu    sync.Mutex
	ln    net.Listener
	conns map[net.Conn]struct{}
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	p := &tcpProxy{t: t, addr: freeAddr(t), target: target, conns: make(map[net.Conn]struct{})}
	p.restore()
	t.Cleanup(p.cut)
	return p
}

func (p *tcpProxy) cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ln != nil {
		p.ln.Close()
		p.ln = nil
	}
	for c := range p.conns {
		c.Close()
	}
	clear(p.conns)
}

func (p *tcpProxy) restore() {
	p.t.Helper()
	var ln net.Listener
	var err error
	for range 50 {
		if ln, err = net.Listen("tcp", p.addr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		p.t.Fatalf("proxy listen %s: %v", p.addr, err)
	}
	p.mu.Lock()
	p.ln = ln
	p.mu.Unlock()
	go p.accept(ln)
}

func (p *tcpProxy) accept(ln net.Listener) {
	for {
		down, err := ln.Accept()
		if err != nil {
			return
		}
		up, err := net.Dial("tcp", p.target)
		if err != nil {
			down.Close()
			continue
		}
		p.mu.Lock()
		if p.ln != ln {
			p.mu.Unlock()
			down.Close()
			up.Close()
			return
		}
		p.conns[down] = struct{}{}
		p.conns[up] = struct{}{}
		p.mu.Unlock()
		go p.pipe(down, up)
		go p.pipe(up, down)
	}
}

func (p *tcpProxy) pipe(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	dst.Close()
	src.Close()
	p.mu.Lock()
	delete(p.conns, dst)
	delete(p.conns, src)
	p.mu.Unlock()
}
