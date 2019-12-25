package grpc

import (
	"sync"
	"time"

	"google.golang.org/grpc"
)

type pool struct {
	size int
	ttl  int64

	//  max streams on a *poolConn
	maxStreams int

	sync.Mutex
	conns map[string]*streamPool
}

type streamPool struct {
	head   *poolConn
	count  int
}

type poolConn struct {
	*grpc.ClientConn
	err     error
	addr    string
	p       *pool
	sp      *streamPool
	next    *poolConn
	streams int
	created int64
}

func newPool(size int, ttl time.Duration, ms int) *pool {
	if ms <= 0 {
		ms = 1
	}
	return &pool{
		size:  size,
		ttl:   int64(ttl.Seconds()),
		maxStreams: ms,
		conns: make(map[string]*streamPool),
	}
}

func (p *pool) getConn(addr string, opts ...grpc.DialOption) (*poolConn, error) {
	p.Lock()
	sp, ok := p.conns[addr]
	if !ok {
		p.conns[addr] = &streamPool{head:&poolConn{}, count:0}
	}
	now := time.Now().Unix()

	// while we have conns check age and then return one
	// otherwise we'll create a new conn
	pre, conn := sp.head, sp.head.next
	for conn != nil {
		d := now - conn.created

		// if conn is old and has no stream kill it and move on
		if conn.streams <= 0 && d > p.ttl {
			conn.ClientConn.Close()
			pre.next = conn.next
			conn = conn.next
			sp.count--
			continue
		}

		// too many streams or too old
		if conn.streams >= p.maxStreams || d > p.ttl {
			pre = conn
			conn = conn.next
			continue
		}

		// we got a good conn, lets unlock and return it
		sp.count--
		conn.streams++
		pre.next = conn.next
		p.Unlock()

		return conn, nil
	}

	p.Unlock()

	// create new conn
	cc, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}

	return &poolConn{cc,nil,addr,p,sp,nil,1,time.Now().Unix()}, nil
}

func (p *pool) release(addr string, conn *poolConn, err error) {
	conn.streams--
	// don't store the conn if it has errored
	if err != nil && conn.streams <= 0 {
		conn.ClientConn.Close()
		return
	}

	// otherwise put it back for reuse
	p.Lock()
	sp, ok := p.conns[addr]
	if !ok || (sp.count >= p.size && conn.streams <= 0) {
		p.Unlock()
		conn.ClientConn.Close()
		return
	}
	sp.count++
	conn.next = sp.head.next
	sp.head = conn
	p.Unlock()
}

func (conn *poolConn)Close()  {
	conn.p.release(conn.addr, conn, conn.err)
}