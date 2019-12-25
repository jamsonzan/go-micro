package grpc

import (
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
)

var (
	tooManyConnection = errors.New("too many open connection")
)

type pool struct {
	size int
	ttl  int64
	//  max streams on a *poolConn
	maxStreams int
	//  max idle conns
	maxIdle    int

	sync.Mutex
	conns map[string]*streamsPool
}

type streamsPool struct {
	//  head of list
	head   *poolConn
	//  the siza of list
	count  int
	//  idle conn
	idle   int
}

type poolConn struct {
	//  grpc conn
	*grpc.ClientConn
	err     error
	addr    string

	//  pool and streams pool
	pool    *pool
	sp      *streamsPool
	streams int
	created int64

	//  list
	pre     *poolConn
	next    *poolConn
}

func newPool(size int, ttl time.Duration, ms int, idle int) *pool {
	if ms <= 0 {
		ms = 1
	}
	if idle <= 0 {
		idle = 0
	}
	return &pool{
		size:  size,
		ttl:   int64(ttl.Seconds()),
		maxStreams: ms,
		maxIdle: idle,
		conns: make(map[string]*streamsPool),
	}
}

func (p *pool) getConn(addr string, opts ...grpc.DialOption) (*poolConn, error) {
	p.Lock()
	sp, ok := p.conns[addr]
	if !ok {
		p.conns[addr] = &streamsPool{head:&poolConn{}, count:0, idle:0}
	}
	// while we have conns check streams and then return one
	// otherwise we'll create a new conn
	conn := sp.head.next
	for conn != nil {
		// too many streams
		if conn.streams >= p.maxStreams{
			conn = conn.next
			continue
		}
		// a idle conn
		if conn.streams == 0 {
			sp.idle--
		}
		// we got a good conn, lets unlock and return it
		conn.streams++
		p.Unlock()
		return conn, nil
	}
	// too many connection
	if sp.count >= p.size {
		return nil, tooManyConnection
	}
	p.Unlock()
	// create new conn
	cc, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	conn = &poolConn{cc,nil,addr,p,sp,1,time.Now().Unix(), nil, nil}
	// add conn to streams pool
	p.Lock()
	addConnAfter(conn, sp.head)
	p.Unlock()

	return conn, nil
}

func (p *pool) release(addr string, conn *poolConn, err error) {
	p.Lock()
	conn.streams--
	if conn.streams > 0 {
		p.Unlock()
		return
	}
	// it has errored or
	// too many idle conn or
	// conn is too old
	now := time.Now().Unix()
	p, sp, created := conn.pool, conn.sp, conn.created
	if err != nil || sp.idle >= p.maxIdle || now-created > p.ttl {
		removeConn(conn)
		p.Unlock()
		conn.ClientConn.Close()
		return
	}
	sp.idle++
	p.Unlock()
	return
}

func (conn *poolConn)Close()  {
	conn.pool.release(conn.addr, conn, conn.err)
}

func removeConn(conn *poolConn)  {
	if conn.next == nil || conn.pre == nil{
		conn.pre = nil
		return
	}
	conn.pre.next = conn.next
	conn.next.pre = conn.pre
	conn.sp.count--
	return
}

func addConnAfter(conn *poolConn, after *poolConn)  {
	conn.next = after.next
	conn.pre = after
	if after.next != nil {
		after.next.pre = conn
	}
	after.next = conn
	conn.sp.count++
	return
}