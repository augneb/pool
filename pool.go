package pool

import (
	"context"
	"errors"
	"sync"
)

type IConn interface {
	Close() error
	IsValid() bool
}

type Config struct {
	MaxOpen  int
	MinOpen  int
	DialFunc func() (IConn, error)
}

type Pool struct {
	c      *Config
	mu     sync.Mutex
	conns  chan IConn
	count  int
	closed bool
}

var (
	errPoolIsClose  = errors.New("Connection pool has been closed")
	errContextClose = errors.New("Get connection close by context")
	errPoolIsFull   = errors.New("Pool is full")
)

func NewPool(c *Config) (*Pool, error) {
	if c.MinOpen > c.MaxOpen || c.MaxOpen <= 0 {
		return nil, errors.New("Number of connection bound error")
	}

	p := &Pool{
		c:     c,
		conns: make(chan IConn, c.MaxOpen),
	}

	if c.MinOpen > 0 {
		for i := 0; i < c.MinOpen; i++ {
			if err := p.makeConn(); err != nil {
				return nil, err
			}
		}
	}

	return p, nil
}

func (p *Pool) SetMaxOpen(num int) *Pool {
	p.mu.Lock()
	p.c.MaxOpen = num
	p.mu.Unlock()

	return p
}

func (p *Pool) Get(ctx context.Context, focusNew ...bool) (IConn, error) {
	if p.isClosed() {
		return nil, errPoolIsClose
	}

	if len(focusNew) > 0 && focusNew[0] {
		conn, err := p.newConn()
		if err != nil {
			return nil, err
		}

		return conn, nil
	}

	select {
	case conn := <-p.conns:
		return conn, nil
	default:
		p.makeConn()

		select {
		case conn := <-p.conns:
			return conn, nil
		case <-ctx.Done():
			return nil, errContextClose
		}
	}
}

func (p *Pool) Close() {
	if !p.isClosed() {
		p.mu.Lock()

		p.closed = true
		close(p.conns)

		for conn := range p.conns {
			conn.Close()
		}

		p.mu.Unlock()
	}
}

func (p *Pool) Release(conn IConn) {
	if p.isClosed() {
		return
	}

	if !conn.IsValid() {
		p.mu.Lock()
		p.count = p.count - 1
		p.mu.Unlock()
		return
	}

	select {
	case p.conns <- conn:
	default:
		conn.Close()
	}
}

func (p *Pool) isClosed() bool {
	p.mu.Lock()
	ret := p.closed
	p.mu.Unlock()

	return ret
}

func (p *Pool) makeConn() error {
	conn, err := p.newConn()
	if err != nil {
		return err
	}

	p.conns <- conn

	return nil
}

func (p *Pool) newConn() (IConn, error) {
	p.mu.Lock()

	if p.count >= p.c.MaxOpen {
		p.mu.Unlock()
		return nil, errPoolIsFull
	}

	p.count += 1
	p.mu.Unlock()

	conn, err := p.c.DialFunc()
	if err != nil {
		p.mu.Lock()
		p.count -= 1
		p.mu.Unlock()

		return nil, err
	}

	return conn, nil
}