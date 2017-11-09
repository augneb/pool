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

	// 本大爷来了，尔等还不跪拜
	p := &Pool{
		c:     c,
		conns: make(chan IConn, c.MaxOpen),
	}

	// 管他呢，先搞它十个八个的先
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
	// 比大更大
	p.mu.Lock()
	p.c.MaxOpen = num
	p.mu.Unlock()

	return p
}

func (p *Pool) Get(ctx context.Context, focusNew ...bool) (IConn, error) {
	if p.isClosed() {
		return nil, errPoolIsClose
	}

	// 新欢口味好一些么？
	if len(focusNew) > 0 && focusNew[0] {
		conn, err := p.newConn()
		if err != nil {
			return nil, err
		}

		return conn, nil
	}

	select {
	// 看我无敌抓 xx 功
	case conn := <-p.conns:
		return conn, nil
	default:
		// 来吧，大爷温暖的怀抱等着你
		p.makeConn()

		// 你们谁先上...
		select {
		case conn := <-p.conns:
			return conn, nil
		case <-ctx.Done():
			return nil, errContextClose
		}
	}
}

func (p *Pool) Close() {
	// 你们都不要我了么？
	if !p.isClosed() {
		p.mu.Lock()

		p.closed = true
		close(p.conns)

		// 一个一个的都去死
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

	// 艹，坏了
	if !conn.IsValid() {
		p.mu.Lock()
		p.count = p.count - 1
		p.mu.Unlock()
		return
	}

	select {
	case p.conns <- conn:
	default:
		// 满了，塞不进去了，扔了算了
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

	// 大力点.. oh yeah ...
	p.conns <- conn

	return nil
}

func (p *Pool) newConn() (IConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 满了满了，边玩去
	if p.count >= p.c.MaxOpen {
		return nil, errPoolIsFull
	}

	// 嗨，妹子，让我搞一下
	conn, err := p.c.DialFunc()
	if err != nil {
		return nil, err
	}

	// 每次都加一，马上就要走上人生巅峰了啊
	p.count += 1

	return conn, nil
}
