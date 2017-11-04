package pool

import (
	"sync"
	"time"
	"errors"
	"container/list"
)

type Conn interface {
	Close() error
	IsValid() bool
}

type idleConn struct {
	c Conn
	t time.Time
}

type Config struct {
	DialFunc    func() (Conn, error)
	MaxOpen     int
	MinOpen     int
	MaxIdle     int
	IdleTimeout time.Duration
}

type Pool struct {
	closed bool
	active int
	idle   list.List
	co     *sync.Cond
	mu     sync.Mutex
	c      Config
}

func NewPool(c Config) (*Pool, error) {
	if c.MaxOpen <= 0 || c.MinOpen > c.MaxOpen || c.MaxIdle > c.MaxOpen {
		return nil, errors.New("config error")
	}

	p :=  &Pool{c: c}

	if c.MinOpen > 0 {
		for i := 0; i < c.MinOpen; i++ {
			c, e := p.c.DialFunc()
			if e != nil {
				return nil, e
			}

			p.Put(c)
		}
	}

	return p, nil
}

func (p *Pool) SetMaxIdle(num int) *Pool {
	p.mu.Lock()
	p.c.MaxIdle = num
	p.mu.Unlock()

	return p
}

func (p *Pool) SetMaxOpen(num int) *Pool {
	p.mu.Lock()
	p.c.MaxOpen = num
	p.mu.Unlock()

	return p
}

func (p *Pool) Get() (Conn, error) {
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("get on closed pool")
	}

	if timeout := p.c.IdleTimeout; timeout > 0 {
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Back()
			if e == nil {
				break
			}

			ic := e.Value.(idleConn)
			if ic.t.Add(timeout).After(time.Now()) {
				break
			}

			p.idle.Remove(e)
			p.release()

			p.mu.Unlock()
			ic.c.Close()
			p.mu.Lock()
		}
	}

	for {
		// 获取可用的连接
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Front()
			if e == nil {
				break
			}

			ic := e.Value.(idleConn)
			p.idle.Remove(e)
			p.mu.Unlock()
			if ic.c.IsValid() {
				return ic.c, nil
			}

			ic.c.Close()
			p.mu.Lock()
			p.release()
		}

		// 获取新连接对象
		if p.c.MaxOpen == 0 || p.active < p.c.MaxOpen {
			p.active += 1
			p.mu.Unlock()

			c, err := p.c.DialFunc()
			if err != nil {
				p.mu.Lock()
				p.release()
				p.mu.Unlock()
				c = nil
			}

			return c, err
		}

		if p.co == nil {
			p.co = sync.NewCond(&p.mu)
		}

		// 等待通知信号
		p.co.Wait()
	}
}

func (p *Pool) Put(c Conn) error {
	p.mu.Lock()

	if !p.closed {
		if !c.IsValid() {
			p.release()
			p.mu.Unlock()
			return nil
		}

		p.idle.PushFront(idleConn{c: c, t: time.Now()})
		if p.c.MaxIdle > 0 && p.idle.Len() > p.c.MaxIdle {
			c = p.idle.Remove(p.idle.Back()).(idleConn).c
		} else {
			c = nil
		}
	}

	if c == nil {
		if p.co != nil {
			p.co.Signal()
		}

		p.mu.Unlock()
		return nil
	}

	p.release()
	p.mu.Unlock()

	return c.Close()
}

func (p *Pool) Close() {
	p.mu.Lock()

	idle := p.idle
	p.idle.Init()
	p.closed = true
	p.active -= idle.Len()
	if p.co != nil {
		p.co.Broadcast()
	}

	p.mu.Unlock()

	for e := idle.Front(); e != nil; e = e.Next() {
		e.Value.(idleConn).c.Close()
	}
}

func (p *Pool) ActiveCount() int {
	p.mu.Lock()
	c := p.active
	p.mu.Unlock()

	return c
}

func (p *Pool) release() {
	p.active--

	if p.co != nil {
		p.co.Signal()
	}
}
