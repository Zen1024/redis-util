package redlock

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

var locker *Rdm

var addrs []string = []string{
	"127.0.0.1:6379",
	"127.0.0.1:6377",
	"127.0.0.1:6375",
	"127.0.0.1:6373",
	"127.0.0.1:6371",
}

func newPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3000,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr, redis.DialDatabase(0))
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < 5*time.Second {
				return nil
			}

			_, err := c.Do("PING")
			return err
		},
	}
}

func init() {
	pools := []*redis.Pool{}
	for _, addr := range addrs {
		pool := newPool(addr)
		pools = append(pools, pool)
	}
	var err error
	locker, err = NewRdm(30*time.Millisecond, 300*time.Millisecond, pools, 10)
	if err != nil {
		panic(err)
	}
	runtime.GOMAXPROCS(4)
}

type Client struct {
	TimeTook time.Duration
}

func (c *Client) Do() {
	time.Sleep(c.TimeTook)
}

func RandClient() *Client {
	time_took := time.Millisecond * time.Duration(rand.Int63n(1000))
	return &Client{
		TimeTook: time_took,
	}
}

func TestFunc(t *testing.T) {
	cs := make([]*Client, 100)
	wg := sync.WaitGroup{}
	wg.Add(100)
	for i := 0; i < 100; i++ {
		cs[i] = RandClient()
	}
	j := 0
	for i := 0; i < 100; i++ {
		c := cs[i]
		go func(c *Client) {
			defer wg.Done()
			locked, val := locker.Lock("foo-lock2")
			fmt.Printf("locked:%v,val:%s,j:%d\n", locked, val, j)
			tmp := j
			j = tmp + 1
			c.Do()
		}(c)
	}
	wg.Wait()
}
