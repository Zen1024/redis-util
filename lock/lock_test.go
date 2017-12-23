package lock

import (
	"github.com/garyburd/redigo/redis"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

var locker *Locker

func newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3000,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "127.0.0.1:6379", redis.DialDatabase(0))
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
	pool := newPool()
	locker = NewLocker(pool, time.Millisecond*30)
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
	cs := make([]*Client, 1000)
	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		cs[i] = RandClient()
	}
	j := 0
	for i := 0; i < 10; i++ {
		c := cs[i]
		go func(c *Client) {
			defer wg.Done()
			if err := locker.Lock("foo-lock"); err != nil {
				t.Fatalf("Err lock:%s\n", err.Error())
			}
			t.Logf("j:%d\n", j)
			tmp := j
			j = tmp + 1
			c.Do()
			locker.UnLock("foo-lock")
		}(c)
	}
	wg.Wait()
}

func BenchmarkLock(b *testing.B) {
	for n := 0; n < b.N; n++ {
		locker.Lock("test-lock")
		locker.UnLock("test-lock")
	}
}
