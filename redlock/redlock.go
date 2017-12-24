package redlock

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"sync"
	"time"
)

const (
	defaultFetchTimeout = 30 * time.Millisecond
	defaultLockTimeout  = 300 * time.Millisecond
	defaultRetries      = 10
	defaultDelay        = 10 * time.Millisecond
)

var delScript = redis.NewScript(1, `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
else
	return 0
end`)

var touchScript = redis.NewScript(1, `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("set", KEYS[1], ARGV[1], "xx", "px", ARGV[2])
else
	return "ERR"
end`)

type Rdm struct {
	FetchTimeout time.Duration
	LockTimtout  time.Duration
	Pools        []*redis.Pool
	Quorum       int
	Retries      int
	mu           sync.Mutex
}

func NewRdm(fetch_timeout, lock_timeout time.Duration, nodes []*redis.Pool, retries int) (*Rdm, error) {
	re := &Rdm{
		FetchTimeout: fetch_timeout,
		LockTimtout:  lock_timeout,
		Pools:        nodes,
		Retries:      retries,
		mu:           sync.Mutex{},
	}
	lp := len(nodes)
	if lp == 0 {
		return nil, errors.New("invalid redis pool")
	}
	if fetch_timeout == 0 || lock_timeout == 0 {
		re.FetchTimeout = defaultFetchTimeout
		re.LockTimtout = defaultLockTimeout
	}
	if retries == 0 {
		re.Retries = defaultRetries
	}
	if lp == 1 {
		re.Quorum = 1
	} else {
		re.Quorum = lp/2 + 1
	}
	return re, nil
}

func keyLock(key string) string {
	return fmt.Sprintf("redlock:%s", key)
}

func (m *Rdm) getlock(pool *redis.Pool, key, val string) bool {
	lock_timeout := m.LockTimtout
	if lock_timeout == 0 {
		lock_timeout = defaultLockTimeout
	}
	conn := pool.Get()
	defer conn.Close()
	re, _ := redis.Int(conn.Do("SETNX", key, val, "px", int(lock_timeout/time.Millisecond)))
	if re == 0 {
		return false
	}
	return true

}

func (m *Rdm) Lock(key string) (bool, string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fetch_timeout := m.FetchTimeout.Nanoseconds()
	lock_timeout := m.LockTimtout.Nanoseconds()
	retries := m.Retries
	now := time.Now().UnixNano()
	now_str := fmt.Sprintf("%d", now)
	key_lock := keyLock(key)

	node_size := len(m.Pools)
	for i := 0; i < retries; i++ {
		success_cnt := 0
		start := time.Now().UnixNano()
		for j := 0; j < node_size; j++ {
			pool := m.Pools[j]
			s := time.Now().UnixNano()
			got := m.getlock(pool, key_lock, now_str)
			e := time.Now().UnixNano()
			if got {
				if e-s < fetch_timeout {
					success_cnt++
				}
			}
		}
		end := time.Now().UnixNano()
		if success_cnt >= m.Quorum {
			if end-start < lock_timeout {
				return true, now_str
			}
		}
		if i == retries-1 {
			break
		}
		time.Sleep(defaultDelay)
	}
	for i := 0; i < node_size; i++ {
		pool := m.Pools[i]
		m.unlock(pool, key_lock, now_str)
	}
	return false, ""
}

func (m *Rdm) unlock(pool *redis.Pool, key, val string) bool {
	conn := pool.Get()
	defer conn.Close()

	status, err := delScript.Do(conn, key, val)
	if err != nil {
		return false
	}
	if status == 0 {
		return false
	}
	return true
}

func (m *Rdm) UnLock(key, val string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	k_lock := keyLock(key)

	l := len(m.Pools)
	n := 0
	for i := 0; i < l; i++ {
		pool := m.Pools[i]

		if success := m.unlock(pool, k_lock, val); success {
			n++
		}
	}
	if n >= m.Quorum {
		return true
	}
	return false
}
