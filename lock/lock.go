package lock

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"math/rand"
	"time"
)

//microsecond jitter
const time_jitter = int64(100)

type Locker struct {
	Pool        *redis.Pool
	LockTimeout time.Duration
}

func NewLocker(pool *redis.Pool, timeout time.Duration) *Locker {
	return &Locker{
		Pool:        pool,
		LockTimeout: timeout,
	}
}

func KeyLock(key string) string {
	return fmt.Sprintf("redis-lock:%s", key)
}

func (l *Locker) lock(key string) (error, string) {
	k_lock := KeyLock(key)
	conn := l.Pool.Get()
	defer conn.Close()
	now := time.Now().UnixNano()
	nowstr := fmt.Sprintf("%d", now)

	re, err := redis.Int(conn.Do("SETNX", k_lock, nowstr))
	if err != nil {
		return err, ""
	}

	if re == 1 {
		//返回lock-id
		return nil, nowstr
	} else {
		last_ts, err := redis.Int64(conn.Do("GET", k_lock))
		if err != nil {
			if err != redis.ErrNil {
				return err, ""
			} else {
				//锁已经被其他客户端释放,直接获取
				conn.Do("SETNX", k_lock, nowstr)
				return nil, nowstr
			}
		}
		dur := now - last_ts
		//超时,需要强制获取
		now2 := time.Now().UnixNano()
		now2_str := fmt.Sprintf("%d", now2)
		if dur > l.LockTimeout.Nanoseconds() {
			old_ts, err := redis.Int64(conn.Do("GETSET", k_lock, now2_str))
			if err != nil {
				if err != redis.ErrNil {
					return err, ""
				}
			}
			//锁未被其他客户端获取
			if old_ts == last_ts {
				conn.Do("SETNX", k_lock, now2_str)
				return nil, now2_str
			}
		}
	}
	return nil, ""
}

func (l *Locker) Lock(key string) error {
	err, id := l.lock(key)
	if err == nil && id != "" {
		return nil
	}
	if err != nil {
		return err
	}
	jit := time.Microsecond * time.Duration(rand.Int63n(time_jitter))
	//retry loop
	for {
		time.Sleep(jit)
		err, id = l.lock(key)
		if err == nil && id != "" {
			return nil
		}
		if err != nil {
			return err
		}
	}

}

func (l *Locker) UnLock(key string) error {
	conn := l.Pool.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", key)
	return err
}
