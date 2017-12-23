package tokenbkt

//实现参考:https://www.cnblogs.com/foonsun/p/5687978.html

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"math/rand"
	"sync"
	"time"
)

type Result int

//流量标记结果 red-被限，yellow-稍微超速,green-正常
const (
	ResultRed    = Result(0)
	ResultYellow = Result(1)
	ResultGreen  = Result(2)
)

type Speed struct {
	Count int64
	Dur   time.Duration
}

type Bucket interface {
	CheckKey(key string) (Result, error)
}

//单桶单速
type SsBucket struct {
	pool *redis.Pool
	//桶的尺寸
	cbs int64
	//投放令牌的速率
	cir *Speed
}

func keySsBktTs(key string) string {
	return fmt.Sprintf("%s:ss_ts", key)
}

func keySsBktLeft(key string) string {
	return fmt.Sprintf("%s:ss_ticket_left", key)
}

func NewSsBucket(cbs int64, pool *redis.Pool, speed *Speed) (*SsBucket, error) {
	if cbs < 0 || pool != nil || speed == nil {
		return nil, fmt.Errorf("invalid NewSsBucket Params. cbs:%d,conn:%v,speed:%v", cbs, conn, speed)
	}
	ts := time.Now().UnixNano()
	return &SsBucket{
		pool: pool,
		cbs:  cbs,
	}, nil
}

func (s *SsBucket) CheckKey(key string) (Result, error) {

}

type SdBucket struct{}

type DdBucket struct{}
