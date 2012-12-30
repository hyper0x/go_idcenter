package provider

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"idcenter/lib"
	"reflect"
	"strconv"
	"sync"
	"time"
)

type RedisCacheProvider struct {
	name                string
	redisServerIp       string
	redisServerPort     int
	redisServerPassword string
	cacheMutex          *sync.RWMutex // Do not need to initialize it when new.
	redisPool           *redis.Pool   // Do not need to initialize it when new.
}

func (self *RedisCacheProvider) Name() string {
	return self.name
}

func (self *RedisCacheProvider) Initialize() error {
	self.cacheMutex = new(sync.RWMutex)
	redisServerAddr := fmt.Sprintf("%s:%d", self.redisServerIp, self.redisServerPort)
	self.redisPool = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", redisServerAddr)
			if err != nil {
				return nil, err
			}
			if len(self.redisServerPassword) > 0 {
				if _, err := c.Do("AUTH", self.redisServerPassword); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
	}
	return nil
}

func (self *RedisCacheProvider) BuildList(group string, begin uint64, end uint64) error {
	self.cacheMutex.Lock()
	defer self.cacheMutex.Unlock()
	if len(group) == 0 {
		errorMsg := fmt.Sprintf("Invalid Parameter(s)! (group=%s)\n", group)
		lib.LogError(errorMsg)
		return errors.New(errorMsg)
	}
	if (begin <= 0) || (end <= 0) || (begin >= end) {
		errorMsg := fmt.Sprintf("Invalid Parameter(s)! (begin=%d, end=%d)\n", begin, end)
		lib.LogError(errorMsg)
		return errors.New(errorMsg)
	}
	conn := self.redisPool.Get()
	defer conn.Close()
	exists, err := redis.Bool(conn.Do("EXISTS", group))
	if err != nil {
		errorMsg := fmt.Sprintf("Redis Error <EXISTS %s>: %s\n ", group, err.Error())
		lib.LogErrorf(errorMsg)
		return errors.New(errorMsg)
	}
	if exists {
		effectedNumber, err := redis.Int(conn.Do("DEL", group))
		if err != nil {
			errorMsg := fmt.Sprintf("Redis Error <DEL %s>: %s\n ", group, err.Error())
			lib.LogError(errorMsg)
			return errors.New(errorMsg)
		}
		if effectedNumber < 1 {
			warningMsg := fmt.Sprintf("Redis warning <DEL %s>: seemingly failed.\n ", group)
			lib.LogWarn(warningMsg)
		}
	}
	for i := begin; i < end; i++ {
		length, err := redis.Int(conn.Do("LPUSH", group, i))
		if err != nil {
			errorMsg := fmt.Sprintf("Redis Error <LPUSH %s %d> (total_length=%d): %s\n ", group, i, length, err.Error())
			lib.LogError(errorMsg)
			return errors.New(errorMsg)
		}
	}
	lib.LogInfof("The list of group '%s' is Builded. (begin=%d, end=%d)\n", group, begin, end)
	return nil

}

func (self *RedisCacheProvider) Pop(group string) (uint64, error) {
	self.cacheMutex.RLock()
	defer self.cacheMutex.RUnlock()
	conn := self.redisPool.Get()
	defer conn.Close()
	value, err := conn.Do("RPOP", group)
	if err != nil {
		errorMsg := fmt.Sprintf("Redis Error <RPOP %s>: %s\n ", group, err.Error())
		lib.LogError(errorMsg)
		return 0, errors.New(errorMsg)
	}
	lib.LogInfoln("Value Type:", reflect.TypeOf(value))
	if value == nil {
		errorMsg := fmt.Sprintf("Empty List! (group=%s)", group)
		return 0, &lib.EmptyListError{errorMsg}
	}
	baValue := value.([]uint8)
	number, err := strconv.ParseUint(string(baValue), 10, 64)
	if err != nil {
		errorMsg := fmt.Sprintf("Converting Error (value=%s): %s\n ", value, err.Error())
		lib.LogError(errorMsg)
		return 0, errors.New(errorMsg)
	}
	return number, nil
}
