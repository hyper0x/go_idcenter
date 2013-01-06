package provider

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"idcenter/lib"
	"strconv"
	"sync"
	"time"
)

var initContext sync.Once
var cacheMutex *sync.RWMutex
var redisPool *redis.Pool
var iRedisCacheProvider *redisCacheProvider

type RedisCacheParameter struct {
	Name string
	Ip string
	Port int
	Password string
}

type redisCacheProvider struct {
	ProviderName string
}

func New(parameter RedisCacheParameter) *redisCacheProvider {
	initContext.Do(func() {
		err := initialize(parameter)
		if err != nil {
			panic(err)
		}
	})
	return iRedisCacheProvider
}

func initialize(parameter RedisCacheParameter) error {
	redisServerAddr := fmt.Sprintf("%v:%v", parameter.Ip, parameter.Port)
	lib.LogInfof("Initialize redis cache provider (%v)...", parameter)
	redisPool = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", redisServerAddr)
			if err != nil {
				return nil, err
			}
			if len(parameter.Password) > 0 {
				if _, err := c.Do("AUTH", parameter.Password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
	}
	cacheMutex = new(sync.RWMutex)
	iRedisCacheProvider = &redisCacheProvider{parameter.Name}
	return nil
}

func (self *redisCacheProvider) Name() string {
	return self.ProviderName
}

func (self *redisCacheProvider) BuildList(group string, begin uint64, end uint64) error {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
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
	conn := redisPool.Get()
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
	lib.LogInfof("The list of group '%s' is builded. (begin=%d, end=%d)\n", group, begin, end)
	return nil

}

func (self *redisCacheProvider) Pop(group string) (uint64, error) {
	cacheMutex.RLock()
	defer cacheMutex.RUnlock()
	conn := redisPool.Get()
	defer conn.Close()
	value, err := conn.Do("RPOP", group)
	if err != nil {
		errorMsg := fmt.Sprintf("Redis Error <RPOP %s>: %s\n ", group, err.Error())
		lib.LogError(errorMsg)
		return 0, errors.New(errorMsg)
	}
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
