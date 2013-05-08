package provider

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"go_idcenter/base"
	"go_lib"
	"strconv"
	"sync"
	"time"
)

var cacheInitContext sync.Once
var rwSignMap map[string]*go_lib.RWSign
var redisPool *redis.Pool
var iRedisCacheProvider *redisCacheProvider

type RedisParameter struct {
	Name     string
	Ip       string
	Port     int
	Password string
	PoolSize uint16
}

type redisCacheProvider struct {
	ProviderName string
}

func NewRedisCacheProvider(parameter RedisParameter) *redisCacheProvider {
	cacheInitContext.Do(func() {
		err := initializeForCacheProvider(parameter)
		if err != nil {
			panic(err)
		}
	})
	return iRedisCacheProvider
}

func initializeForCacheProvider(parameter RedisParameter) error {
	redisServerAddr := fmt.Sprintf("%v:%v", parameter.Ip, parameter.Port)
	base.Logger().Infof("Initialize redis cache provider (parameter=%v)...", parameter)
	redisPool = &redis.Pool{
		MaxIdle:     int(parameter.PoolSize),
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
	rwSignMap = make(map[string]*go_lib.RWSign)
	iRedisCacheProvider = &redisCacheProvider{parameter.Name}
	return nil
}

func (self redisCacheProvider) Name() string {
	return self.ProviderName
}

func (self redisCacheProvider) BuildList(group string, begin uint64, end uint64) (bool, error) {
	if len(group) == 0 {
		errorMsg := fmt.Sprint("The group name is INVALID!")
		base.Logger().Errorln(errorMsg)
		return false, errors.New(errorMsg)
	}
	rwSign := getRWSign(group)
	rwSign.Set()
	defer rwSign.Unset()
	if (begin <= 0) || (end <= 0) || (begin >= end) {
		errorMsg := fmt.Sprintf("Invalid Parameter(s)! (begin=%d, end=%d)\n", begin, end)
		base.Logger().Error(errorMsg)
		return false, errors.New(errorMsg)
	}
	conn := redisPool.Get()
	defer conn.Close()
	exists, err := redis.Bool(conn.Do("EXISTS", group))
	if err != nil {
		errorMsg := fmt.Sprintf("Redis Error <EXISTS %s>: %s\n ", group, err.Error())
		base.Logger().Errorf(errorMsg)
		return false, errors.New(errorMsg)
	}
	if exists {
		effectedKeys, err := redis.Int(conn.Do("DEL", group))
		if err != nil {
			errorMsg := fmt.Sprintf("Redis Error <DEL %s>: %s\n ", group, err.Error())
			base.Logger().Error(errorMsg)
			return false, errors.New(errorMsg)
		}
		if effectedKeys < 1 {
			warningMsg := fmt.Sprintf("Redis warning <DEL %s>: seemingly failed.\n ", group)
			base.Logger().Warn(warningMsg)
		}
	}
	for i := begin; i < end; i++ {
		length, err := redis.Int(conn.Do("LPUSH", group, i))
		if err != nil {
			errorMsg := fmt.Sprintf("Redis Error <LPUSH %s %d> (total_length=%d): %s\n ", group, i, length, err.Error())
			base.Logger().Error(errorMsg)
			return false, errors.New(errorMsg)
		}
	}
	base.Logger().Infof("The list of group '%s' is builded. (begin=%d, end=%d)\n", group, begin, end)
	return true, nil

}

func (self redisCacheProvider) Pop(group string) (uint64, error) {
	if len(group) == 0 {
		errorMsg := fmt.Sprint("The group name is INVALID!")
		base.Logger().Errorln(errorMsg)
		return 0, errors.New(errorMsg)
	}
	rwSign := getRWSign(group)
	rwSign.RSet()
	defer rwSign.RUnset()
	conn := redisPool.Get()
	defer conn.Close()
	value, err := conn.Do("RPOP", group)
	if err != nil {
		errorMsg := fmt.Sprintf("Redis Error <RPOP %s>: %s\n ", group, err.Error())
		base.Logger().Error(errorMsg)
		return 0, errors.New(errorMsg)
	}
	if value == nil {
		errorMsg := fmt.Sprintf("Empty List! (group=%s)", group)
		return 0, &base.EmptyListError{errorMsg}
	}
	baValue := value.([]uint8)
	number, err := strconv.ParseUint(string(baValue), 10, 64)
	if err != nil {
		errorMsg := fmt.Sprintf("Converting Error (value=%s): %s\n ", value, err.Error())
		base.Logger().Error(errorMsg)
		return 0, errors.New(errorMsg)
	}
	return number, nil
}

func (self redisCacheProvider) Clear(group string) (bool, error) {
	if len(group) == 0 {
		errorMsg := fmt.Sprint("The group name is INVALID!")
		base.Logger().Errorln(errorMsg)
		return false, errors.New(errorMsg)
	}
	rwSign := getRWSign(group)
	rwSign.RSet()
	defer rwSign.RUnset()
	conn := redisPool.Get()
	defer conn.Close()
	effectedKeys, err := redis.Int(conn.Do("DEL", group))
	if err != nil {
		errorMsg := fmt.Sprintf("Redis Error <DEL %s>: %s\n ", group, err.Error())
		base.Logger().Error(errorMsg)
		return false, errors.New(errorMsg)
	}
	base.Logger().Infof("Redis Cache Provider: The group '%s' is cleared. (affectedKeys=%v)", group, (effectedKeys > 0))
	return true, nil
}

func getRWSign(group string) *go_lib.RWSign {
	if len(group) == 0 {
		return nil
	}
	rwSign := rwSignMap[group]
	if rwSign == nil {
		rwSign = go_lib.NewRWSign()
		rwSignMap[group] = rwSign
	}
	return rwSign
}
