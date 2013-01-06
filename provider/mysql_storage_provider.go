package provider

import (
	"errors"
	"fmt"
	"github.com/ziutek/mymysql/autorc"
	_ "github.com/ziutek/mymysql/thrsafe"
	"go-idcenter/lib"
	"strconv"
	"sync"
	"time"
)

var initContext sync.Once
var cacheMutex *sync.RWMutex
var iMysqlConnPool *mysqlConnPool
var iMysqlStorageProvider *mysqlStorageProvider

type mysqlConnPool struct {
	size uint16
	pool chan *autorc.Conn
}

type MysqlStorageParameter struct {
	Name string
	Ip string
	Port int
	DbName string
	User string
	Password string
	PoolSize uint16
}

type mysqlStorageProvider struct {
	ProviderName string
}

func New(parameter MysqlStorageParameter) *mysqlStorageProvider {
	initContext.Do(func() {
		err := initialize(parameter)
		if err != nil {
			panic(err)
		}
	})
	return iMysqlStorageProvider
}

func initialize(parameter MysqlStorageParameter) error {


	mysqlPool = &redis.Pool{
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
iMysqlStorageProvider = &mysqlStorageProvider{parameter.Name}
return nil
}

