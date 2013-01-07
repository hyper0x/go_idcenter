package provider

import (
	"errors"
	"fmt"
	"github.com/ziutek/mymysql/autorc"
	_ "github.com/ziutek/mymysql/thrsafe"
	"go_idcenter/lib"
	"sync"
)

var initContext sync.Once
var mysqlConnPool *Pool
var iMysqlStorageProvider *mysqlStorageProvider

type StorageParameter struct {
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

func New(parameter StorageParameter) *mysqlStorageProvider {
	initContext.Do(func() {
		err := initialize(parameter)
		if err != nil {
			panic(err)
		}
	})
	return iMysqlStorageProvider
}

func initialize(parameter StorageParameter) error {
	mysqlServerAddr := fmt.Sprintf("%v:%v", parameter.Ip, parameter.Port)
	lib.LogInfof("Initialize mysql storage provider (parameter=%v)...", parameter)
	mysqlConnPool := &Pool{Id:"MySQL Connection Pool", Size:int(parameter.PoolSize)}
	initFunc := func() (interface{}, error) {
		conn := autorc.New("tcp", "", mysqlServerAddr, parameter.User, parameter.Password)
		err := conn.Use(parameter.DbName))
		if err != nil {
			errorMsg := fmt.Sprintf("Occur error when mysql connection initialization (parameter=%v): %s\n", parameter, err)
			lib.LogErrorln(errorMsg)
			return nil, err
		}
		return conn, nil
	}
	err := mysqlConnPool.Init(initFunc)
	if err != nil {
		errorMsg := fmt.Sprintf("Occur error when mysql connection pool initialization (parameter=%v): %s\n", parameter, err)
		lib.LogErrorln(errorMsg)
		return errors.New(errorMsg)
	}
	iMysqlStorageProvider = &mysqlStorageProvider{parameter.Name}
	return nil
}

