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

type StorageParameter struct {
	Name string
	Ip string
	Port int
	DbName string
	User string
	Password string
	PoolSize uint16
}

type mysqlConnPool struct {
	StorageParameter
	pool chan *autorc.Conn
}



func initPool(parameter StorageParameter) (*mysqlConnPool, error) {
	mysqlServerAddr := fmt.Sprintf("%v:%v", parameter.Ip, parameter.Port)
	lib.LogInfof("Initialize mysql storage provider (parameter=%v)...", parameter)
	iMysqlConnPool := &mysqlConnPool{parameter, make(chan *autorc.Conn)}
	for i := 0; i < parameter.PoolSize; i++ {
		conn := autorc.New("tcp", "", mysqlServerAddr, parameter.User, parameter.Password)
		err := conn.Use(dbname))
		if err != nil {
			errorMsg := fmt.Sprintf("Occur error when mysql connection pool initialization (parameter=%v): %s\n", parameter, err)
			lib.LogErrorln(errorMsg)
			return nil, err
		}
		iMysqlConnPool.pool <- conn
	}
}


