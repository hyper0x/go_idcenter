package provider

import (
	"errors"
	"fmt"
	"github.com/ziutek/mymysql/autorc"
	_ "github.com/ziutek/mymysql/thrsafe"
	. "go_idcenter/base"
	"go_lib"
	"go_lib/pool"
	"sync"
	"time"
)

const (
	TABLE_NAME = "group"
	TIMEOUT_MS = time.Duration(100)
)

type MysqlParameter struct {
	Name     string
	Ip       string
	Port     int
	DbName   string
	User     string
	Password string
	PoolSize uint16
}

type mysqlStorageProvider struct {
	ProviderName string
}

var storageInitContext sync.Once
var mysqlConnPool *pool.Pool
var signMap map[string]*go_lib.Sign
var iMysqlStorageProvider *mysqlStorageProvider

func NewMysqlStorageProvider(parameter MysqlParameter) *mysqlStorageProvider {
	storageInitContext.Do(func() {
		err := initializeForStorageProvider(parameter)
		if err != nil {
			panic(err)
		}
	})
	return iMysqlStorageProvider
}

func initializeForStorageProvider(parameter MysqlParameter) error {
	mysqlServerAddr := fmt.Sprintf("%v:%v", parameter.Ip, parameter.Port)
	Logger().Infof("Initialize mysql storage provider (parameter=%v)...", parameter)
	mysqlConnPool = &pool.Pool{Id: "MySQL Connection Pool", Size: int(parameter.PoolSize)}
	initFunc := func() (interface{}, error) {
		conn := autorc.New("tcp", "", mysqlServerAddr, parameter.User, parameter.Password)
		conn.Raw.Register("set names utf8")
		err := conn.Use(parameter.DbName)
		if err != nil {
			errorMsg := fmt.Sprintf("Occur error when mysql connection initialization (parameter=%v): %s\n", parameter, err)
			Logger().Errorln(errorMsg)
			return nil, err
		}
		return conn, nil
	}
	err := mysqlConnPool.Init(initFunc)
	if err != nil {
		errorMsg := fmt.Sprintf("Occur error when mysql connection pool initialization (parameter=%v): %s\n", parameter, err)
		Logger().Errorln(errorMsg)
		return errors.New(errorMsg)
	}
	signMap = make(map[string]*go_lib.Sign)
	iMysqlStorageProvider = &mysqlStorageProvider{parameter.Name}
	return nil
}

func getMysqlConnection() (*autorc.Conn, error) {
	element, ok := mysqlConnPool.Get(TIMEOUT_MS)
	if !ok {
		errorMsg := fmt.Sprintf("Getting mysql connection is FAILING!")
		return nil, errors.New(errorMsg)
	}
	if element == nil {
		errorMsg := fmt.Sprintf("The mysql connection is UNUSABLE!")
		return nil, errors.New(errorMsg)
	}
	var conn *autorc.Conn
	switch t := element.(type) {
	case *autorc.Conn:
		conn = element.(*autorc.Conn)
	default:
		errorMsg := fmt.Sprintf("The type of element in pool is UNMATCHED! (type=%v)", t)
		return nil, errors.New(errorMsg)
	}
	return conn, nil
}

func releaseMysqlConnection(conn *autorc.Conn) bool {
	if conn == nil {
		return false
	}
	result := mysqlConnPool.Put(conn, TIMEOUT_MS)
	return result
}

func (self mysqlStorageProvider) Name() string {
	return self.ProviderName
}

func (self mysqlStorageProvider) BuildInfo(group string, start uint64, step uint32) (bool, error) {
	if len(group) == 0 {
		errorMsg := fmt.Sprint("The group name is INVALID!")
		Logger().Errorln(errorMsg)
		return false, errors.New(errorMsg)
	}
	errorMsgPrefix := fmt.Sprintf("Occur error when build group info (group=%v, start=%v, step=%v)", group, start, step)
	conn, err := getMysqlConnection()
	defer releaseMysqlConnection(conn)
	if err != nil {
		errorMsg := fmt.Sprintf("%s: %s", errorMsgPrefix, err)
		Logger().Errorln(errorMsg)
		return false, errors.New(errorMsg)
	}
	groupInfo, err := self.get(conn, group)
	if err != nil {
		errorMsg := fmt.Sprintf("%s: %s", errorMsgPrefix, err)
		Logger().Errorln(errorMsg)
		return false, errors.New(errorMsg)
	}
	if groupInfo != nil {
		warnMsg := fmt.Sprintf("The group '%s' already exists. IGNORE group info building.", group)
		Logger().Warnln(warnMsg)
		return false, nil
	}
	creation_dt := formatTime(time.Now())
	rawSql := "insert `%s`(`name`, `start`, `step`, `count`, `begin`, `end`, `creation_dt`) values('%s', %v, %v, %v, %v, %v, '%v')"
	sql := fmt.Sprintf(rawSql, TABLE_NAME, group, start, step, 0, 0, 0, creation_dt)
	_, _, err = conn.QueryFirst(sql)
	if err != nil {
		errorMsg := fmt.Sprintf("%s (sql=%s): %s", errorMsgPrefix, sql, err)
		Logger().Errorln(errorMsg)
		return false, errors.New(errorMsg)
	}
	return true, nil
}

func (self mysqlStorageProvider) Get(group string) (*GroupInfo, error) {
	if len(group) == 0 {
		errorMsg := fmt.Sprint("The group name is INVALID!")
		Logger().Errorln(errorMsg)
		return nil, errors.New(errorMsg)
	}
	errorMsgPrefix := fmt.Sprintf("Occur error when get group info (group=%v)", group)
	conn, err := getMysqlConnection()
	defer releaseMysqlConnection(conn)
	if err != nil {
		errorMsg := fmt.Sprintf("%s: %s", errorMsgPrefix, err)
		Logger().Errorln(errorMsg)
		return nil, errors.New(errorMsg)
	}
	return self.get(conn, group)
}

func (self mysqlStorageProvider) get(conn *autorc.Conn, group string) (*GroupInfo, error) {
	errorMsgPrefix := fmt.Sprintf("Occur error when get group info (group=%v)", group)
	rawSql := "select `start`, `step`, `count`, `begin`, `end`, `last_modified` from `%s` where `name`='%s'"
	sql := fmt.Sprintf(rawSql, TABLE_NAME, group)
	row, _, err := conn.QueryFirst(sql)
	if err != nil {
		errorMsg := fmt.Sprintf("%s (sql=%s): %s", errorMsgPrefix, sql, err)
		Logger().Errorln(errorMsg)
		return nil, errors.New(errorMsg)
	}
	if row == nil {
		return nil, nil
	}
	start := row.Uint64(0)
	step := uint32(row.Uint(1))
	count := row.Uint64(2)
	begin := row.Uint64(3)
	end := row.Uint64(4)
	lastModified := row.Time(5, time.Local)
	idRange := IdRange{Begin: begin, End: end}
	groupInfo := GroupInfo{Name: group, Start: start, Step: step, Count: count, Range: idRange, LastModified: lastModified}
	return &groupInfo, nil
}

func (self mysqlStorageProvider) Propel(group string) (*IdRange, error) {
	if len(group) == 0 {
		errorMsg := fmt.Sprint("The group name is INVALID!")
		Logger().Errorln(errorMsg)
		return nil, errors.New(errorMsg)
	}
	sign := getSign(group)
	sign.Set()
	defer sign.Unset()
	if len(group) == 0 {
		errorMsg := fmt.Sprint("The group name is INVALID!")
		Logger().Errorln(errorMsg)
		return nil, errors.New(errorMsg)
	}
	errorMsgPrefix := fmt.Sprintf("Occur error when propel (group=%v)", group)
	conn, err := getMysqlConnection()
	defer releaseMysqlConnection(conn)
	if err != nil {
		errorMsg := fmt.Sprintf("%s: %s", errorMsgPrefix, err)
		Logger().Errorln(errorMsg)
		return nil, errors.New(errorMsg)
	}
	groupInfo, err := self.get(conn, group)
	if err != nil {
		errorMsg := fmt.Sprintf("%s: %s", errorMsgPrefix, err)
		Logger().Errorln(errorMsg)
		return nil, errors.New(errorMsg)
	}
	if groupInfo == nil {
		warnMsg := fmt.Sprintf("The group '%s' not exist. IGNORE propeling.", group)
		Logger().Warnln(warnMsg)
		return nil, nil
	}
	idRange := groupInfo.Range
	var newBegin, newEnd uint64
	if groupInfo.Count == 0 {
		newBegin = groupInfo.Start
		newEnd = groupInfo.Start + uint64(groupInfo.Step)
	} else {
		newBegin = idRange.End
		newEnd = idRange.End + uint64(groupInfo.Step)
	}
	newCount := groupInfo.Count + 1
	rawSql := "update `%s` set `begin`=%v, `end`=%v, `count`=%v where `name`='%s'"
	sql := fmt.Sprintf(rawSql, TABLE_NAME, newBegin, newEnd, newCount, group)
	_, _, err = conn.QueryFirst(sql)
	if err != nil {
		errorMsg := fmt.Sprintf("%s (sql=%s): %s", errorMsgPrefix, sql, err)
		Logger().Errorln(errorMsg)
		return nil, errors.New(errorMsg)
	}
	newIdRange := IdRange{Begin: newBegin, End: newEnd}
	return &newIdRange, nil
}

func (self mysqlStorageProvider) Clear(group string) (bool, error) {
	if len(group) == 0 {
		errorMsg := fmt.Sprint("The group name is INVALID!")
		Logger().Errorln(errorMsg)
		return false, errors.New(errorMsg)
	}
	errorMsgPrefix := fmt.Sprintf("Occur error when clear group info (group=%v)", group)
	conn, err := getMysqlConnection()
	defer releaseMysqlConnection(conn)
	if err != nil {
		errorMsg := fmt.Sprintf("%s: %s", errorMsgPrefix, err)
		Logger().Errorln(errorMsg)
		return false, errors.New(errorMsg)
	}
	rawSql := "delete from `%s` where `name`='%s'"
	sql := fmt.Sprintf(rawSql, TABLE_NAME, group)
	_, result, err := conn.QueryFirst(sql)
	if err != nil {
		errorMsg := fmt.Sprintf("%s (sql=%s): %s", errorMsgPrefix, sql, err)
		Logger().Errorln(errorMsg)
		return false, errors.New(errorMsg)
	}
	var affectedRows uint64 = 0
	if result != nil {
		affectedRows = result.AffectedRows()
	}
	Logger().Infof("MySQL Storage Provider: The group '%s' is cleared. (affectedRows=%v)", group, (affectedRows > 0))
	return true, nil
}

func getSign(group string) *go_lib.Sign {
	if len(group) == 0 {
		return nil
	}
	sign := signMap[group]
	if sign == nil {
		sign = go_lib.NewSign()
		signMap[group] = sign
	}
	return sign
}

func formatTime(t time.Time) string {
	return fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%03d",
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		t.Nanosecond()/100000)
}
