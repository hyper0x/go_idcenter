package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"go_idcenter/base"
	"go_idcenter/manager"
	"go_idcenter/provider"
	"go_lib"
	"net/http"
	"strconv"
)

var serverPort int
var iConfig go_lib.Config
var idCenterManager manager.IdCenterManager

func init() {
	flag.IntVar(&serverPort, "port", 9092, "the server (http listen) port")
	iConfig = go_lib.Config{Path: base.CONFIG_FILE_NAME}
	err := iConfig.ReadConfig(false)
	if err != nil {
		errorMsg := fmt.Sprintf("Config Loading error: %s", err)
		go_lib.LogFatalf(errorMsg)
		panic(errors.New(errorMsg))
	}
	configRedisPort := iConfig.Dict["redis_server_port"]
	redisPort, err := strconv.Atoi(configRedisPort)
	if err != nil {
		errorMsg := fmt.Sprintf("The redis server port '%v' is INVALID! Error: %s", configRedisPort, err)
		go_lib.LogFatalf(errorMsg)
		panic(errors.New(errorMsg))
	}
	configRedisPoolSize := iConfig.Dict["redis_server_pool_size"]
	redisPoolSize, err := strconv.Atoi(configRedisPoolSize)
	if err != nil {
		errorMsg := fmt.Sprintf("The redis server pool size '%v' is INVALID! Error: %s", redisPoolSize, err)
		go_lib.LogFatalf(errorMsg)
		panic(errors.New(errorMsg))
	}
	cacheParameter := provider.RedisParameter{
		Name:     "Redis Cache Provider",
		Ip:       iConfig.Dict["redis_server_ip"],
		Port:     redisPort,
		Password: iConfig.Dict["redis_server_password"],
		PoolSize: uint16(redisPoolSize),
	}
	rcp := manager.NewRedisCacheProvider(cacheParameter)
	err = manager.RegisterProvider(interface{}(rcp).(base.Provider))
	if err != nil {
		errorMsg := fmt.Sprintf("Redis Cache provider register error: %s", err)
		go_lib.LogFatalf(errorMsg)
		panic(errors.New(errorMsg))
	}
	configMysqlPort := iConfig.Dict["mysql_server_port"]
	mysqlPort, err := strconv.Atoi(configMysqlPort)
	if err != nil {
		errorMsg := fmt.Sprintf("The mysql server port '%v' is INVALID! Error: %s", configMysqlPort, err)
		go_lib.LogFatalf(errorMsg)
		panic(errors.New(errorMsg))
	}
	configMysqlPoolSize := iConfig.Dict["mysql_server_pool_size"]
	mysqlPoolSize, err := strconv.Atoi(configMysqlPoolSize)
	if err != nil {
		errorMsg := fmt.Sprintf("The mysql server pool size '%v' is INVALID! Error: %s", configMysqlPoolSize, err)
		go_lib.LogFatalf(errorMsg)
		panic(errors.New(errorMsg))
	}
	storageParameter := provider.MysqlParameter{
		Name:     "Mysql Storage Provider",
		Ip:       iConfig.Dict["redis_server_ip"],
		Port:     mysqlPort,
		DbName:   iConfig.Dict["mysql_server_db_name"],
		User:     iConfig.Dict["mysql_server_user"],
		Password: iConfig.Dict["mysql_server_password"],
		PoolSize: uint16(mysqlPoolSize),
	}
	msp := manager.NewMysqlStorageProvider(storageParameter)
	err = manager.RegisterProvider(interface{}(msp).(base.Provider))
	if err != nil {
		errorMsg := fmt.Sprintf("MySQL Storage provider register error: %s", err)
		go_lib.LogFatalf(errorMsg)
		panic(errors.New(errorMsg))
	}
	configIdStart := iConfig.Dict["id_start"]
	idStart, err := strconv.Atoi(configIdStart)
	if err != nil {
		errorMsg := fmt.Sprintf("The start number of id '%v' is INVALID! Error: %s", configIdStart, err)
		go_lib.LogFatalf(errorMsg)
		panic(errors.New(errorMsg))
	}
	configIdStep := iConfig.Dict["id_step"]
	idStep, err := strconv.Atoi(configIdStep)
	if err != nil {
		errorMsg := fmt.Sprintf("The step number of id '%v' is INVALID! Error: %s", configIdStep, err)
		go_lib.LogFatalf(errorMsg)
		panic(errors.New(errorMsg))
	}
	idCenterManager = manager.IdCenterManager{
		CacheProviderName:   rcp.Name(),
		StorageProviderName: msp.Name(),
		Start:               uint64(idStart),
		Step:                uint32(idStep),
	}
}

func doForId(w http.ResponseWriter, r *http.Request) {
	hj, ok := w.(http.Hijacker)
	var errorMsg string
	if !ok {
		errorMsg = "The Web Server does not support Hijacking! "
		http.Error(w, errorMsg, http.StatusInternalServerError)
		go_lib.LogErrorf(errorMsg)
		return
	}
	conn, bufrw, err := hj.Hijack()
	if err != nil {
		errorMsg = "Internal error!"
		http.Error(w, errorMsg, http.StatusInternalServerError)
		go_lib.LogErrorf(errorMsg+" Hijacking Error: %s\n", err)
		return
	}
	defer conn.Close()
	r.ParseForm()
	group := r.FormValue("group")
	op := r.FormValue("op")
	go_lib.LogInfof("Receive a request for id (group=%s, op=%s))...\n", group, op)
	var respContent interface{}
	if op == "clear" {
		result, err := idCenterManager.Clear(group)
		if err != nil {
			errorMsg = fmt.Sprintf("Clear id group error: %s", err)
			go_lib.LogErrorln(errorMsg)
		}
		respContent = interface{}(result)
	} else {
		currentId, err := idCenterManager.GetId(group)
		if err != nil {
			errorMsg = fmt.Sprintf("Get id error: %s", err)
			go_lib.LogErrorln(errorMsg)
		}
		respContent = interface{}(currentId)
	}
	if len(errorMsg) > 0 {
		respContent = interface{}("Internal error!")
	}
	pushResponse(bufrw, respContent, group, op)
}

func pushResponse(bufrw *bufio.ReadWriter, content interface{}, group string, op string) {
	literals := fmt.Sprintf("%v", content)
	_, err := bufrw.Write([]byte(literals))
	if err == nil {
		err = bufrw.Flush()
	}
	if err != nil {
		go_lib.LogErrorf("Pushing response error (content=%v, group=%s, op=%s): %s\n", literals, group, op, err)
	} else {
		go_lib.LogErrorf("The response '%v' has been pushed. (group=%s, op=%s)\n", literals, group, op)
	}
}

func main() {
	flag.Parse()
	http.HandleFunc("/id", doForId)
	go_lib.LogInfof("Starting id center http server (port=%d)...\n", serverPort)
	err := http.ListenAndServe(":"+fmt.Sprintf("%d", serverPort), nil)
	if err != nil {
		go_lib.LogFatalln("Listen and serve error: ", err)
	} else {
		go_lib.LogInfoln("The id center http server is started.")
	}
}
