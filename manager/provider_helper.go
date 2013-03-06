package manager

import (
	"go_idcenter/base"
	"go_idcenter/provider"
)

func NewRedisCacheProvider(parameter provider.RedisParameter) base.CacheProvider {
	return interface{}(*provider.NewRedisCacheProvider(parameter)).(base.CacheProvider)
}

func NewMysqlStorageProvider(parameter provider.MysqlParameter) base.StorageProvider {
	return interface{}(*provider.NewMysqlStorageProvider(parameter)).(base.StorageProvider)
}
