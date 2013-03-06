package go_idcenter

import (
	"go_idcenter/base"
	"go_idcenter/provider"
)

func NewRedisCacheProvider(parameter provider.CacheParameter) base.CacheProvider {
	return interface{}(*provider.NewRedisCacheProvider(parameter)).(base.CacheProvider)
}

func NewMysqlStorageProvider(parameter provider.StorageParameter) base.StorageProvider {
	return interface{}(*provider.NewMysqlStorageProvider(parameter)).(base.StorageProvider)
}
