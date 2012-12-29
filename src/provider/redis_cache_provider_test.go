package cache

import "testing"

func TestRedisCacheProvider(t *testing.T) {
	rcp := &RedisCacheProvider{
		name:"Test Redis Cache Provider",
		redisServerIp:"127.0.0.1",
		redisServerPort:6379,
	}
	err := rcp.Initialize()
	if err != nil {
		t.Errorf("Initialize Error: %s", err.Error())
	}
}
