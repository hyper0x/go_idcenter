package provider

import (
	"idcenter/lib"
	"testing"
)

func TestRedisCacheProvider(t *testing.T) {
	rcp := &RedisCacheProvider{
		name:            "Test Redis Cache Provider",
		redisServerIp:   "127.0.0.1",
		redisServerPort: 6379,
	}
	err := rcp.Initialize()
	if err != nil {
		t.Errorf("Initialize Error: %s", err.Error())
		t.FailNow()
	}
	group := "test"
	err = rcp.BuildList(group, 1, 100)
	if err != nil {
		t.Errorf("BuildList Error: %s", err.Error())
		t.FailNow()
	}
	var value uint64
	for i := 1; i < 100; i++ {
		value, err = rcp.Pop(group)
		if err != nil {
			t.Errorf("Pop Error: %s", err.Error())
			t.FailNow()
		}
	}
	value, err = rcp.Pop(group)
	if value != 0 || err == nil {
		t.FailNow()
	}
	switch err.(type) {
	case lib.EmptyListError:
		t.Logf("Pop from a empty list of group '%s'.", group)
	default:
		t.Errorf("Pop Error: %s", err.Error())
		t.FailNow()
	}
}
