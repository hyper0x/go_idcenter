package provider

import (
	"go-idcenter/lib"
	"testing"
	"runtime/debug"
)

func TestRedisCacheProvider(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			lib.LogErrorf("Fatal Error: %s\n", err)
		}
	}()
	parameter := CacheParameter{
		Name:            "Test Redis Cache Provider",
		Ip:   "127.0.0.1",
		Port: 6379,
		PoolSize: uint16(3),
	}
	rcp := New(parameter)
	group := "test"
	err := rcp.BuildList(group, 1, 100)
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
	case *lib.EmptyListError:
		t.Logf("Pop from a empty list of group '%s'.", group)
	default:
		t.Errorf("Pop Error: %s", err.Error())
		t.FailNow()
	}
}
