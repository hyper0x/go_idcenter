package provider

import (
	"go_idcenter/base"
	"runtime/debug"
	"testing"
)

func TestRedisCacheProvider(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			t.Errorf("Fatal Error: %s\n", err)
		}
	}()
	parameter := RedisParameter{
		Name:     "Test Redis Cache Provider",
		Ip:       "127.0.0.1",
		Port:     6379,
		PoolSize: uint16(3),
	}
	rcp := NewRedisCacheProvider(parameter)
	group := "test"

	// Build & Pop
	ok, err := rcp.BuildList(group, 1, 100)
	if err != nil {
		t.Errorf("BuildList Error: %s\n", err.Error())
		t.FailNow()
	}
	if !ok {
		t.Error("Building list is Failing!\n")
		t.FailNow()
	}
	var value uint64
	for i := 1; i < 100; i++ {
		value, err = rcp.Pop(group)
		if err != nil {
			t.Errorf("Pop Error: %s\n", err.Error())
			t.FailNow()
		}
	}
	value, err = rcp.Pop(group)
	if value != 0 || err == nil {
		t.FailNow()
	}
	switch err.(type) {
	case *base.EmptyListError:
		t.Logf("Pop from a empty list of group '%s'.\n", group)
	default:
		t.Errorf("Pop Error: %s", err.Error())
		t.FailNow()
	}

	// Build & Clear
	ok, err = rcp.BuildList(group, 1, 100)
	if err != nil {
		t.Errorf("BuildList Error: %s\n", err.Error())
		t.FailNow()
	}
	if !ok {
		t.Error("Building list is Failing!\n")
		t.FailNow()
	}
	ok, err = rcp.Clear(group)
	if err != nil {
		t.Errorf("Clear Error: %s", err.Error())
		t.FailNow()
	}
	if !ok {
		t.Error("Clear is Failing!\n")
		t.FailNow()
	}
}
