package provider

import (
	. "go_idcenter/base"
	// "runtime/debug"
	"testing"
)

func TestMysqlStorageProvider(t *testing.T) {
	// defer func() {
	// 	if err := recover(); err != nil {
	// 		debug.PrintStack()
	// 		lib.LogErrorf("Fatal Error: %s\n", err)
	// 	}
	// }()
	parameter := MysqlParameter{
		Name:     "Test MySQL Storage Provider",
		Ip:       "127.0.0.1",
		Port:     3306,
		DbName:   "idcenter",
		User:     "root",
		Password: "haolin",
		PoolSize: uint16(3),
	}
	msp := NewMysqlStorageProvider(parameter)
	group := "test"
	start := uint64(100)
	step := uint32(1000)

	// Build & Get & Propel
	ok, err := msp.BuildInfo(group, start, step)
	if err != nil {
		t.Errorf("BuildInfo Error: %s\n", err.Error())
		t.FailNow()
	}
	if !ok {
		t.Error("BuildInfo list is Failing!")
		t.FailNow()
	}
	groupInfo, err := msp.Get(group)
	if err != nil {
		t.Errorf("Get Error: %s", err.Error())
		t.FailNow()
	}
	if groupInfo == nil {
		t.Error("Not group info!\n")
		t.FailNow()
	}
	if groupInfo.Name != group || groupInfo.Start != start || groupInfo.Step != step {
		t.Error("Not same group info!\n")
		t.FailNow()
	}
	var idRange *IdRange
	var begin, end uint64 = start, start + uint64(step)
	for i := 1; i <= 100; i++ {
		idRange, err = msp.Propel(group)
		if err != nil {
			t.Errorf("Propel Error: %s", err.Error())
			t.FailNow()
		}
		if idRange == nil {
			t.Errorf("Not id range! (%v)", i)
			t.FailNow()
		}
		if idRange.Begin != begin {
			t.Errorf("Not same begin! (%v, %v!=%v)", i, idRange.Begin, begin)
			t.FailNow()
		}
		if idRange.End != end {
			t.Errorf("Not same end! (%v, %v!=%v)", i, idRange.End, end)
			t.FailNow()
		}
		begin = end
		end = end + uint64(step)
	}

	// Clear
	ok, err = msp.Clear(group)
	if err != nil {
		t.Errorf("Clear Error: %s\n", err.Error())
		t.FailNow()
	}
	if !ok {
		t.Error("Clear list is Failing!")
		t.FailNow()
	}
}
