package manager

import (
	"errors"
	"fmt"
	"go_idcenter/base"
	"go_idcenter/provider"
	"testing"
	"time"
)

func TestIdCenterManager(t *testing.T) {
	cp, sp, err := registerProvidersForTest()
	if err != nil {
		t.Errorf("Provider register error: %s", err)
		t.FailNow()
	}
	start := uint64(1)
	step := uint32(100)
	idCenterManager := IdCenterManager{
		CacheProviderName:   cp.Name(),
		StorageProviderName: sp.Name(),
		Start:               start,
		Step:                step,
	}
	group := "id_center_manager_test"
	defer func() {
		result, err := idCenterManager.Clear(group)
		if err != nil {
			t.Errorf("Clear Error: %s", err)
			t.FailNow()
		}
		t.Logf("Clear Result: %v", result)
		UnregisterProvider(cp)
		UnregisterProvider(sp)
	}()
	var currentId uint64
	currentId, err = idCenterManager.GetId(group)
	if err != nil {
		t.Errorf("Get id error: %s", err)
	}
	if currentId != start {
		t.Logf("The id '%d' is not equals start '%d'.", currentId, start)
		t.FailNow()
	}
	currentId, err = idCenterManager.GetId(group)
	if err != nil {
		t.Errorf("Get id error: %s", err)
	}
	if currentId != (start + 1) {
		t.Logf("The id '%d' is not equals start '%d' plus 1.", currentId, start)
		t.FailNow()
	}
}

func TestIdCenterManagerForBenchmark(t *testing.T) {
	cp, sp, err := registerProvidersForTest()
	if err != nil {
		t.Errorf("Provider register error: %s", err)
		t.FailNow()
	}
	start := uint64(1)
	step := uint32(100)
	idCenterManager := IdCenterManager{
		CacheProviderName:   cp.Name(),
		StorageProviderName: sp.Name(),
		Start:               start,
		Step:                step,
	}
	group := "id_center_manager_bechmark"
	defer func() {
		result, err := idCenterManager.Clear(group)
		if err != nil {
			t.Errorf("Clear Error: %s", err)
			t.FailNow()
		}
		t.Logf("Clear Result: %v", result)
		UnregisterProvider(cp)
		UnregisterProvider(sp)
	}()
	var previousId uint64
	var currentId uint64
	loopNumbers := []int{1000, 2000, 5000, 10000}
	testNumber := len(loopNumbers)
	results := make([][]interface{}, testNumber)
	for j := 0; j < testNumber; j++ {
		loopNumber := loopNumbers[j]
		t.Logf("Testing by loop number '%d'...\n", loopNumber)
		ns1 := time.Now().UnixNano()
		for i := 1; i <= loopNumber; i++ {
			currentId, err = idCenterManager.GetId(group)
			if err != nil {
				t.Errorf("Get id error: %s", err)
			}
			if previousId == 0 {
				if currentId != start {
					t.Logf("The id '%d' is not equals start '%d'.", currentId, start)
					t.FailNow()
				}
			} else {
				expectedId := previousId + 1
				if currentId != expectedId {
					t.Logf("The id '%d' is not equals '%d' (%d + 1).", currentId, expectedId, previousId)
					t.FailNow()
				}
			}
			previousId = currentId
		}
		ns2 := time.Now().UnixNano()
		totalCostNs := ns2 - ns1
		totalCost := float64(totalCostNs) / float64(1000)
		eachCost := float64(totalCost) / float64(loopNumber)
		results[j] = []interface{}{loopNumber, totalCost, eachCost}
	}
	for _, n := range results {
		fmt.Printf("Benchmark Result (loopNumber=%d) - Total cost (microsecond): %f, Each cost (microsecond): %f.\n", n[0], n[1], n[2])
	}
}

func registerProvidersForTest() (base.CacheProvider, base.StorageProvider, error) {
	cacheParameter := provider.RedisParameter{
		Name:     "Test Redis Cache Provider",
		Ip:       "127.0.0.1",
		Port:     6379,
		PoolSize: uint16(3),
	}
	rcp := NewRedisCacheProvider(cacheParameter)
	err := RegisterProvider(interface{}(rcp).(base.Provider))
	if err != nil {
		errorMsg := fmt.Sprintf("Redis Cache provider register error: %s", err)
		return nil, nil, errors.New(errorMsg)
	}
	storageParameter := provider.MysqlParameter{
		Name:     "Test MySQL Storage Provider",
		Ip:       "127.0.0.1",
		Port:     3306,
		DbName:   "idcenter",
		User:     "root",
		Password: "haolin",
		PoolSize: uint16(3),
	}
	msp := NewMysqlStorageProvider(storageParameter)
	err = RegisterProvider(interface{}(msp).(base.Provider))
	if err != nil {
		errorMsg := fmt.Sprintf("MySQL Storage provider register error: %s", err)
		return nil, nil, errors.New(errorMsg)
	}
	return rcp, msp, nil
}
