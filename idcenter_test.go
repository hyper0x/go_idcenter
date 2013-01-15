package go_idcenter

import (
	"errors"
	"fmt"
	"go_idcenter/base"
	"go_idcenter/provider"
	"testing"
)

func TestIdCenterManager(t *testing.T) {
	cp, sp, err := registerProviders()
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

func BenchmarkIdCenterManager(b *testing.B) {
	cp, sp, err := registerProviders()
	if err != nil {
		b.Errorf("Provider register error: %s", err)
		b.FailNow()
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
			b.Errorf("Clear Error: %s", err)
			b.FailNow()
		}
		b.Logf("Clear Result: %v", result)
		UnregisterProvider(cp)
		UnregisterProvider(sp)
	}()
	var previousId uint64
	var currentId uint64
	for i := 1; i <= b.N; i++ {
		currentId, err = idCenterManager.GetId(group)
		if err != nil {
			b.Errorf("Get id error: %s", err)
		}
		if previousId == 0 {
			if currentId != start {
				b.Logf("The id '%d' is not equals start '%d'.", currentId, start)
				b.FailNow()
			}
		} else {
			expectedId := previousId + 1
			if currentId != expectedId {
				b.Logf("The id '%d' is not equals '%d' (%d + 1).", currentId, expectedId, previousId)
				b.FailNow()
			}
		}
		previousId = currentId
	}
}

func registerProviders() (base.CacheProvider, base.StorageProvider, error) {
	rcp := getRedisCacheProvider()
	err := RegisterProvider(interface{}(rcp).(base.Provider))
	if err != nil {
		errorMsg := fmt.Sprintf("Redis Cache provider register error: %s", err)
		return nil, nil, errors.New(errorMsg)
	}
	msp := getMysqlStorageProvider()
	err = RegisterProvider(interface{}(msp).(base.Provider))
	if err != nil {
		errorMsg := fmt.Sprintf("MySQL Storage provider register error: %s", err)
		return nil, nil, errors.New(errorMsg)
	}
	return rcp, msp, nil
}

func getRedisCacheProvider() base.CacheProvider {
	parameter := provider.CacheParameter{
		Name:     "Test Redis Cache Provider",
		Ip:       "127.0.0.1",
		Port:     6379,
		PoolSize: uint16(3),
	}
	return interface{}(*provider.NewCacheProvider(parameter)).(base.CacheProvider)
}

func getMysqlStorageProvider() base.StorageProvider {
	parameter := provider.StorageParameter{
		Name:     "Test MySQL Storage Provider",
		Ip:       "127.0.0.1",
		Port:     3306,
		DbName:   "idcenter",
		User:     "root",
		Password: "haolin",
		PoolSize: uint16(3),
	}
	return interface{}(*provider.NewStorageProvider(parameter)).(base.StorageProvider)
}
