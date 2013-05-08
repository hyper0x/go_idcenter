package manager

import (
	"bytes"
	"errors"
	"fmt"
	"go_idcenter/base"
	"reflect"
	"runtime/debug"
)

const (
	_ = iota
	DEFAULT_START
	DEFAULT_STEP = 1000
)

var cacheProviderMap = make(map[string]base.CacheProvider)
var storageProviderMap = make(map[string]base.StorageProvider)

func RegisterProvider(provider base.Provider) error {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			errorMsg := fmt.Sprintf("Occur FATAL error when register provider (provider=%v): %s", provider, err)
			base.Logger().Fatalln(errorMsg)
		}
	}()
	if provider == nil {
		panicMsg := "IdCenter: The provider is nil!\n"
		base.Logger().Fatal(panicMsg)
		panic(panicMsg)
	}
	name := provider.Name()
	if len(name) == 0 {
		panicMsg := "IdCenter: The name of provider is nil!\n"
		base.Logger().Fatal(panicMsg)
		panic(panicMsg)
	}
	switch t := interface{}(provider).(type) {
	case base.CacheProvider:
		if _, contains := cacheProviderMap[name]; contains {
			errorMsg := fmt.Sprintf("IdCenter: Repetitive cache provider name '%s'!\n", name)
			base.Logger().Error(errorMsg)
			return errors.New(errorMsg)
		}
		cp, ok := interface{}(provider).(base.CacheProvider)
		if !ok {
			errorMsg := fmt.Sprintf("IdCenter: Incorrect cache provider type! (name '%s')\n", name)
			base.Logger().Error(errorMsg)
			return errors.New(errorMsg)
		}
		cacheProviderMap[name] = cp
	case base.StorageProvider:
		if _, contains := storageProviderMap[name]; contains {
			errorMsg := fmt.Sprintf("IdCenter: Repetitive storage provider name '%s'!\n", name)
			base.Logger().Error(errorMsg)
			return errors.New(errorMsg)
		}
		sp, ok := interface{}(provider).(base.StorageProvider)
		if !ok {
			errorMsg := fmt.Sprintf("IdCenter: Incorrect cache provider type! (name '%s')\n", name)
			base.Logger().Error(errorMsg)
			return errors.New(errorMsg)
		}
		storageProviderMap[name] = sp
	default:
		panicMsg := fmt.Sprintf("IdCenter: Unexpected Provider type '%v'! (name=%s)\n", t, name)
		base.Logger().Fatal(panicMsg)
		panic(panicMsg)
	}
	return nil
}

func UnregisterProvider(provider base.Provider) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			errorMsg := fmt.Sprintf("Occur FATAL error when unregister provider (provider=%v): %s", provider, err)
			base.Logger().Fatalln(errorMsg)
		}
	}()
	if provider == nil {
		panicMsg := "IdCenter: The provider is nil!\n"
		base.Logger().Fatal(panicMsg)
		panic(panicMsg)
	}
	name := provider.Name()
	if len(name) == 0 {
		panicMsg := "IdCenter: The name of provider is nil!\n"
		base.Logger().Fatal(panicMsg)
		panic(panicMsg)
	}
	switch t := interface{}(provider).(type) {
	case base.CacheProvider:
		_, contains := cacheProviderMap[name]
		if contains {
			delete(cacheProviderMap, name)
		} else {
			base.Logger().Warnf("IdCenter: The cache Provider named '%s' is NOTEXISTENT!\n", name)
		}
	case base.StorageProvider:
		_, contains := storageProviderMap[name]
		if contains {
			delete(storageProviderMap, name)
		} else {
			base.Logger().Warnf("IdCenter: The storage Provider named '%s' is NOTEXISTENT!\n", name)
		}
	default:
		panicMsg := fmt.Sprintf("IdCenter: Unexpected Provider type '%v'! (name=%s)\n", t, name)
		base.Logger().Fatal(panicMsg)
		panic(panicMsg)
	}
	return
}

type IdCenterManager struct {
	CacheProviderName   string
	StorageProviderName string
	Start               uint64
	Step                uint32
}

func (self *IdCenterManager) GetId(group string) (uint64, error) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			errorMsg := fmt.Sprintf("Occur FATAL error when get id (group=%v): %s", group, err)
			base.Logger().Fatalln(errorMsg)
		}
	}()
	cacheProvider := self.getCacheProvider()
	storageProvider := self.getStorageProvider()
	id, err := cacheProvider.Pop(group)
	if err != nil {
		switch err.(type) {
		case *base.EmptyListError:
			warningMsg := fmt.Sprintf("Warning: The list of group '%s' is empty.", group)
			base.Logger().Warn(warningMsg)
		default:
			errorMsg := fmt.Sprintf("Occur error when pop id for group '%s': %s\n", group, err.Error())
			base.Logger().Error(errorMsg)
			return 0, err
		}
	}
	if id > 0 {
		return id, nil
	}
	base.Logger().Infof("Prepare check & build id list for group '%s'...\n", group)
	groupInfo, err := storageProvider.Get(group)
	if err != nil {
		errorMsg := fmt.Sprintf("Occur error when get group (name='%s') info : %s\n", group, err.Error())
		base.Logger().Error(errorMsg)
		return 0, err
	}
	if groupInfo == nil {
		currentStart := self.Start
		if currentStart <= 0 {
			currentStart = DEFAULT_START
		}
		currentStep := self.Step
		if currentStep <= 0 {
			currentStep = DEFAULT_STEP
		}
		ok, err := storageProvider.BuildInfo(group, currentStart, currentStep)
		if err != nil {
			errorMsg := fmt.Sprintf("Occur error when initialize group '%s': %s", group, err.Error())
			base.Logger().Errorln(errorMsg)
			return 0, err
		}
		if !ok {
			warnMsg := fmt.Sprintf("Building group info is FAILING. Maybe the group already exists. (group=%v)", group)
			base.Logger().Warnln(warnMsg)
		}
	}
	idRange, err := storageProvider.Propel(group)
	if err != nil {
		errorMsg := fmt.Sprintf("Occur error when propel id for group '%s': %s\n", group, err.Error())
		base.Logger().Error(errorMsg)
		return 0, err
	}
	currentBegin := idRange.Begin
	currentEnd := idRange.End
	ok, err := cacheProvider.BuildList(group, currentBegin, currentEnd)
	if err != nil {
		errorMsg := fmt.Sprintf("Occur error when build id list for group '%s': %s\n", group, err.Error())
		base.Logger().Error(errorMsg)
		return 0, err
	}
	if !ok {
		warnMsg := fmt.Sprintf("Building id list is FAILING. (group=%v)", group)
		base.Logger().Warnln(warnMsg)
	}
	id, err = cacheProvider.Pop(group)
	if err != nil {
		switch err.(type) {
		case *base.EmptyListError:
			warningMsg := fmt.Sprintf("Warning: The list of group '%s' is empty.", group)
			base.Logger().Warn(warningMsg)
		default:
			errorMsg := fmt.Sprintf("Occur error when pop id for group '%s': %s\n", group, err.Error())
			base.Logger().Error(errorMsg)
			return 0, err
		}
	}
	return id, nil
}

func (self *IdCenterManager) Clear(group string) (bool, error) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			errorMsg := fmt.Sprintf("Occur FATAL error when clear (group=%v): %s", group, err)
			base.Logger().Fatalln(errorMsg)
		}
	}()
	storageProvider := self.getStorageProvider()
	spResult, spErr := storageProvider.Clear(group)
	cacheProvider := self.getCacheProvider()
	cpResult, cpErr := cacheProvider.Clear(group)
	if spErr != nil || cpErr != nil {
		var errorMsgBuffer bytes.Buffer
		errorMsgBuffer.WriteString("Clear Failing:")
		if spErr != nil {
			errorMsgBuffer.WriteString(fmt.Sprintf("%s: %s; ", reflect.TypeOf(storageProvider).Name(), spErr))
		}
		if cpErr != nil {
			errorMsgBuffer.WriteString(fmt.Sprintf("%s: %s; ", reflect.TypeOf(cacheProvider).Name(), cpErr))
		}
		errorMsgBuffer.WriteString("\n")
		return false, errors.New(errorMsgBuffer.String())
	}
	return (spResult && cpResult), nil
}

func (self *IdCenterManager) getCacheProvider() base.CacheProvider {
	cacheProvider, contains := cacheProviderMap[self.CacheProviderName]
	if !contains {
		panicMsg := fmt.Sprintf("IdCenter: The cache Provider named '%s' is NOTEXISTENT! Please register the provider.\n", self.CacheProviderName)
		panic(panicMsg)
	}
	return cacheProvider
}

func (self *IdCenterManager) getStorageProvider() base.StorageProvider {
	storageProvider, contains := storageProviderMap[self.StorageProviderName]
	if !contains {
		panicMsg := fmt.Sprintf("IdCenter: The storage Provider named '%s' is NOTEXISTENT! Please register the provider.\n", self.StorageProviderName)
		panic(panicMsg)
	}
	return storageProvider
}
