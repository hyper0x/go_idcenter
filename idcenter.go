package go_idcenter

import (
	"bytes"
	"errors"
	"fmt"
	. "go_idcenter/base"
	"go_idcenter/lib"
	"go_lib"
	"reflect"
	"runtime/debug"
)

const (
	_ = iota
	DEFAULT_START
	DEFAULT_STEP = 1000
)

var cacheProviderMap = make(map[string]CacheProvider)
var storageProviderMap = make(map[string]StorageProvider)

func RegisterProvider(provider Provider) error {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			errorMsg := fmt.Sprintf("Occur FATAL error when register provider (provider=%v): %s", provider, err)
			go_lib.LogFatalln(errorMsg)
		}
	}()
	if provider == nil {
		panicMsg := "IdCenter: The provider is nil!\n"
		go_lib.LogFatal(panicMsg)
		panic(panicMsg)
	}
	name := provider.Name()
	if len(name) == 0 {
		panicMsg := "IdCenter: The name of provider is nil!\n"
		go_lib.LogFatal(panicMsg)
		panic(panicMsg)
	}
	switch t := interface{}(provider).(type) {
	case CacheProvider:
		if _, contains := cacheProviderMap[name]; contains {
			errorMsg := fmt.Sprintf("IdCenter: Repetitive cache provider name '%s'!\n", name)
			go_lib.LogError(errorMsg)
			return errors.New(errorMsg)
		}
		cp, ok := interface{}(provider).(CacheProvider)
		if !ok {
			errorMsg := fmt.Sprintf("IdCenter: Incorrect cache provider type! (name '%s')\n", name)
			go_lib.LogError(errorMsg)
			return errors.New(errorMsg)
		}
		cacheProviderMap[name] = cp
	case StorageProvider:
		if _, contains := storageProviderMap[name]; contains {
			errorMsg := fmt.Sprintf("IdCenter: Repetitive storage provider name '%s'!\n", name)
			go_lib.LogError(errorMsg)
			return errors.New(errorMsg)
		}
		sp, ok := interface{}(provider).(StorageProvider)
		if !ok {
			errorMsg := fmt.Sprintf("IdCenter: Incorrect cache provider type! (name '%s')\n", name)
			go_lib.LogError(errorMsg)
			return errors.New(errorMsg)
		}
		storageProviderMap[name] = sp
	default:
		panicMsg := fmt.Sprintf("IdCenter: Unexpected Provider type '%v'! (name=%s)\n", t, name)
		go_lib.LogFatal(panicMsg)
		panic(panicMsg)
	}
	return nil
}

func UnregisterProvider(provider Provider) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			errorMsg := fmt.Sprintf("Occur FATAL error when unregister provider (provider=%v): %s", provider, err)
			go_lib.LogFatalln(errorMsg)
		}
	}()
	if provider == nil {
		panicMsg := "IdCenter: The provider is nil!\n"
		go_lib.LogFatal(panicMsg)
		panic(panicMsg)
	}
	name := provider.Name()
	if len(name) == 0 {
		panicMsg := "IdCenter: The name of provider is nil!\n"
		go_lib.LogFatal(panicMsg)
		panic(panicMsg)
	}
	switch t := interface{}(provider).(type) {
	case CacheProvider:
		_, contains := cacheProviderMap[name]
		if contains {
			delete(cacheProviderMap, name)
		} else {
			go_lib.LogWarnf("IdCenter: The cache Provider named '%s' is NOTEXISTENT!\n", name)
		}
	case StorageProvider:
		_, contains := storageProviderMap[name]
		if contains {
			delete(storageProviderMap, name)
		} else {
			go_lib.LogWarnf("IdCenter: The storage Provider named '%s' is NOTEXISTENT!\n", name)
		}
	default:
		panicMsg := fmt.Sprintf("IdCenter: Unexpected Provider type '%v'! (name=%s)\n", t, name)
		go_lib.LogFatal(panicMsg)
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
			go_lib.LogFatalln(errorMsg)
		}
	}()
	cacheProvider := self.getCacheProvider()
	storageProvider := self.getStorageProvider()
	id, err := cacheProvider.Pop(group)
	if err != nil {
		switch err.(type) {
		case *lib.EmptyListError:
			warningMsg := fmt.Sprintf("Warning: The list of group '%s' is empty.", group)
			go_lib.LogWarn(warningMsg)
		default:
			errorMsg := fmt.Sprintf("Occur error when pop id for group '%s': %s\n", group, err.Error())
			go_lib.LogError(errorMsg)
			return 0, err
		}
	}
	if id > 0 {
		return id, nil
	}
	go_lib.LogInfof("Prepare check & build id list for group '%s'...\n", group)
	groupInfo, err := storageProvider.Get(group)
	if err != nil {
		errorMsg := fmt.Sprintf("Occur error when get group (name='%s') info : %s\n", group, err.Error())
		go_lib.LogError(errorMsg)
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
			go_lib.LogErrorln(errorMsg)
			return 0, err
		}
		if !ok {
			warnMsg := fmt.Sprintf("Building group info is FAILING. Maybe the group already exists. (group=%v)", group)
			go_lib.LogWarnln(warnMsg)
		}
	}
	idRange, err := storageProvider.Propel(group)
	if err != nil {
		errorMsg := fmt.Sprintf("Occur error when propel id for group '%s': %s\n", group, err.Error())
		go_lib.LogError(errorMsg)
		return 0, err
	}
	currentBegin := idRange.Begin
	currentEnd := idRange.End
	ok, err := cacheProvider.BuildList(group, currentBegin, currentEnd)
	if err != nil {
		errorMsg := fmt.Sprintf("Occur error when build id list for group '%s': %s\n", group, err.Error())
		go_lib.LogError(errorMsg)
		return 0, err
	}
	if !ok {
		warnMsg := fmt.Sprintf("Building id list is FAILING. (group=%v)", group)
		go_lib.LogWarnln(warnMsg)
	}
	id, err = cacheProvider.Pop(group)
	if err != nil {
		switch err.(type) {
		case *lib.EmptyListError:
			warningMsg := fmt.Sprintf("Warning: The list of group '%s' is empty.", group)
			go_lib.LogWarn(warningMsg)
		default:
			errorMsg := fmt.Sprintf("Occur error when pop id for group '%s': %s\n", group, err.Error())
			go_lib.LogError(errorMsg)
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
			go_lib.LogFatalln(errorMsg)
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

func (self *IdCenterManager) getCacheProvider() CacheProvider {
	cacheProvider, contains := cacheProviderMap[self.CacheProviderName]
	if !contains {
		panicMsg := fmt.Sprintf("IdCenter: The cache Provider named '%s' is NOTEXISTENT! Please register the provider.\n", self.CacheProviderName)
		panic(panicMsg)
	}
	return cacheProvider
}

func (self *IdCenterManager) getStorageProvider() StorageProvider {
	storageProvider, contains := storageProviderMap[self.StorageProviderName]
	if !contains {
		panicMsg := fmt.Sprintf("IdCenter: The storage Provider named '%s' is NOTEXISTENT! Please register the provider.\n", self.StorageProviderName)
		panic(panicMsg)
	}
	return storageProvider
}
