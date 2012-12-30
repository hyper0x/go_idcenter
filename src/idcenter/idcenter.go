package idcenter

import (
	"errors"
	"fmt"
	"idcenter/lib"
	"reflect"
	"time"
)

const (
	_             = iota
	DEFAULT_START = 1
	DEFAULT_STEP  = 1000
)

type GroupInfo struct {
	Name         string
	Start        uint64
	Range        IdRange
	Step         uint32
	Count        uint64
	LastModified time.Duration
}

type IdRange struct {
	Begin uint64
	End   uint64
}

type Provider interface {
	Name() string
	Initialize() error
}

type CacheProvider interface {
	Name() string
	Initialize() error
	BuildList(group string, begin uint64, end uint64) error
	Pop(group string) (uint64, error)
}

type StorageProvider interface {
	Name() string
	Initialize() error
	BuildInfo(group string, start uint64, step uint32) error
	Get(group string) (*GroupInfo, error)
	Propel(group string) (*IdRange, error)
}

var cacheProviderMap = make(map[string]CacheProvider)

var storageProviderMap = make(map[string]StorageProvider)

func RegisterCacheProvider(provider Provider) error {
	if provider == nil {
		panicMsg := "IdCenter: The provider is nil!\n"
		lib.LogFatal(panicMsg)
		panic(panicMsg)
	}
	name := provider.Name()
	if len(name) == 0 {
		panicMsg := "IdCenter: The name of provider is nil!\n"
		lib.LogFatal(panicMsg)
		panic(panicMsg)
	}
	switch provider.(type) {
	case CacheProvider:
		if _, contains := cacheProviderMap[name]; contains {
			errorMsg := fmt.Sprintf("IdCenter: Repetitive cache provider name '%s'!\n", name)
			lib.LogError(errorMsg)
			return errors.New(errorMsg)
		}
		cp, ok := interface{}(provider).(CacheProvider)
		if !ok {
			errorMsg := fmt.Sprintf("IdCenter: Incorrect cache provider type! (name '%s')\n", name)
			lib.LogError(errorMsg)
			return errors.New(errorMsg)
		}
		cacheProviderMap[name] = cp
	case StorageProvider:
		if _, contains := storageProviderMap[name]; contains {
			errorMsg := fmt.Sprintf("IdCenter: Repetitive storage provider name '%s'!\n", name)
			lib.LogError(errorMsg)
			return errors.New(errorMsg)
		}
		sp, ok := interface{}(provider).(StorageProvider)
		if !ok {
			errorMsg := fmt.Sprintf("IdCenter: Incorrect cache provider type! (name '%s')\n", name)
			lib.LogError(errorMsg)
			return errors.New(errorMsg)
		}
		storageProviderMap[name] = sp
	default:
		panicMsg := fmt.Sprintf("IdCenter: Unexpected Provider type '%v'! (name=%s)\n", reflect.TypeOf(provider), name)
		lib.LogFatal(panicMsg)
		panic(panicMsg)
	}
	return nil
}

type IdCenterManager struct {
	CacheProviderName   string
	StorageProviderName string
	Start               uint64
	Step                uint32
}

func (self *IdCenterManager) getId(group string) (uint64, error) {
	cacheProvider, contains := cacheProviderMap[self.CacheProviderName]
	if !contains {
		panicMsg := fmt.Sprintf("IdCenter: The cache Provider named '%s' is NOTEXISTENT!\n", self.CacheProviderName)
		panic(panicMsg)
	}
	storageProvider, contains := storageProviderMap[self.StorageProviderName]
	if !contains {
		panicMsg := fmt.Sprintf("IdCenter: The storage Provider named '%s' is NOTEXISTENT!\n", self.StorageProviderName)
		panic(panicMsg)
	}
	id, err := cacheProvider.Pop(group)
	if err != nil {
		switch err.(type) {
		case lib.EmptyListError:
			warningMsg := fmt.Sprintf("Warning: The list of group '%s' is empty.", group)
			lib.LogWarn(warningMsg)
		default:
			errorMsg := fmt.Sprintf("Occur error when pop id for group '%s': %s\n", group, err.Error())
			lib.LogError(errorMsg)
			return 0, err
		}
	}
	if id > 0 {
		return id, nil
	}
	lib.LogInfof("Prepare check & build id list for group '%s'...\n", group)
	groupInfo, err := storageProvider.Get(group)
	if err != nil {
		errorMsg := fmt.Sprintf("Occur error when get group (name='%s') info : %s\n", group, err.Error())
		lib.LogError(errorMsg)
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
		err = storageProvider.BuildInfo(group, currentStart, currentStep)
		if err != nil {
			errorMsg := fmt.Sprintf("Occur error when initialize group '%s': %s", group, err.Error())
			lib.LogErrorln(errorMsg)
			return 0, err
		}
	}
	idRange, err := storageProvider.Propel(group)
	if err != nil {
		errorMsg := fmt.Sprintf("Occur error when propel id for group '%s': %s\n", group, err.Error())
		lib.LogError(errorMsg)
		return 0, err
	}
	currentBegin := idRange.Begin
	currentEnd := idRange.End
	cacheProvider.BuildList(group, currentBegin, currentEnd)
	if err != nil {
		errorMsg := fmt.Sprintf("Occur error when build id list for group '%s': %s\n", group, err.Error())
		lib.LogError(errorMsg)
		return 0, err
	}
	id, err = cacheProvider.Pop(group)
	if err != nil {
		switch err.(type) {
		case lib.EmptyListError:
			warningMsg := fmt.Sprintf("Warning: The list of group '%s' is empty.", group)
			lib.LogWarn(warningMsg)
		default:
			errorMsg := fmt.Sprintf("Occur error when pop id for group '%s': %s\n", group, err.Error())
			lib.LogError(errorMsg)
			return 0, err
		}
	}
	return id, nil
}
