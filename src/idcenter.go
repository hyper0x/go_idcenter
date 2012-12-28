package main

import (
	"fmt"
	"errors"
	"time"
	"lib/logging"
)

type ProviderType int

const(
	_ = iota
	CACHE_PROVIDER_TYPE ProviderType
	STORAGE_PROVIDER_TYPE ProviderType
	DEFAULT_START = 1
	DEFAULT_STEP = 1000
)

type GroupInfo struct {
	Name string
	Start uint64
	Range IdRange
	Step uint32
	Count uint64
    LastModified time.Duration
}

type IdRange struct {
	Begin uint64
	End uint64
}

type Provider interface {
	Name() string
}

type CacheProvider interface {
	Name() string
	BuildList(begin uint64, end uint64) error
	Pop() (uint64, error)
}

type StorageProvider interface {
	Name() string
	Initialize(group string, start uint64, step uint32) error
	Get(group string) (GroupInfo, error)
	Propel(group string) (IdRange, error)
}

var cacheProviderMap map[string]CacheProvider = map[string]CacheProvider{};

var storageProviderMap map[string]StorageProvider = map[string]StorageProvider{};

func RegisterCacheProvider(providerType ProviderType, provider Provider) error {
	if provider == nil {
		panicMsg := "IdCenter: The provider is nil!"
		logging.LogFatalln(panicMsg)
		panic(panicMsg)
	}
	name := provider.Name()
	if name == nil {
		panicMsg := "IdCenter: The name of provider is nil!\n"
		logging.LogFatalln(panicMsg)
		panic(panicMsg)
	}
	if providerType == nil {
		panicMsg := fmt.Sprintf("IdCenter: The type of provider is nil! (name=%s)\n", name)
		logging.LogFatalln(panicMsg)
		panic(panicMsg)
	}
	var providerTypeName string
	switch providerType {
	case CACHE_PROVIDER_TYPE:
		providerTypeName = "cache"
		if _, contains := cacheProviderMap[name]; contains {
			errorMsg := fmt.Sprintf("IdCenter: Repetitive %s provider name '%s'!\n", providerTypeName, name)
			logging.LogErrorln(errorMsg)
			return errors.New(errorMsg)
		}
		cacheProviderMap[name] = provider
	case STORAGE_PROVIDER_TYPE:
		providerTypeName = "storage"
		providerTypeName = "cache"
		if _, contains := storageProviderMap[name]; contains {
			errorMsg := fmt.Sprintf("IdCenter: Repetitive %s provider name '%s'!\n", providerTypeName, name)
			logging.LogErrorln(errorMsg)
			return errors.New(errorMsg)
		}
		storageProviderMap[name] = provider
	default:
		panicMsg := fmt.Sprintf("IdCenter: Unexpected Provider type '%d'! (name=%s)\n", providerType, name)
		logging.LogFatalln(panicMsg)
		panic(panicMsg)
	}
	return nil
}

type IdCenterManager struct {
	CacheProviderName string
	StorageProviderName string
	Start uint64
	Step uint32
}

func (self IdCenterManager) getId(group string) uint64 {
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
	id, err := cacheProvider.Pop()
	if err != nil {
		errorMsg := fmt.Sprintf("Occur error when pop id for group '%s': %s", group, err.Error())
		logging.LogErrorln(errorMsg)
		return err
	}
	if id != nil && id > 0 {
		return id
	}
	logging.LogInfof("Prepare check & build id list for group '%s'...", group)
	groupInfo, err := storageProvider.Get(group)
	if err != nil {
		errorMsg := fmt.Sprintf("Occur error when get group (name='%s') info : %s", group, err.Error())
		logging.LogErrorln(errorMsg)
		return err
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
		err = storageProvider.Initialize(group, currentStart, currentStep)
		if err != nil {
			errorMsg := fmt.Sprintf("Occur error when initialize group '%s': %s", group, err.Error())
			logging.LogErrorln(errorMsg)
			return err
		}
	}
	idRange, err := storageProvider.Propel(group)
	if err != nil {
		errorMsg := fmt.Sprintf("Occur error when propel id for group '%s': %s", group, err.Error())
		logging.LogErrorln(errorMsg)
		return err
	}
	currentBegin := idRange.Begin
	currentEnd := idRange.End
	cacheProvider.BuildList(currentBegin, currentEnd)
	if err != nil {
		errorMsg := fmt.Sprintf("Occur error when build id list for group '%s': %s", group, err.Error())
		logging.LogErrorln(errorMsg)
		return err
	}
	id, err = cacheProvider.Pop()
	if err != nil {
		errorMsg := fmt.Sprintf("Occur error when pop id for group '%s': %s", group, err.Error())
		logging.LogErrorln(errorMsg)
		return err
	}
	return id
}

