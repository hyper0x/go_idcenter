package base

import (
	"time"
)

type GroupInfo struct {
	Name         string
	Start        uint64
	Step         uint32
	Count        uint64
	Range        IdRange
	LastModified time.Time
}

type IdRange struct {
	Begin uint64
	End   uint64
}

type Provider interface {
	Name() string
}

type CacheProvider interface {
	Name() string
	BuildList(group string, begin uint64, end uint64) (bool, error)
	Pop(group string) (uint64, error)
	Clear(group string) (bool, error)
}

type StorageProvider interface {
	Name() string
	BuildInfo(group string, start uint64, step uint32) (bool, error)
	Get(group string) (*GroupInfo, error)
	Propel(group string) (*IdRange, error)
	Clear(group string) (bool, error)
}
