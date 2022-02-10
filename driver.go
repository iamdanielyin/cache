package cache

import (
	"github.com/pkg/errors"
	"sync"
)

var (
	driverMap   = make(map[string]Driver)
	driverMapMu sync.RWMutex
)

type Driver interface {
	Name() string
	NewCache(map[string]interface{}) (Cache, error)
}

func RegisterDriver(driver Driver) {
	driverMapMu.Lock()
	defer driverMapMu.Unlock()

	name := driver.Name()
	if name == "" {
		panic(errors.New(`cache: missing driver name`))
	}
	driverMap[name] = driver
}

func UnregisterDriver(name string) {
	driverMapMu.Lock()
	defer driverMapMu.Unlock()

	delete(driverMap, name)
}
