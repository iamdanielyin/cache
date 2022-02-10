package cache

import (
	"fmt"
	"time"
)

type Cache interface {
	Getter

	TTL(key string) (time.Duration, bool)
	Set(key string, value interface{}, expiration ...time.Duration) error
	HasPrefix(s string, limit ...int) (map[string]string, error)
	HasSuffix(s string, limit ...int) (map[string]string, error)
	Contains(s string, limit ...int) (map[string]string, error)
	Incr(key string) (int, error)
	IncrBy(key string, step int) (int, error)
	IncrByFloat(key string, step float64) (float64, error)
	Del(keys ...string) error
	Close() error

	Next() Cache
	Previous() Cache
	SetNext(next Cache)
	SetPrevious(previous Cache)

	Publish(channel string, message interface{}) error
	Subscribe(channels []string, handler func(string, string)) error
	PSubscribe(patterns []string, handler func(string, string)) error
	RemoteSupport() bool
}

func NewCache(c *Config) (Cache, error) {
	if c == nil {
		return nil, fmt.Errorf(`cache: driver not specified`)
	}

	driverMapMu.RLock()
	defer driverMapMu.RUnlock()

	driver, ok := driverMap[c.Driver]
	if !ok {
		return nil, fmt.Errorf(`cache: unregistered driver: %s`, c.Driver)
	}
	return driver.NewCache(c.Options)
}

type Config struct {
	Driver  string
	Options map[string]interface{}
}

func NewMultiLevelCache(configs []Config) (Cache, error) {
	defaultErr := fmt.Errorf(`cache: at least two levels of structure and the last level must support remote messages`)
	if len(configs) < 2 {
		return nil, defaultErr
	}

	// 初始化
	var (
		caches []Cache
		newErr error
	)
	for index, config := range configs {
		cache, err := NewCache(&config)
		if err != nil {
			newErr = err
			break
		}
		caches = append(caches, cache)
		if index == len(configs)-1 && !cache.RemoteSupport() {
			newErr = defaultErr
			break
		}
	}
	if newErr != nil {
		for _, item := range caches {
			_ = item.Close()
		}
		return nil, newErr
	}

	// 设置层级
	for index, cache := range caches {
		if index == 0 {
			cache.SetNext(caches[index+1])
		} else if index > 0 && index < len(caches)-1 {
			cache.SetPrevious(caches[index-1])
			cache.SetNext(caches[index+1])
		} else if index == len(caches)-1 {
			cache.SetPrevious(caches[index-1])
		}
	}

	// 返回首级
	return caches[0], nil
}
