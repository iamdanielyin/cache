package redis

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/iamdanielyin/cache"
	"github.com/iamdanielyin/cache/json"
	"strings"
	"time"
)

func init() {
	cache.RegisterDriver(&redisDriver{})
}

type redisDriver struct {
}

func (r *redisDriver) Name() string {
	return "redis"
}

const connectChannel = "CONNECT_CHANNEL"

func (r *redisDriver) NewCache(config map[string]interface{}) (cache.Cache, error) {
	opts := new(redis.UniversalOptions)
	if err := json.Copy(config, &opts); err != nil {
		return nil, fmt.Errorf(`cache: parse redis options failed: %s`, err.Error())
	}

	cmd := redis.NewUniversalClient(opts)
	if _, err := cmd.Ping(context.Background()).Result(); err != nil {
		return nil, err
	}
	inst := &redisCache{rdb: cmd}
	err := inst.Subscribe([]string{connectChannel}, func(channel string, data string) {
		if data == "" {
			return
		}

		if inst.previous != nil {
			keys := strings.Split(data, ",")
			_ = inst.previous.Del(keys...)
		}
	})
	return inst, err
}

type redisCache struct {
	rdb      redis.UniversalClient
	next     cache.Cache
	previous cache.Cache
}

func (r *redisCache) SetNext(next cache.Cache) {
	r.next = next
}

func (r *redisCache) SetPrevious(previous cache.Cache) {
	r.previous = previous
}

func (r *redisCache) Publish(channel string, message interface{}) error {
	return r.rdb.Publish(context.Background(), channel, message).Err()
}

func (r *redisCache) Subscribe(channels []string, handler func(string, string)) error {
	ps := r.rdb.Subscribe(context.Background(), channels...)
	if _, err := ps.Receive(context.Background()); err != nil {
		return err
	}
	ch := ps.Channel()
	go func(ch <-chan *redis.Message) {
		for msg := range ch {
			handler(msg.Channel, msg.Payload)
		}
	}(ch)
	return nil
}

func (r *redisCache) PSubscribe(patterns []string, handler func(string, string)) error {
	pubsub := r.rdb.PSubscribe(context.Background(), patterns...)
	if _, err := pubsub.Receive(context.Background()); err != nil {
		return err
	}
	ch := pubsub.Channel()
	go func(ch <-chan *redis.Message) {
		for msg := range ch {
			handler(msg.Channel, msg.Payload)
		}
	}(ch)
	return nil
}

func (r *redisCache) TTL(path string) (time.Duration, bool) {
	dur, err := r.rdb.TTL(context.Background(), path).Result()
	return dur, err == nil
}

func (r *redisCache) Has(key string) bool {
	has := r.rdb.Exists(context.Background(), key).Val() > 1
	if !has && r.next != nil {
		has = r.next.Has(key)
	}
	return has
}

func (r *redisCache) HasGet(key string, dst interface{}) bool {
	s, err := r.rdb.Get(context.Background(), key).Result()
	has := err == nil
	if has && s != "" {
		var v struct {
			ExpiredDuration int64       `json:"expired_duration"`
			CreatedAt       time.Time   `json:"created_at"`
			Data            interface{} `json:"data"`
		}
		if err = json.Parse(s, &v); err == nil {
			err = json.Copy(v.Data, dst)
		}
	} else if r.next != nil {
		has = r.next.HasGet(key, dst)
		var ttl time.Duration
		ttl, has = r.next.TTL(key)
		if has {
			_ = r.Set(key, dst, ttl)
		}
	}
	return has
}

func (r *redisCache) HasGetInt(key string) (int, bool) {
	s, err := r.rdb.Get(context.Background(), key).Result()
	has := err == nil
	if !has && r.next != nil {
		var v int
		v, has = r.next.HasGetInt(key)
		var ttl time.Duration
		ttl, has = r.next.TTL(key)
		if has {
			_ = r.Set(key, v, ttl)
		}
		return v, has
	}
	var v struct {
		ExpiredDuration int64     `json:"expired_duration"`
		CreatedAt       time.Time `json:"created_at"`
		Data            int       `json:"data"`
	}
	err = json.Parse(s, &v)
	return v.Data, has
}

func (r *redisCache) HasGetInt8(key string) (int8, bool) {
	v, has := r.HasGetInt(key)
	return int8(v), has
}

func (r *redisCache) HasGetInt16(key string) (int16, bool) {
	v, has := r.HasGetInt(key)
	return int16(v), has
}

func (r *redisCache) HasGetInt32(key string) (int32, bool) {
	v, has := r.HasGetInt(key)
	return int32(v), has
}

func (r *redisCache) HasGetInt64(key string) (int64, bool) {
	v, has := r.HasGetInt(key)
	return int64(v), has
}

func (r *redisCache) HasGetUint(key string) (uint, bool) {
	v, has := r.HasGetUint64(key)
	return uint(v), has
}

func (r *redisCache) HasGetUint8(key string) (uint8, bool) {
	v, has := r.HasGetUint64(key)
	return uint8(v), has
}

func (r *redisCache) HasGetUint16(key string) (uint16, bool) {
	v, has := r.HasGetUint64(key)
	return uint16(v), has
}

func (r *redisCache) HasGetUint32(key string) (uint32, bool) {
	v, has := r.HasGetUint64(key)
	return uint32(v), has
}

func (r *redisCache) HasGetUint64(key string) (uint64, bool) {
	v, has := r.HasGetInt(key)
	return uint64(v), has
}

func (r *redisCache) HasGetFloat(key string) (float64, bool) {
	s, err := r.rdb.Get(context.Background(), key).Result()
	has := err == nil
	if !has && r.next != nil {
		var v float64
		v, has = r.next.HasGetFloat(key)
		var ttl time.Duration
		ttl, has = r.next.TTL(key)
		if has {
			_ = r.Set(key, v, ttl)
		}
		return v, has
	}
	var v struct {
		ExpiredDuration int64     `json:"expired_duration"`
		CreatedAt       time.Time `json:"created_at"`
		Data            float64   `json:"data"`
	}
	err = json.Parse(s, &v)
	return v.Data, has
}

func (r *redisCache) HasGetFloat32(key string) (float32, bool) {
	v, has := r.HasGetFloat(key)
	return float32(v), has
}

func (r *redisCache) HasGetFloat64(key string) (float64, bool) {
	return r.HasGetFloat(key)
}

func (r *redisCache) HasGetString(key string) (string, bool) {
	s, err := r.rdb.Get(context.Background(), key).Result()
	has := err == nil
	if !has && r.next != nil {
		var v string
		v, has = r.next.HasGetString(key)
		var ttl time.Duration
		ttl, has = r.next.TTL(key)
		if has {
			_ = r.Set(key, v, ttl)
		}
		return v, has
	}
	var v struct {
		ExpiredDuration int64     `json:"expired_duration"`
		CreatedAt       time.Time `json:"created_at"`
		Data            string    `json:"data"`
	}
	err = json.Parse(s, &v)
	return v.Data, has
}

func (r *redisCache) HasGetBool(key string) (bool, bool) {
	s, err := r.rdb.Get(context.Background(), key).Result()
	has := err == nil
	if !has && r.next != nil {
		var v bool
		v, has = r.next.HasGetBool(key)
		var ttl time.Duration
		ttl, has = r.next.TTL(key)
		if has {
			_ = r.Set(key, v, ttl)
		}
		return v, has
	}
	var v struct {
		ExpiredDuration int64     `json:"expired_duration"`
		CreatedAt       time.Time `json:"created_at"`
		Data            bool      `json:"data"`
	}
	err = json.Parse(s, &v)
	return v.Data, has
}

func (r *redisCache) HasGetTime(key string) (time.Time, bool) {
	var v time.Time
	has := r.HasGet(key, &v)
	return v, has
}

func (r *redisCache) Get(key string, dst interface{}) {
	_ = r.HasGet(key, dst)
}

func (r *redisCache) GetInt(key string) int {
	v, _ := r.HasGetInt(key)
	return v
}

func (r *redisCache) GetInt8(key string) int8 {
	v, _ := r.HasGetInt8(key)
	return v
}

func (r *redisCache) GetInt16(key string) int16 {
	v, _ := r.HasGetInt16(key)
	return v
}

func (r *redisCache) GetInt32(key string) int32 {
	v, _ := r.HasGetInt32(key)
	return v
}

func (r *redisCache) GetInt64(key string) int64 {
	v, _ := r.HasGetInt64(key)
	return v
}

func (r *redisCache) GetUint(key string) uint {
	v, _ := r.HasGetUint(key)
	return v
}

func (r *redisCache) GetUint8(key string) uint8 {
	v, _ := r.HasGetUint8(key)
	return v
}

func (r *redisCache) GetUint16(key string) uint16 {
	v, _ := r.HasGetUint16(key)
	return v
}

func (r *redisCache) GetUint32(key string) uint32 {
	v, _ := r.HasGetUint32(key)
	return v
}

func (r *redisCache) GetUint64(key string) uint64 {
	v, _ := r.HasGetUint64(key)
	return v
}

func (r *redisCache) GetFloat(key string) float64 {
	v, _ := r.HasGetFloat(key)
	return v
}

func (r *redisCache) GetFloat32(key string) float32 {
	v, _ := r.HasGetFloat32(key)
	return v
}

func (r *redisCache) GetFloat64(key string) float64 {
	v, _ := r.HasGetFloat64(key)
	return v
}

func (r *redisCache) GetString(key string) string {
	v, _ := r.HasGetString(key)
	return v
}

func (r *redisCache) GetBool(key string) bool {
	v, _ := r.HasGetBool(key)
	return v
}

func (r *redisCache) GetTime(key string) time.Time {
	v, _ := r.HasGetTime(key)
	return v
}

func (r *redisCache) DefaultGet(key string, dst interface{}, defaultValue interface{}) {
	if !r.HasGet(key, dst) {
		_ = json.Copy(defaultValue, dst)
	}
}

func (r *redisCache) DefaultGetInt(key string, defaultValue int) int {
	if v, has := r.HasGetInt(key); has {
		return v
	}
	return defaultValue
}

func (r *redisCache) DefaultGetInt8(key string, defaultValue int8) int8 {
	if v, has := r.HasGetInt8(key); has {
		return v
	}
	return defaultValue
}

func (r *redisCache) DefaultGetInt16(key string, defaultValue int16) int16 {
	if v, has := r.HasGetInt16(key); has {
		return v
	}
	return defaultValue
}

func (r *redisCache) DefaultGetInt32(key string, defaultValue int32) int32 {
	if v, has := r.HasGetInt32(key); has {
		return v
	}
	return defaultValue
}

func (r *redisCache) DefaultGetInt64(key string, defaultValue int64) int64 {
	if v, has := r.HasGetInt64(key); has {
		return v
	}
	return defaultValue
}

func (r *redisCache) DefaultGetUint(key string, defaultValue uint) uint {
	if v, has := r.HasGetUint(key); has {
		return v
	}
	return defaultValue
}

func (r *redisCache) DefaultGetUint8(key string, defaultValue uint8) uint8 {
	if v, has := r.HasGetUint8(key); has {
		return v
	}
	return defaultValue
}

func (r *redisCache) DefaultGetUint16(key string, defaultValue uint16) uint16 {
	if v, has := r.HasGetUint16(key); has {
		return v
	}
	return defaultValue
}

func (r *redisCache) DefaultGetUint32(key string, defaultValue uint32) uint32 {
	if v, has := r.HasGetUint32(key); has {
		return v
	}
	return defaultValue
}

func (r *redisCache) DefaultGetUint64(key string, defaultValue uint64) uint64 {
	if v, has := r.HasGetUint64(key); has {
		return v
	}
	return defaultValue
}

func (r *redisCache) DefaultGetFloat(key string, defaultValue float64) float64 {
	if v, has := r.HasGetFloat(key); has {
		return v
	}
	return defaultValue
}

func (r *redisCache) DefaultGetFloat32(key string, defaultValue float32) float32 {
	if v, has := r.HasGetFloat32(key); has {
		return v
	}
	return defaultValue
}

func (r *redisCache) DefaultGetFloat64(key string, defaultValue float64) float64 {
	if v, has := r.HasGetFloat64(key); has {
		return v
	}
	return defaultValue
}

func (r *redisCache) DefaultGetString(key string, defaultValue string) string {
	if v, has := r.HasGetString(key); has {
		return v
	}
	return defaultValue
}

func (r *redisCache) DefaultGetBool(key string, defaultValue bool) bool {
	if v, has := r.HasGetBool(key); has {
		return v
	}
	return defaultValue
}

func (r *redisCache) DefaultGetTime(key string, defaultValue time.Time) time.Time {
	if v, has := r.HasGetTime(key); has {
		return v
	}
	return defaultValue
}

func (r *redisCache) Next() cache.Cache {
	return r.next
}

func (r *redisCache) Previous() cache.Cache {
	return r.previous
}

func (r *redisCache) HasPrefix(s string, limit ...int) (map[string]string, error) {
	v, err := r.contains(fmt.Sprintf("%s*", s), limit...)

	if err == nil && len(v) == 0 && r.next != nil {
		return r.next.HasPrefix(s, limit...)
	}

	return v, err
}

func (r *redisCache) HasSuffix(s string, limit ...int) (map[string]string, error) {
	v, err := r.contains(fmt.Sprintf("*%s", s), limit...)

	if err == nil && len(v) == 0 && r.next != nil {
		return r.next.HasSuffix(s, limit...)
	}

	return v, err
}

func (r *redisCache) Contains(s string, limit ...int) (map[string]string, error) {
	v, err := r.contains(fmt.Sprintf("*%s*", s), limit...)

	if err == nil && len(v) == 0 && r.next != nil {
		return r.next.Contains(s, limit...)
	}

	return v, err
}

func (r *redisCache) Set(key string, value interface{}, expiration ...time.Duration) error {
	var dur time.Duration
	if len(expiration) > 0 {
		dur = expiration[0]
	}
	cv := struct {
		ExpiredDuration time.Duration `json:"expired_duration"`
		CreatedAt       time.Time     `json:"created_at"`
		Data            interface{}   `json:"data"`
	}{
		ExpiredDuration: dur,
		CreatedAt:       time.Now(),
		Data:            value,
	}
	v := json.Stringify(&cv, false)
	err := r.rdb.Set(context.Background(), key, v, dur).Err()
	if err == nil && r.next != nil {
		err = r.next.Set(key, value, expiration...)
	}
	return err
}

func (r *redisCache) Incr(key string) (int, error) {
	if r.next != nil {
		return r.next.Incr(key)
	}

	v, err := r.rdb.Incr(context.Background(), key).Result()
	return int(v), err
}

func (r *redisCache) IncrBy(key string, step int) (int, error) {
	if r.next != nil {
		return r.next.IncrBy(key, step)
	}

	v, err := r.rdb.IncrBy(context.Background(), key, int64(step)).Result()
	return int(v), err
}

func (r *redisCache) IncrByFloat(key string, step float64) (float64, error) {
	if r.next != nil {
		return r.next.IncrByFloat(key, step)
	}

	return r.rdb.IncrByFloat(context.Background(), key, step).Result()
}

func (r *redisCache) Del(keys ...string) error {
	err := r.rdb.Del(context.Background(), keys...).Err()

	if r.next != nil {
		err = r.next.Del(keys...)
	} else {
		err = r.Publish(connectChannel, strings.Join(keys, ","))
	}

	return err
}

func (r *redisCache) Close() error {
	if r.rdb != nil {
		return r.rdb.Close()
	}
	return nil
}

func (r *redisCache) RemoteSupport() bool {
	return true
}

func (r *redisCache) contains(pattern string, limit ...int) (map[string]string, error) {

	var count int64
	if len(limit) > 0 {
		count = int64(limit[0])
	}

	var (
		cursor uint64
		values = make(map[string]string)
	)

	for {
		var keys []string
		var err error
		keys, cursor, err = r.rdb.Scan(context.Background(), cursor, pattern, count).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			s, err := r.rdb.Get(context.Background(), key).Result()
			if err != nil {
				return nil, err
			}
			var v struct {
				ExpiredDuration int64     `json:"expired_duration"`
				CreatedAt       time.Time `json:"created_at"`
				Data            string    `json:"data"`
			}
			if err = json.Parse(s, &v); err == nil {
				values[key] = v.Data
			}
		}
		if cursor == 0 {
			break
		}
	}

	return values, nil
}
