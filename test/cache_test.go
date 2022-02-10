package test

import (
	"github.com/iamdanielyin/cache"
	_ "github.com/iamdanielyin/cache/driver/ldb"
	_ "github.com/iamdanielyin/cache/driver/redis"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func init() {
	initEnvs()
}

func initEnvs() {
	data, _ := ioutil.ReadFile(".env")
	for _, item := range strings.Split(string(data), "\n") {
		if ss := strings.Split(item, "="); len(ss) == 2 {
			_ = os.Setenv(ss[0], ss[1])
		}
	}
}

func TestNewMultiLevelCache(t *testing.T) {
	inst, err := cache.NewMultiLevelCache([]cache.Config{
		{
			Driver: "ldb",
			Options: map[string]interface{}{
				"path": "tmp/",
			},
		},
		{
			Driver: "redis",
			Options: map[string]interface{}{
				"addrs":    []string{os.Getenv("REDIS_ADDR")},
				"password": os.Getenv("REDIS_PWD"),
				"db":       2,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.Set("foo1", "bar1"); err != nil {
		t.Fatal(err)
	}
	t.Log(inst.GetString("foo1"))

	if err := inst.Del("foo1"); err != nil {
		t.Fatal(err)
	}
	t.Log(inst.GetString("foo1"))
}
