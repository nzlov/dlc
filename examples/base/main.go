package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/nzlov/dlc"
)

type Cache struct {
	m map[string][]byte
}

func (c *Cache) SaveExpire(key string, t time.Duration, data []byte) {
	c.m[key] = data
}

func (c *Cache) GetExpire(key string, t time.Duration) ([]byte, bool) {
	data, ok := c.m[key]
	return data, ok
}

func (c *Cache) Clear(keys ...string) {
	for _, v := range keys {
		delete(c.m, v)
	}
}

type A struct {
	a int

	intLoader2 *dlc.Loader[string, dlc.Config]
	intLoader3 *dlc.Loader[string, os.File]
	intLoader4 *dlc.Loader[string, os.File]
	intLoader  *dlc.Loader[string, int]
}

func main() {
	config := dlc.Config{
		Wait:      time.Second,
		CacheTime: time.Minute,
		MaxBatch:  100,
		Prefix:    "a",
	}
	cache := &Cache{
		m: map[string][]byte{},
	}

	strLoader := dlc.NewLoader(config.NewPrefix("string:"), cache, func(keys []string) ([]*string, error) {
		vs := []*string{}
		for _, v := range keys {
			vs = append(vs, &v)
		}
		return vs, nil
	})

	s, err := strLoader.Load("a")
	fmt.Println(*s, err)

	m := map[string]int{
		"a": 1,
		"b": 2,
	}
	a := A{}
	a.intLoader = dlc.NewLoader(config.WithPrefix("int:"), cache, func(keys []string) ([]*int, error) {
		fmt.Println(keys)
		vs := []*int{}
		for _, v := range keys {
			mv, ok := m[v]
			if ok {
				vs = append(vs, &mv)
			} else {
				vs = append(vs, nil)
				return nil, errors.New("not found")
			}
		}
		return vs, nil
	})
	i, errs := a.intLoader.LoadAll([]string{"a", "c"})
	fmt.Println(i, errs)
	i, errs = a.intLoader.LoadAll([]string{"a", "c"})
	fmt.Println(i, errs)
	fmt.Println(cache.m)
}
