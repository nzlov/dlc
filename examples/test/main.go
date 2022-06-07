package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/nzlov/dlc"
)

type _cache struct {
	m *sync.Map
}

func (c *_cache) SaveExpire(key string, t time.Duration, data []byte) {
	c.m.Store(key, data)
}

func (c *_cache) GetExpire(key string, t time.Duration) ([]byte, bool) {
	data, ok := c.m.Load(key)
	if !ok {
		return []byte{}, false
	}
	return data.([]byte), ok
}

func (c *_cache) Clear(keys ...string) {
	for _, v := range keys {
		c.m.Delete(v)
	}
}

func (c *_cache) Out() {
	fmt.Println("C Out:")
	c.m.Range(func(k, v any) bool {
		fmt.Println(k, string(v.([]byte)))
		return true
	})
}

func main() {
	config := dlc.Config{
		Wait:      time.Millisecond * 100,
		CacheTime: time.Minute,
		MaxBatch:  50,
		Prefix:    "a",
	}
	cache := &_cache{
		m: &sync.Map{},
	}
	strLoader := dlc.NewLoader(config.NewPrefix("string:"), cache, func(keys []int) ([]*int, error) {
		vs := make([]*int, len(keys))
		for i, v := range keys {
			nv := v
			vs[i] = &nv
		}
		fmt.Println("loaderlen:", len(keys), ":", len(vs))
		return vs, nil
	})
	var wg sync.WaitGroup

	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := i; j < i+100; j++ {
				// append slice 加锁解决并发安全问题
				v, err := strLoader.Load(j)
				if err != nil {
					panic(err)
				}
				if *v != j {
					cache.Out()
					panic(fmt.Sprintf("j[%v]<>v[%v]", j, *v))
				}
			}
			fmt.Println(i, "end")
		}(i)
	}
	wg.Wait()
}
