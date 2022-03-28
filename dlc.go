package dlc

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type Fetch[K comparable, R any] func(keys []K) ([]*R, error)

type Cache interface {
	SaveExpire(string, time.Duration, []byte)
	GetExpire(string, time.Duration) ([]byte, bool)
	Clear(...string)
}

// LoaderConfig captures the config to create a new Loader
type Config struct {
	// Wait is how long wait before sending a batch
	Wait time.Duration

	// Cache Time
	CacheTime time.Duration

	// MaxBatch will limit the maximum number of keys to send in one batch, 0 = not limit
	MaxBatch int

	// Cache Key prefix
	Prefix string
}

func (c Config) NewWait(w time.Duration) Config {
	d := c
	d.Wait = w
	return d
}

func (c Config) NewCacheTime(t time.Duration) Config {
	d := c
	d.CacheTime = t
	return d
}

func (c Config) NewMaxBatch(m int) Config {
	d := c
	d.MaxBatch = m
	return d
}

func (c Config) NewPrefix(p string) Config {
	d := c
	d.Prefix = p
	return d
}

type Loader[K comparable, R any] struct {
	fetch Fetch[K, R]

	wait time.Duration

	maxBatch int

	prefix string

	cache     Cache
	cachetime time.Duration

	batch *batch[K, R]
	lock  *sync.Mutex
}

func NewLoader[K comparable, R any](config Config, cache Cache, f Fetch[K, R]) *Loader[K, R] {
	return &Loader[K, R]{
		fetch:     f,
		wait:      config.Wait,
		maxBatch:  config.MaxBatch,
		prefix:    config.Prefix,
		cache:     cache,
		cachetime: config.CacheTime,
		lock:      &sync.Mutex{},
	}
}

type batch[K comparable, R any] struct {
	l       *Loader[K, R]
	query   chan *result[K, R]
	err     error
	closing bool
	done    chan struct{}
	wg      *sync.WaitGroup
}

func (l *Loader[K, R]) Key(key K) string {
	return fmt.Sprintf("%v%v", l.prefix, key)
}

func (c Config) WithPrefix(p string) Config {
	d := c
	d.Prefix += p
	return d
}

func (l *Loader[K, R]) cb() {
	l.lock.Lock()
	l.batch = nil
	l.lock.Unlock()
}

func (l *Loader[K, R]) gb() *batch[K, R] {
	l.lock.Lock()
	b := l.batch
	if b == nil {
		b = newbatch(l, 1000)
		l.batch = b
	}
	b.wg.Add(1)
	l.lock.Unlock()
	return b
}

// Load a  by key, batching and caching will be applied automatically
func (l *Loader[K, R]) Load(key K) (*R, error) {
	return l.LoadThunk(key)()
}

func (l *Loader[K, R]) LoadThunk(key K) func() (*R, error) {
	if it, ok := l.cache.GetExpire(l.Key(key), l.cachetime); ok {
		return func() (*R, error) {
			return loaderWithBytes[R](it)
		}
	}
	b := l.gb()
	r := &result[K, R]{
		k: key,
	}
	b.query <- r

	return func() (*R, error) {
		<-b.done
		if b.err != nil {
			l.unsafeSet(key, r.v)
		}
		return r.v, b.err
	}
}

// LoadAll fetches many keys at once. It will be broken into appropriate sized
// sub batches depending on how the loader is configured
func (l *Loader[K, R]) LoadAll(keys []K) ([]*R, []error) {
	results := make([]func() (*R, error), len(keys))

	for i, key := range keys {
		results[i] = l.LoadThunk(key)
	}

	s := make([]*R, len(keys))
	errors := make([]error, len(keys))
	for i, thunk := range results {
		s[i], errors[i] = thunk()
	}
	return s, errors
}

// LoadAllThunk returns a function that when called will block waiting for a s.
// This method should be used if you want one goroutine to make requests to many
// different data loaders without blocking until the thunk is called.
func (l *Loader[K, R]) LoadAllThunk(keys []K) func() ([]*R, []error) {
	results := make([]func() (*R, error), len(keys))
	for i, key := range keys {
		results[i] = l.LoadThunk(key)
	}
	return func() ([]*R, []error) {
		s := make([]*R, len(keys))
		errors := make([]error, len(keys))
		for i, thunk := range results {
			s[i], errors[i] = thunk()
		}
		return s, errors
	}
}

// Clear the value at key from the cache, if it exists
func (l *Loader[K, R]) Clear(keys ...K) {
	nk := []string{}
	for _, v := range keys {
		nk = append(nk, l.Key(v))
	}

	l.cache.Clear(nk...)
}

func (l *Loader[K, R]) unsafeSet(key K, value *R) {
	data := []byte{}
	if value != nil {
		data, _ = json.Marshal(value)
	}
	l.cache.SaveExpire(l.Key(key), l.cachetime, data)
}

func loaderWithBytes[T any](value []byte) (*T, error) {
	o := new(T)
	return o, json.Unmarshal(value, o)
}

func newbatch[K comparable, R any](l *Loader[K, R], ql int) *batch[K, R] {
	b := &batch[K, R]{
		done:  make(chan struct{}),
		l:     l,
		query: make(chan *result[K, R], ql),
		wg:    &sync.WaitGroup{},
	}
	go b.start()
	return b
}

func (b *batch[K, R]) start() {
	rs := []*result[K, R]{}
	keys := []K{}
	t := time.After(time.Millisecond * 10)
L:
	for {
		select {
		case r := <-b.query:
			keys = append(keys, r.k)
			rs = append(rs, r)
			b.wg.Done()
			if len(rs) > b.l.maxBatch {
				b.l.cb()
				break L
			}
		case <-t:
			b.l.cb()
			break L
		}
	}
	go func() {
		b.wg.Wait()
		close(b.query)
	}()
	for r := range b.query {
		rs = append(rs, r)
		keys = append(keys, r.k)
		b.wg.Done()
	}

	vs, err := b.l.fetch(keys)
	if err != nil {
		b.err = err
	} else {
		for i, v := range vs {
			rs[i].v = v
		}
	}
	close(b.done)
}

type result[K comparable, R any] struct {
	k K
	v *R
}
