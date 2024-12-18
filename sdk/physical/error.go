// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package physical

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	log "github.com/hashicorp/go-hclog"
)

const (
	// DefaultErrorPercent is used to determin how often we error
	DefaultErrorPercent = 20
)

// ErrorInjector is used to add errors into underlying physical requests
type ErrorInjector struct {
	backend      Backend
	errorPercent int
	randomLock   *sync.Mutex
	random       *rand.Rand
}

// Verify ErrorInjector satisfies the correct interfaces
var (
	_ Backend = (*ErrorInjector)(nil)
)

// NewErrorInjector returns a wrapped physical backend to inject error
func NewErrorInjector(b Backend, errorPercent int, logger log.Logger) *ErrorInjector {
	if errorPercent < 0 || errorPercent > 100 {
		errorPercent = DefaultErrorPercent
	}
	logger.Info("creating error injector")

	return &ErrorInjector{
		backend:      b,
		errorPercent: errorPercent,
		randomLock:   new(sync.Mutex),
		random:       rand.New(rand.NewSource(int64(time.Now().Nanosecond()))),
	}
}

// oss start
func (e *ErrorInjector) GetBackend() Backend {
	return e.backend
}

// oss end

func (e *ErrorInjector) SetErrorPercentage(p int) {
	e.errorPercent = p
}

func (e *ErrorInjector) addError() error {
	e.randomLock.Lock()
	roll := e.random.Intn(100)
	e.randomLock.Unlock()
	if roll < e.errorPercent {
		return errors.New("random error")
	}

	return nil
}

func (e *ErrorInjector) Put(ctx context.Context, entry *Entry) error {
	if err := e.addError(); err != nil {
		return err
	}
	return e.backend.Put(ctx, entry)
}

func (e *ErrorInjector) Get(ctx context.Context, key string) (*Entry, error) {
	if err := e.addError(); err != nil {
		return nil, err
	}
	return e.backend.Get(ctx, key)
}

func (e *ErrorInjector) Delete(ctx context.Context, key string) error {
	if err := e.addError(); err != nil {
		return err
	}
	return e.backend.Delete(ctx, key)
}

func (e *ErrorInjector) List(ctx context.Context, prefix string) ([]string, error) {
	if err := e.addError(); err != nil {
		return nil, err
	}
	return e.backend.List(ctx, prefix)
}

func (e *ErrorInjector) ListPage(ctx context.Context, prefix string, after string, limit int) ([]string, error) {
	if err := e.addError(); err != nil {
		return nil, err
	}
	return e.backend.ListPage(ctx, prefix, after, limit)
}
