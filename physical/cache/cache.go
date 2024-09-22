/*
CREATE DATABASE openbao PRECISION 'ns' KEEP 3650 DURATION 10 BUFFER 16;

USE opencache;

CREATE STABLE supercache  (
    ts timestamp,
	k  VARCHAR(4096),
	v  VARBINARY(60000)
) TAGS (
    NamespaceID VARCHAR(1024),
    Name VARCHAR(64)
);
*/

package cache

import (
	"context"
	"time"

	log "github.com/hashicorp/go-hclog"
	"github.com/openbao/openbao/helper/namespace"
	"github.com/openbao/openbao/physical/tdengine"
	"github.com/openbao/openbao/sdk/v2/physical"

	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

// Cache is a physical backend that stores data
// within TDEngine database.
type Cache struct {
	defaultExpiration time.Duration
	nsID, name        string
	td                *tdengine.TDEngineBackend
	logger            log.Logger
}

func New(conf map[string]string, nsID, name string, defaultExpiration time.Duration, logger log.Logger) (*Cache, error) {
	if conf == nil {
		conf = make(map[string]string)
	}

	newconf := make(map[string]string)
	for k, v := range conf {
		if k == "database" || k == "stable" {
			continue
		}
		newconf[k] = v
	}

	if database, ok := conf["database_cache"]; ok {
		newconf["database"] = database
	} else {
		newconf["database"] = "opencache"
	}
	if stable, ok := conf["stable_cache"]; ok {
		newconf["stable"] = stable
	} else {
		newconf["stable"] = "supercache"
	}
	if create, ok := conf["create_stable_cache"]; ok {
		newconf["create_stable"] = create
	} else {
		newconf["create_stable"] = `CREATE STABLE IF NOT EXISTS supercache ( ts timestamp, k VARCHAR(4096), v VARBINARY(60000) ) TAGS ( NamespaceID VARCHAR(1024), NamespacePath VARCHAR(64) )`
	}

	newconf["ns_id"] = nsID
	newconf["ns_path"] = name
	newconf["table"] = nsID + "_" + name

	m, err := tdengine.NewTDEngineBackend(newconf, logger)
	if err != nil {
		return nil, err
	}

	return &Cache{
		defaultExpiration: defaultExpiration,
		nsID:              nsID,
		name:              name,
		td:                m,
		logger:            logger,
	}, nil
}

func (m *Cache) Context() context.Context {
	return namespace.ContextWithNamespace(context.Background(), &namespace.Namespace{
		ID:             m.nsID,
		CustomMetadata: map[string]string{},
	})
}

func (m *Cache) Add(k string, x []byte, d time.Duration) error {
	if d == 0 {
		d = m.defaultExpiration
	}
	i := int64(0)
	if d > 0 {
		i = d.Nanoseconds()
	}

	ctx := m.Context()
	entry := &physical.Entry{Key: k, Value: x}
	return m.td.AddWithDuration(ctx, entry, i, 1)
}

func (m *Cache) Set(k string, x []byte, d time.Duration) error {
	if d == 0 {
		d = m.defaultExpiration
	}
	i := int64(0)
	if d > 0 {
		i = d.Nanoseconds()
	}

	ctx := m.Context()
	entry := &physical.Entry{Key: k, Value: x}
	return m.td.AddWithDuration(ctx, entry, i, 2)
}

func (m *Cache) SetDefault(k string, x []byte) error {
	return m.Set(k, x, 0)
}

func (m *Cache) get(k string) (*physical.Entry, bool) {
	i := int64(0)
	if m.defaultExpiration > 0 {
		i = m.defaultExpiration.Nanoseconds()
	}

	ctx := m.Context()
	entry, err := m.td.GetWithDuration(ctx, k, i)
	if err != nil {
		m.logger.Error("failed to get key", "key", k, "error", err)
		return nil, false
	}
	if entry == nil {
		return nil, false
	}

	return entry, true
}

func (m *Cache) Get(k string) ([]byte, bool) {
	entry, ok := m.get(k)
	if !ok {
		return nil, false
	}

	return entry.Value, true
}

func (m *Cache) GetWithExpiration(k string) ([]byte, time.Time, bool) {
	entry, ok := m.get(k)
	if !ok {
		return nil, time.Time{}, false
	}

	var expiration time.Time
	if err := expiration.UnmarshalBinary(entry.ValueHash); err != nil {
		m.logger.Error("failed to unmarshal expiration", "key", k, "error", err)
		return nil, time.Time{}, false
	}

	return entry.Value, expiration, true
}

func (m *Cache) Delete(k string) {
	ctx := m.Context()
	if err := m.td.Delete(ctx, k); err != nil {
		m.logger.Error("failed to delete key", "key", k, "error", err)
	}
}

type Item struct {
	Object     []byte
	Expiration int64
}

func (m *Cache) Items() map[string]Item {
	ctx := m.Context()
	entries, err := m.td.Items(ctx)
	if err != nil {
		m.logger.Error("failed to list keys", "error", err)
		return nil
	}

	items := make(map[string]Item, len(entries))
	for _, entry := range entries {
		var expiration time.Time
		if err := expiration.UnmarshalBinary(entry.ValueHash); err != nil {
			m.logger.Error("failed to unmarshal expiration", "key", entry.Key, "time", string(entry.ValueHash), "error", err)
			return nil
		}
		items[entry.Key] = Item{
			Object:     entry.Value,
			Expiration: expiration.UnixNano(),
		}
	}

	return items
}
