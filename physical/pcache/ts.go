/*
CREATE DATABASE opencache PRECISION 'ns' KEEP 3650 DURATION 10 BUFFER 16;

USE opencache;

CREATE STABLE supercache  (
    ts timestamp,
	k  VARCHAR(4096),
	v  VARBINARY(60000)
) TAGS (
    NamespaceID VARCHAR(1024),
    NamespacePath VARCHAR(64)
);
*/

package pcache

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/openbao/openbao/api/v2"
	"github.com/openbao/openbao/helper/namespace"
	"github.com/openbao/openbao/physical/tdengine"
	"github.com/openbao/openbao/sdk/v2/physical"

	_ "github.com/taosdata/driver-go/v3/taosSql"
)

// TD is a physical backend that stores data
// within TDEngine database.
type TD struct {
	defaultExpiration time.Duration
	ns                *namespace.Namespace
	td                *tdengine.TDEngineBackend
}

var _ Cache = (*TD)(nil)

func NewTD(id, path string, defaultExpiration time.Duration, logger ...hclog.Logger) (*TD, error) {
	newconf := map[string]string{
		"database":       "opencache",
		"stable":         "supercache",
		"connection_url": "root:taosdata@tcp(vm0:6030)/opencache?readTimeout=20m",
	}
	if database := api.ReadBaoVariable("TDCACHE_DATABASE"); database != "" {
		newconf["database"] = database
	}
	if stable := api.ReadBaoVariable("TDCACHE_STABLE"); stable != "" {
		newconf["stable"] = stable
	}
	if connURL := api.ReadBaoVariable("TDCACHE_CONNECTION_URL"); connURL != "" {
		newconf["connection_url"] = connURL
	}
	newconf["create_stable"] = fmt.Sprintf(`CREATE STABLE IF NOT EXISTS %s.%s ( ts timestamp, k VARCHAR(4096), v VARBINARY(60000) ) TAGS ( NamespaceID VARCHAR(1024), NamespacePath VARCHAR(64) )`, newconf["database"], newconf["stable"])
	m, err := tdengine.NewTDEngineBackend(newconf, logger...)
	if err != nil {
		return nil, err
	}

	return &TD{
		defaultExpiration: defaultExpiration,
		ns:                &namespace.Namespace{ID: id, Path: path},
		td:                m,
	}, nil
}

func (m *TD) GetNS(ns *namespace.Namespace, key string, obj interface{}) (bool, error) {
	if ns == nil {
		return false, errNilNamespace
	}
	m.ns = ns
	return m.Get(key, obj)
}

func (m *TD) SetDefaultNS(ns *namespace.Namespace, key string, obj interface{}) error {
	if ns == nil {
		return errNilNamespace
	}
	m.ns = ns
	return m.SetDefault(key, obj)
}

func (m *TD) DeleteNS(ns *namespace.Namespace, key string) error {
	if ns == nil {
		return errNilNamespace
	}
	m.ns = ns
	return m.Delete(key)
}

func (m *TD) FlushNS(ns *namespace.Namespace) error {
	if ns == nil {
		return errNilNamespace
	}
	m.ns = ns
	return m.Flush()
}

func (m *TD) Context() context.Context {
	return namespace.ContextWithNamespace(context.Background(), m.ns)
}

func (m *TD) Add(k string, x interface{}, d time.Duration) error {
	ok, err := m.Get(k, x)
	if err != nil {
		return err
	}
	if ok {
		return m.Set(k, x, d)
	}
	return nil
}

func (m *TD) Set(k string, x interface{}, d time.Duration) error {
	if d == MarkerDefaultExpiration {
		d = m.defaultExpiration
	}
	if d <= 0 { // if the real expiration is less than or equal to 0, we don't store it
		return nil
	}

	bs, err := interfaceToBytes(x)
	if err != nil {
		return err
	}
	entry := &physical.Entry{Key: k, Value: bs}
	return m.td.AddWithDuration(m.Context(), entry, d.Nanoseconds(), 1)
}

func (m *TD) SetDefault(k string, x interface{}) error {
	return m.Set(k, x, m.defaultExpiration)
}

func (m *TD) getContext(ctx context.Context, k string) (*physical.Entry, error) {
	i := int64(0)
	if m.defaultExpiration > 0 {
		i = m.defaultExpiration.Nanoseconds()
	}

	entry, err := m.td.GetWithDuration(ctx, k, i)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

func (m *TD) Get(k string, x interface{}) (bool, error) {
	entry, err := m.getContext(m.Context(), k)
	if err != nil {
		return false, err
	}
	if entry == nil {
		return false, nil
	}

	if x == nil {
		return true, nil
	}

	return true, bytesToInterface(entry.Value, x)
}

func (m *TD) Increment(k string, n int64) error {
	entry, err := m.getContext(m.Context(), k)
	if err != nil {
		return err
	}
	if entry == nil {
		return nil
	}

	var x int64
	if err := bytesToInterface(entry.Value, &x); err != nil {
		return err
	}

	x += n
	bs, err := interfaceToBytes(x)
	if err != nil {
		return err
	}
	entry.Value = bs
	return m.td.AddWithDuration(m.Context(), entry, m.defaultExpiration.Nanoseconds(), 1)
}

func (m *TD) GetWithExpiration(k string, x interface{}) (time.Time, error) {
	entry, err := m.getContext(m.Context(), k)
	if err != nil {
		return time.Time{}, err
	}
	if entry == nil {
		return time.Time{}, nil
	}

	var expiration time.Time
	if entry.ValueHash != nil {
		if err := bytesToInterface(entry.ValueHash, &expiration); err != nil {
			return expiration, err
		}
	}

	err = bytesToInterface(entry.Value, x)
	return expiration, err
}

func (m *TD) Delete(k string) error {
	return m.td.Delete(m.Context(), k)
}

func (m *TD) Flush() error {
	return m.td.Flush(m.Context())
}

func (m *TD) Items(object interface{}) (map[string]Item, error) {
	entries, err := m.td.Items(m.Context())
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}

	return entriesToItems(entries, object)
}

func entriesToItems(entries []*physical.Entry, object interface{}) (map[string]Item, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	items := make(map[string]Item)
	for _, entry := range entries {
		var nano int64
		if entry.ValueHash != nil {
			var expiration time.Time
			if err := bytesToInterface(entry.ValueHash, &expiration); err != nil {
				return nil, err
			} else {
				nano = expiration.UnixNano()
			}
		}
		if err := bytesToInterface(entry.Value, object); err != nil {
			return nil, err
		}
		v := reflect.ValueOf(object)
		tmp := reflect.New(reflect.TypeOf(object))
		tmp.Elem().Set(v)
		items[entry.Key] = Item{Object: tmp.Elem().Elem().Interface(), Expiration: nano}
	}

	return items, nil
}
