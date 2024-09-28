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

package cache

import (
	"context"
	"encoding/json"
	"reflect"
	"time"
	"unsafe"

	"github.com/hashicorp/go-hclog"
	"github.com/openbao/openbao/helper/namespace"
	"github.com/openbao/openbao/physical/tdengine"
	"github.com/openbao/openbao/sdk/v2/physical"

	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

const (
	NoExpiration      = -1
	DefaultExpiration = 0
)

// Cache is a physical backend that stores data
// within TDEngine database.
type Cache struct {
	defaultExpiration time.Duration
	nsID, name        string
	td                *tdengine.TDEngineBackend
	logger            hclog.Logger
}

func New(conf map[string]string, nsID, name string, defaultExpiration time.Duration, logger hclog.Logger) (*Cache, error) {
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
		newconf["create_stable"] = `CREATE STABLE IF NOT EXISTS opencache.supercache ( ts timestamp, k VARCHAR(4096), v VARBINARY(60000) ) TAGS ( NamespaceID VARCHAR(1024), NamespacePath VARCHAR(64) )`
	}

	newconf["ns_id"] = nsID
	newconf["ns_path"] = name
	newconf["table"] = nsID + "_" + name
	if newconf["connection_url"] == "" {
		newconf["connection_url"] = "root:taosdata@http(localhost:6041)/"
	}

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

func intToByteArray(num int64) []byte {
	size := int(unsafe.Sizeof(num))
	arr := make([]byte, size)
	for i := 0; i < size; i++ {
		byt := *(*uint8)(unsafe.Pointer(uintptr(unsafe.Pointer(&num)) + uintptr(i)))
		arr[i] = byt
	}
	return arr
}

func byteArrayToInt(arr []byte) int64 {
	val := int64(0)
	size := len(arr)
	for i := 0; i < size; i++ {
		*(*uint8)(unsafe.Pointer(uintptr(unsafe.Pointer(&val)) + uintptr(i))) = arr[i]
	}
	return val
}

func interfaceToBytes(x interface{}) ([]byte, error) {
	if x == nil {
		return []byte{}, nil
	}

	switch v := x.(type) {
	case int:
		return intToByteArray(int64(v)), nil
	case int8:
		return intToByteArray(int64(v)), nil
	case int16:
		return intToByteArray(int64(v)), nil
	case int32:
		return intToByteArray(int64(v)), nil
	case int64:
		return intToByteArray(v), nil
	case uint:
		return intToByteArray(int64(v)), nil
	case uint8:
		return intToByteArray(int64(v)), nil
	case uint16:
		return intToByteArray(int64(v)), nil
	case uint32:
		return intToByteArray(int64(v)), nil
	case time.Time:
		return v.MarshalBinary()
	case time.Duration:
		return intToByteArray(int64(v)), nil
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
	}

	return json.Marshal(x)
}

func bytesToInterface(b []byte, x interface{}) error {
	if x == nil || len(b) == 0 {
		return nil
	}

	var err error
	switch v := x.(type) {
	case *int:
		*v = int(byteArrayToInt(b))
	case *int8:
		*v = int8(byteArrayToInt(b))
	case *int16:
		*v = int16(byteArrayToInt(b))
	case *int32:
		*v = int32(byteArrayToInt(b))
	case *int64:
		*v = byteArrayToInt(b)
	case *uint:
		*v = uint(byteArrayToInt(b))
	case *uint8:
		*v = uint8(byteArrayToInt(b))
	case *uint16:
		*v = uint16(byteArrayToInt(b))
	case *uint32:
		*v = uint32(byteArrayToInt(b))
	case *time.Time:
		err = (*v).UnmarshalBinary(b)
	case *time.Duration:
		*v = time.Duration(byteArrayToInt(b))
	case *[]byte:
		*v = b
	case *string:
		*v = string(b)
	default:
		err = json.Unmarshal(b, x)
	}

	return err
}

func (m *Cache) AddContext(ctx context.Context, k string, x interface{}, d time.Duration) error {
	if d == 0 {
		d = m.defaultExpiration
	}
	i := int64(0)
	if d > 0 {
		i = d.Nanoseconds()
	}

	bs, err := interfaceToBytes(x)
	if err != nil {
		return err
	}
	entry := &physical.Entry{Key: k, Value: bs}
	return m.td.AddWithDuration(ctx, entry, i, 1)
}

func (m *Cache) Add(k string, x interface{}, d time.Duration) error {
	return m.AddContext(m.Context(), k, x, d)
}

func (m *Cache) SetContext(ctx context.Context, k string, x interface{}, d time.Duration) error {
	if d == 0 {
		d = m.defaultExpiration
	}
	i := int64(0)
	if d > 0 {
		i = d.Nanoseconds()
	}

	bs, err := interfaceToBytes(x)
	if err != nil {
		return err
	}
	entry := &physical.Entry{Key: k, Value: bs}
	return m.td.AddWithDuration(ctx, entry, i, 2)
}

func (m *Cache) Set(k string, x interface{}, d time.Duration) error {
	return m.SetContext(m.Context(), k, x, d)
}

func (m *Cache) SetDefaultContext(ctx context.Context, k string, x interface{}) error {
	return m.SetContext(ctx, k, x, 0)
}

func (m *Cache) SetDefault(k string, x interface{}) error {
	return m.Set(k, x, 0)
}

func (m *Cache) getContext(ctx context.Context, k string) (*physical.Entry, error) {
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

func (m *Cache) GetContext(ctx context.Context, k string, x interface{}) error {
	entry, err := m.getContext(ctx, k)
	if err != nil {
		return err
	}
	if entry == nil {
		return nil
	}

	return bytesToInterface(entry.Value, x)
}

func (m *Cache) Get(k string, x interface{}) error {
	return m.GetContext(m.Context(), k, x)
}

func (m *Cache) GetWithExpirationContext(ctx context.Context, k string, x interface{}) (time.Time, error) {
	entry, err := m.getContext(ctx, k)
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

func (m *Cache) GetWithExpiration(k string, x interface{}) (time.Time, error) {
	return m.GetWithExpirationContext(m.Context(), k, x)
}

func (m *Cache) DeleteContext(ctx context.Context, k string) error {
	return m.td.Delete(ctx, k)
}

func (m *Cache) Delete(k string) error {
	return m.DeleteContext(m.Context(), k)
}

type Item struct {
	Object     interface{}
	Expiration int64
}

func (m *Cache) FlushContext(ctx context.Context) error {
	return m.td.Flush(ctx)
}

func (m *Cache) Flush() error {
	return m.FlushContext(m.Context())
}

func (m *Cache) ItemsContext(ctx context.Context, object interface{}) (map[string]Item, error) {
	entries, err := m.td.Items(ctx)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}

	return entriesToItems(entries, object)
}

func (m *Cache) Items(object interface{}) (map[string]Item, error) {
	return m.ItemsContext(m.Context(), object)
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
