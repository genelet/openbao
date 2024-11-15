package pcache

import (
	"encoding/json"
	"os"
	"time"
	"unsafe"

	"github.com/hashicorp/go-hclog"
	"github.com/openbao/openbao/helper/namespace"
)

const (
	MarkerNoExpiration      = -1
	MarkerDefaultExpiration = 0
)

type Item struct {
	Object     interface{}
	Expiration int64
}

type Cache interface {
	Flush() error
	Get(string, interface{}) (bool, error)
	Items(interface{}) (map[string]Item, error)
	Add(string, interface{}, time.Duration) error
	Set(string, interface{}, time.Duration) error
	SetDefault(string, interface{}) error
	Increment(string, int64) error
	Delete(string) error

	GetNS(*namespace.Namespace, string, interface{}) (bool, error)
	SetDefaultNS(*namespace.Namespace, string, interface{}) error
	DeleteNS(*namespace.Namespace, string) error
	FlushNS(*namespace.Namespace) error
}

func New(id, path string, defaultExpiration, cleanupInterval time.Duration, logger ...hclog.Logger) (Cache, error) {
	switch os.Getenv("OPENBAO_PCACHE") {
	case "tdengine":
		return NewTD(id, path, defaultExpiration, logger...)
	}
	return NewPatrick(defaultExpiration, cleanupInterval), nil
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
