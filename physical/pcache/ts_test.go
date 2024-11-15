package pcache

import (
	"fmt"
	"testing"
	"time"

	"github.com/openbao/openbao/sdk/v2/physical"
)

func TestInterfaceToBytes(t *testing.T) {
	type A struct {
		Name  string
		Value int
	}

	xs := []interface{}{
		"hello",
		1,
		2,
		uint32(3),
		int64(4),
		time.Now(),
		&A{Name: "hellp", Value: 11},
		&A{Name: "world", Value: 22},
	}

	var y0 string
	var y1 int
	var y2 int
	var y3 uint32
	var y4 int64
	var y5 time.Time
	y6 := A{}
	y7 := A{}

	ys := []interface{}{
		&y0,
		&y1,
		&y2,
		&y3,
		&y4,
		&y5,
		&y6,
		&y7,
	}

	var zs []interface{}
	for i, x := range xs {
		bs, err := interfaceToBytes(x)
		if err != nil {
			t.Fatal(err)
		}
		err = bytesToInterface(bs, ys[i])
		if err != nil {
			t.Fatal(err)
		}
		zs = append(zs, ys[i])
	}

	if *(zs[0].(*string)) != "hello" ||
		*(zs[1].(*int)) != 1 ||
		*(zs[2].(*int)) != 2 ||
		*(zs[3].(*uint32)) != 3 ||
		*(zs[4].(*int64)) != 4 ||
		zs[6].(*A).Name != "hellp" ||
		zs[6].(*A).Value != 11 ||
		zs[7].(*A).Name != "world" ||
		zs[7].(*A).Value != 22 {
		t.Errorf("zs: %#v", zs)
	}

	arr := []*A{
		{"hello", 1},
		{"world", 2},
		{"hello world", 3},
	}

	var entries []*physical.Entry
	for i, a := range arr {
		bs, err := interfaceToBytes(a)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := time.Now().MarshalBinary()
		entries = append(entries, &physical.Entry{Key: fmt.Sprintf("key%d", i), Value: bs, ValueHash: b})
	}

	items, err := entriesToItems(entries, new(A))
	if err != nil {
		t.Fatal(err)
	}

	for key, item := range items {
		if key == "key0" && item.Object.(A).Name != "hello" && item.Object.(A).Value != 1 ||
			key == "key1" && item.Object.(A).Name != "world" && item.Object.(A).Value != 2 ||
			key == "key2" && item.Object.(A).Name != "hello world" && item.Object.(A).Value != 3 {
			t.Errorf("item.Key: %s, %#v", key, item.Object)
		}
	}
}
