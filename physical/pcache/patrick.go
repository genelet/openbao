package pcache

import (
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/go-secure-stdlib/strutil"
	"github.com/openbao/openbao/helper/namespace"
	"github.com/patrickmn/go-cache"
)

var errNilNamespace = fmt.Errorf("nil namespace in oidc cache request")

var _ Cache = (*Patrick)(nil)

type Patrick struct {
	defaultExpiration, cleanupInterval time.Duration
	*cache.Cache
}

func NewPatrick(defaultExpiration, cleanupInterval time.Duration) *Patrick {
	return &Patrick{
		defaultExpiration: defaultExpiration,
		cleanupInterval:   cleanupInterval,
		Cache:             cache.New(defaultExpiration, cleanupInterval),
	}
}

func nskey(ns *namespace.Namespace, key string) string {
	return fmt.Sprintf("v0:%s:%s", ns.ID, key)
}

func (c *Patrick) GetNS(ns *namespace.Namespace, key string, obj interface{}) (bool, error) {
	if ns == nil {
		return false, errNilNamespace
	}
	return c.Get(nskey(ns, key), obj)
}

func (c *Patrick) SetDefaultNS(ns *namespace.Namespace, key string, obj interface{}) error {
	if ns == nil {
		return errNilNamespace
	}
	return c.SetDefault(nskey(ns, key), obj)
}

func (c *Patrick) DeleteNS(ns *namespace.Namespace, key string) error {
	if ns == nil {
		return errNilNamespace
	}
	return c.Delete(nskey(ns, key))
}

// isTargetNamespacedKey returns true for a properly constructed namespaced key (<version>:<nsID>:<key>)
// where <nsID> matches any targeted nsID
func IsTargetNamespacedKey(nskey string, nsTargets []string) bool {
	split := strings.Split(nskey, ":")
	return len(split) >= 3 && strutil.StrListContains(nsTargets, split[1])
}

func (c *Patrick) FlushNS(ns *namespace.Namespace) error {
	if ns == nil {
		return errNilNamespace
	}

	// Remove all items from the provided namespace as well as the shared, "no namespace" section.
	for itemKey := range c.Cache.Items() {
		if IsTargetNamespacedKey(itemKey, []string{namespace.RootNamespaceID, ns.ID}) {
			if err := c.Delete(itemKey); err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *Patrick) Flush() error {
	p.Cache.Flush()
	return nil
}

func (p *Patrick) Get(k string, object interface{}) (bool, error) {
	v, found := p.Cache.Get(k)
	if object == nil || !found {
		return found, nil
	}
	bs, err := interfaceToBytes(v)
	if err != nil {
		return false, err
	}
	return true, bytesToInterface(bs, object)
}

func (p *Patrick) Items(object interface{}) (map[string]Item, error) {
	items := p.Cache.Items()
	if len(items) == 0 {
		return nil, nil
	}
	results := make(map[string]Item)
	for k, v := range items {
		results[k] = Item{Object: v.Object, Expiration: v.Expiration}
	}
	return results, nil
}

func (p *Patrick) Add(k string, object interface{}, d time.Duration) error {
	return p.Cache.Add(k, object, d)
}

func (p *Patrick) Set(k string, object interface{}, d time.Duration) error {
	p.Cache.Set(k, object, d)
	return nil
}

func (p *Patrick) SetDefault(k string, object interface{}) error {
	p.Cache.SetDefault(k, object)
	return nil
}

func (p *Patrick) Increment(k string, n int64) error {
	return p.Cache.Increment(k, n)
}

func (p *Patrick) Delete(k string) error {
	p.Cache.Delete(k)
	return nil
}
