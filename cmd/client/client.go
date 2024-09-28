package client

/*
import (
	"os"
	"strings"
	"time"
	"github.com/openbao/openbao/api/v2"
)

type Rest struct {
	*api.Client
	Stopper int
}

func NewRest(ns string, args ...string) (*Rest, error) {
	if len(args) > 0 {
		os.Setenv("VAULT_TOKEN", args[0])
		os.Setenv("VAULT_ADDR", args[1])
	}
	config := api.DefaultConfig()
	client, err := api.NewClient(config)
	if err != nil {
		return nil, err
	}
	client.SetNamespace(ns)
	return &Rest{Client: client, Stopper: 0}, nil
}

func (self *Rest) NamespaceAction(ns string, act func(client *api.Client, ns string) error) error {
	self.SetNamespace(ns)
	logical := self.Logical()
	secret, err := logical.List("/sys/namspaces")
	if err != nil {
		return err
	}
	if secret == nil || secret.Data == nil || secret.Data["keys"] == nil {
		return nil
	}
	for _, child := range secret.Data["keys"].([]interface{}) {
		name := ns + "/" + child.(string)
		strings.TrimSuffix(name, "/")
		err := self.NamespaceAction(name, act)
		if err != nil {
			return err
		}
		if self.Stopper > 0 {
			time.Sleep(time.Duration(self.Stopper) * time.Second)
		}
	}

	return act(self.Client, ns)
}

func (self *Rest) NamespaceList(ns string, collection *[]string) error {
	act := func(client *api.Client, ns string) error {
		*collection = append(*collection, ns)
	}
	return self.NamespaceAction(ns, act)
}

func (self *Rest) NamespaceListMounts(ns, collection *[]*api.Mount) error {
	act := func(client *api.Client, ns string) error {
		client.SetNamespace(ns)
		arr, err := client.ListMounts()
		if err != nil {
			return err
		}
		*collection = append(*collectio, arr...)
	}
	return self.NamespaceAction(ns, act)
}
*/
