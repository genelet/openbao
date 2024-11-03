package env

import (
	"os"
	"strings"

	log "github.com/hashicorp/go-hclog"
	"github.com/openbao/openbao/helper/namespace"
	"github.com/openbao/openbao/physical/tdengine"
	"github.com/openbao/openbao/sdk/v2/physical"
	"github.com/openbao/openbao/sdk/v2/physical/inmem"
)

func DefaultBackend(logger log.Logger) (physical.Backend, error) {
	switch strings.ToLower(os.Getenv("OPENBAO_MOUNTABLE")) {
	case "tdengine":
		physicalBackend, err := tdengine.NewTDEngineBackend(map[string]string{
			"connection_url": "root:taosdata@tcp(vm0:6030)/testbao",
			"database":       "testbao",
		}, logger)
		if err == nil {
			if err = physicalBackend.DropAllTables("root", "mount"); err == nil {
				err = physicalBackend.Flush(namespace.RootContext(nil))
			}
		}
		return physicalBackend, err
	default:
	}
	return inmem.NewInmem(nil, logger)
}
