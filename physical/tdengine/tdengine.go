/*
CREATE DATABASE openbao PRECISION 'ns' KEEP 3650 DURATION 10 BUFFER 16;

CREATE STABLE superbao  (
    ts timestamp,
	k  VARCHAR(4096),
	v  VARBINARY(60000)
) TAGS (
    NamespaceID VARCHAR(64),
    NamespacePath VARCHAR(1024)
);

CREATE TABLE root_
USING superbao ( NamespaceID, NamespacePath)
TAGS ("root", "");
*/

package tdengine

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	metrics "github.com/armon/go-metrics"
	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-secure-stdlib/strutil"
	"github.com/openbao/openbao/api/v2"
	"github.com/openbao/openbao/helper/namespace"
	"github.com/openbao/openbao/sdk/v2/physical"

	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

// Verify TDEngineBackend satisfies the correct interfaces
var (
	_ physical.Backend = (*TDEngineBackend)(nil)
)

// TDEngineBackend is a physical backend that stores data
// within TDEngine database.
type TDEngineBackend struct {
	db            *sql.DB
	database      string
	sTable        string
	namespaceID   string
	namespacePath string
	logger        log.Logger
	permitPool    *physical.PermitPool
	conf          map[string]string
}

// NewTDEngineBackend constructs a TDEngine backend using the given API client and
// server address and credential for accessing tdengine database.
func NewTDEngineBackend(conf map[string]string, logger log.Logger) (physical.Backend, error) {
	connURL := conf["connection_url"]
	if envURL := api.ReadBaoVariable("TDENGINE_CONNECTION_URL"); envURL != "" {
		connURL = envURL
	}

	var maxParInt int
	var err error
	maxParStr, ok := conf["max_parallel"]
	if ok {
		maxParInt, err = strconv.Atoi(maxParStr)
		if err != nil {
			return nil, fmt.Errorf("failed parsing max_parallel parameter: %w", err)
		}
		if logger.IsDebug() {
			logger.Debug("max_parallel set", "max_parallel", maxParInt)
		}
	} else {
		maxParInt = physical.DefaultParallelOperations
	}
	db, err := sql.Open("taosRestful", connURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect TDEngine: %w", err)
	}
	db.SetMaxOpenConns(maxParInt)

	maxIdleConnsStr, ok := conf["max_idle_connections"]
	if ok {
		maxIdleConns, err := strconv.Atoi(maxIdleConnsStr)
		if err != nil {
			return nil, fmt.Errorf("failed parsing max_idle_connections parameter: %w", err)
		}
		if logger.IsDebug() {
			logger.Debug("max_idle_connections set", "max_idle_connections", maxIdleConnsStr)
		}
		db.SetMaxIdleConns(maxIdleConns)
	}

	database := conf["database"]
	if database == "" {
		database = "openbao"
	}
	schemaRows, err := db.Query(`SELECT name FROM information_schema.ins_databases WHERE name = "` + database + `"`)
	if err != nil {
		return nil, fmt.Errorf("failed to check tdengine schema exist: %w", err)
	}
	defer schemaRows.Close()
	schemaExist := schemaRows.Next()
	if !schemaExist {
		if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS `" + database + "`"); err != nil {
			return nil, fmt.Errorf("failed to create tdengine database %s: %w", database, err)
		}
	}

	sTable := conf["stable"]
	if sTable == "" {
		sTable = "superbao"
	}
	stableRows, err := db.Query(`SELECT stable_name FROM information_schema.ins_stables WHERE stable_name = "` + sTable + `" AND db_name = "` + database + `"`)
	if err != nil {
		return nil, fmt.Errorf("failed to check tdengine stable exist: %w", err)
	}
	defer stableRows.Close()
	stableExist := stableRows.Next()
	if !stableExist {
		if _, err := db.Exec("CREATE STABLE IF NOT EXISTS " + sTable + " ( ts timestamp, k VARCHAR(4096), v VARBINARY(60000) ) TAGS ( NamespaceID VARCHAR(64), NamespacePath VARCHAR(1024) )"); err != nil {
			return nil, fmt.Errorf("failed to create tdengine stable %s: %w", sTable, err)
		}
	}

	// Setup the backend.
	m := &TDEngineBackend{
		db:            db,
		database:      database,
		sTable:        sTable,
		namespaceID:   namespace.RootNamespaceID,
		namespacePath: "",
		logger:        logger,
		permitPool:    physical.NewPermitPool(maxParInt),
		conf:          conf,
	}

	tableRows, err := db.Query(`SELECT table_name FROM information_schema.ins_tables WHERE table_name = "` + m.tablename() + `" AND stable_name = "` + sTable + `" AND db_name = "` + database + `"`)
	if err != nil {
		return nil, fmt.Errorf("failed to check tdengine root table exists: %w", err)
	}
	defer tableRows.Close()
	tableExist := tableRows.Next()
	if !tableExist {
		err = m.createIfNotExists()
	}

	return m, err
}

// CreateIfNotExists creates the table if it does not exist.
func (m *TDEngineBackend) createIfNotExists() error {
	// Create the table if it does not exist.
	_, err := m.db.Exec("CREATE TABLE IF NOT EXISTS " + m.database + "." + m.tablename() + " USING " + m.database + "." + m.sTable + ` ( NamespaceID, NamespacePath ) TAGS ( "` + m.namespaceID + `", "` + strings.ReplaceAll(m.namespacePath, "/", "_") + `" )`)
	if err != nil {
		return fmt.Errorf("failed to create tdengine table %s: %w", m.tablename(), err)
	}
	return nil
}

// tablename returns the table name for the current namespace.
func (m *TDEngineBackend) tablename() string {
	return m.namespaceID + "_" + strings.ReplaceAll(m.namespacePath, "/", "_")
}

// Set the namespace.
func (m *TDEngineBackend) setNamespace(ctx context.Context) (string, error) {
	ns, err := namespace.FromContext(ctx)
	if err != nil && err == namespace.ErrNoNamespace {
		ns = &namespace.Namespace{
			ID:   namespace.RootNamespaceID,
			Path: "",
		}
	} else if err != nil {
		return "", fmt.Errorf("namespace in ctx error: %w", err)
	}

	m.namespaceID = ns.ID
	m.namespacePath = ns.Path
	return m.database + `.` + ns.ID + "_" + strings.ReplaceAll(ns.Path, "/", "_"), nil
}

// Put is used to insert or update an entry.
func (m *TDEngineBackend) Put(ctx context.Context, entry *physical.Entry) error {
	defer metrics.MeasureSince([]string{"tdengine", "put"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	tname, err := m.setNamespace(ctx)
	if err != nil {
		return err
	}

	statement := fmt.Sprintf(`INSERT INTO %s VALUES (now, '%s', "\x%x")`, tname, entry.Key, entry.Value)
	_, err = m.db.ExecContext(ctx, statement)
	if err != nil {
		return fmt.Errorf("failed to execute statement %s: %w", statement, err)
	}
	return nil
}

// Get is used to fetch an entry.
func (m *TDEngineBackend) Get(ctx context.Context, key string) (*physical.Entry, error) {
	defer metrics.MeasureSince([]string{"tdengine", "get"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	tname, err := m.setNamespace(ctx)
	if err != nil {
		return nil, err
	}

	var result []byte
	err = m.db.QueryRowContext(ctx, `SELECT v FROM `+tname+` WHERE k="`+key+`"`).Scan(&result)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &physical.Entry{
		Key:   key,
		Value: result,
	}, err
}

// Delete is used to permanently delete an entry
func (m *TDEngineBackend) Delete(ctx context.Context, key string) error {
	defer metrics.MeasureSince([]string{"tdengine", "delete"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	tname, err := m.setNamespace(ctx)
	if err != nil {
		return err
	}

	_, err = m.db.ExecContext(ctx, `DELETE FROM `+tname+` WHERE k=`, key)
	return err
}

// List is used to list all the keys under a given
// prefix, up to the next prefix.
func (m *TDEngineBackend) List(ctx context.Context, prefix string) ([]string, error) {
	defer metrics.MeasureSince([]string{"tdengine", "list"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	tname, err := m.setNamespace(ctx)
	if err != nil {
		return nil, err
	}

	// Add the % wildcard to the prefix to do the prefix search
	statement := `SELECT k FROM ` + tname + ` WHERE k LIKE "` + prefix + `%"`
	rows, err := m.db.QueryContext(ctx, statement)
	if err != nil {
		return nil, fmt.Errorf("failed to list statement %s with prefix %s: %w", statement, prefix, err)
	}

	var keys []string
	for rows.Next() {
		var key string
		err = rows.Scan(&key)
		if err != nil {
			return nil, fmt.Errorf("failed to scan rows: %w", err)
		}

		key = strings.TrimPrefix(key, prefix)
		if i := strings.Index(key, "/"); i == -1 {
			// Add objects only from the current 'folder'
			keys = append(keys, key)
		} else if i != -1 {
			// Add truncated 'folder' paths
			keys = strutil.AppendIfMissing(keys, string(key[:i+1]))
		}
	}

	sort.Strings(keys)
	return keys, nil
}

func (m *TDEngineBackend) ListPage(ctx context.Context, prefix string, after string, limit int) ([]string, error) {
	defer metrics.MeasureSince([]string{"tdengine", "list_page"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	tname, err := m.setNamespace(ctx)
	if err != nil {
		return nil, err
	}

	// Add the % wildcard to the prefix to do the prefix search
	likePrefix := prefix + "%"
	statement := `SELECT k FROM ` + tname + ` WHERE k LIKE "` + likePrefix + `" ORDER BY k`
	rows, err := m.db.QueryContext(ctx, statement)
	if err != nil {
		return nil, fmt.Errorf("failed to list statement: %w", err)
	}

	var keys []string
	trigger := false
	for rows.Next() {
		var key string
		err = rows.Scan(&key)
		if err != nil {
			return nil, fmt.Errorf("failed to scan rows: %w", err)
		}

		if !trigger && key == prefix+after {
			trigger = true
		}
		if !trigger {
			continue
		}
		key = strings.TrimPrefix(key, prefix)
		if i := strings.Index(key, "/"); i == -1 {
			// Add objects only from the current 'folder'
			keys = append(keys, key)
		} else if i != -1 {
			// Add truncated 'folder' paths
			keys = strutil.AppendIfMissing(keys, string(key[:i+1]))
		}
	}

	return keys, nil
}
