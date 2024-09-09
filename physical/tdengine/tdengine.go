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
	db         *sql.DB
	database   string
	sTable     string
	logger     log.Logger
	permitPool *physical.PermitPool
	conf       map[string]string
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
		logger.Debug("max_parallel set", "max_parallel", maxParInt)
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
		logger.Debug("max_idle_connections set", "max_idle_connections", maxIdleConnsStr)
		db.SetMaxIdleConns(maxIdleConns)
	}

	database := conf["database"]
	if database == "" {
		database = "openbao"
	}

	sTable := conf["stable"]
	if sTable == "" {
		sTable = "superbao"
	}

	// Setup the backend.
	m := &TDEngineBackend{
		db:         db,
		database:   database,
		sTable:     sTable,
		logger:     logger,
		permitPool: physical.NewPermitPool(maxParInt),
		conf:       conf,
	}

	schemaRows, err := db.Query(`SELECT name FROM information_schema.ins_databases WHERE name = "` + database + `"`)
	if err != nil {
		return nil, fmt.Errorf("failed to check tdengine schema exist: %w", err)
	}
	defer schemaRows.Close()
	schemaExist := schemaRows.Next()
	if !schemaExist {
		if _, err = db.Exec("CREATE DATABASE IF NOT EXISTS `" + database + "`"); err != nil {
			return nil, fmt.Errorf("failed to create tdengine database %s: %w", database, err)
		}
		logger.Debug("tdengine database created", "database", database)
	}

	stableExist, err := m.existingStable(sTable)
	if err != nil {
		logger.Error("failed to check tdengine stable exist", "stable", sTable, "error", err)
		return nil, fmt.Errorf("failed to check tdengine stable exist: %w", err)
	} else if !stableExist {
		if _, err = db.Exec("CREATE STABLE IF NOT EXISTS " + sTable + " ( ts timestamp, k VARCHAR(4096), v VARBINARY(60000) ) TAGS ( NamespaceID VARCHAR(64), NamespacePath VARCHAR(1024) )"); err != nil {
			return nil, fmt.Errorf("failed to create tdengine stable %s: %w", sTable, err)
		}
		logger.Debug("tdengine stable created", "stable", sTable)
	}

	tname := tablename(namespace.RootNamespace)
	tableExist, err := m.existingTable(tname)
	if err != nil {
		return nil, fmt.Errorf("failed to check tdengine root table exist: %w", err)
	} else if !tableExist {
		if _, err = m.db.Exec("CREATE TABLE IF NOT EXISTS " + m.database + "." + tname + " USING " + m.database + "." + m.sTable + ` ( NamespaceID, NamespacePath ) TAGS ( "` + namespace.RootNamespace.ID + `", "" )`); err != nil {
			return nil, fmt.Errorf("failed to create tdengine table %s: %w", tname, err)
		}
		logger.Debug("tdengine root table created", "table", tname)
	}

	return m, err
}

func quote(s string) string {
	return strings.ReplaceAll(s, `;`, ``)
}

// tagname returns the table name for the current namespace.
func tagname(ns *namespace.Namespace) string {
	p := ns.Path
	if p == "" {
		return ""
	}

	if strings.HasPrefix(p, "/") {
		p = p[1:]
	}
	p = strings.ReplaceAll(p, "/", "_")
	return quote(p)
}

// tablename returns the table name for the current namespace.
func tablename(ns *namespace.Namespace) string {
	tag := tagname(ns)
	if tag == "" {
		return ns.ID
	}
	return ns.ID + "_" + tag
}

func (m *TDEngineBackend) existingChildren(tname string) (bool, error) {
	statement := `SELECT table_name from information_schema.ins_tables WHERE table_name LIKE "` + tname + `_%" AND db_name = "` + m.database + `"`
	return m.existing(statement)
}

func (m *TDEngineBackend) existingTable(tname string) (bool, error) {
	statement := `SELECT table_name FROM information_schema.ins_tables WHERE table_name = "` + tname + `" AND db_name = "` + m.database + `"`
	return m.existing(statement)
}

func (m *TDEngineBackend) existingStable(tname string) (bool, error) {
	statement := `SELECT stable_name FROM information_schema.ins_stables WHERE stable_name = "` + tname + `" AND db_name = "` + m.database + `"`
	return m.existing(statement)
}

func (m *TDEngineBackend) existing(statement string) (bool, error) {
	tableRows, err := m.db.Query(statement)
	if err != nil {
		return false, err
	}
	defer tableRows.Close()
	return tableRows.Next(), nil
}

// CreateIfNotExists creates the table if it does not exist.
func (m *TDEngineBackend) CreateIfNotExists(ctx context.Context, ns0, ns1 *namespace.Namespace) error {
	var tname, parent, p string
	parent = tablename(ns0)
	id := ns0.ID
	if id != ns1.ID {
		return fmt.Errorf("invalid namespace id %s", ns1.ID)
	}
	if strings.Contains(ns1.Path, "_") {
		return fmt.Errorf("invalid namespace path %s", ns1.Path)
	}
	tname = parent + "_" + ns1.Path
	p = tname[len(id)+1:]

	parentExist, err := m.existingTable(parent)
	if err != nil {
		m.logger.Error("failed to check tdengine parent table exist", "parent", parent, "error", err)
		return fmt.Errorf("failed to check tdengine parent table exist: %w", err)
	} else if !parentExist {
		m.logger.Error("parent namespace not found", "parent", parent)
		return fmt.Errorf("parent namespace not found")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Create the table if it does not exist.
	statement := "CREATE TABLE IF NOT EXISTS " + m.database + "." + tname + " USING " + m.database + "." + m.sTable + ` ( NamespaceID, NamespacePath ) TAGS ( "` + id + `", "` + p + `" )`
	_, err = m.db.Exec(statement)
	if err != nil {
		m.logger.Error("failed to create tdengine table", "table", tname, "error", err)
		return fmt.Errorf("failed to create tdengine table %s: %w", tname, err)
	}

	m.logger.Debug("tdengine table created", "table", tname)
	return nil
}

// DropIfExists drop the table if it exists.
func (m *TDEngineBackend) DropIfExists(ctx context.Context, ns0, ns1 *namespace.Namespace) error {
	parent := tablename(ns0)
	tableExist, err := m.existingTable(parent)
	if err != nil {
		m.logger.Error("failed to check tdengine parent table exist", "parent", parent, "error", err)
		return fmt.Errorf("failed to check tdengine parent table exist: %w", err)
	} else if !tableExist {
		m.logger.Error("parent namespace not found", "parent", parent)
		return fmt.Errorf("parent namespace not found %s", parent)
	}

	if ns0.ID != ns1.ID {
		return fmt.Errorf("invalid namespace id %s", ns1.ID)
	}
	if strings.Contains(ns1.Path, "_") {
		return fmt.Errorf("invalid namespace path %s", ns1.Path)
	}

	tname := parent + "_" + ns1.Path
	tagExist, err := m.existingChildren(tname)
	if err != nil {
		m.logger.Error("failed to check tdengine children exist", "table", tname, "error", err)
		return fmt.Errorf("failed to check tdengine children exist: %w", err)
	} else if tagExist {
		m.logger.Error("children namespace found", "table", tname)
		return fmt.Errorf("children namespace found %s", tname)
	}

	tableExist, err = m.existingTable(tname)
	if err != nil {
		m.logger.Error("failed to check tdengine table exist", "table", tname, "error", err)
		return fmt.Errorf("failed to check tdengine table exist: %w", err)
	} else if !tableExist {
		m.logger.Error("table not found", "table", tname)
		return fmt.Errorf("table not found %s", tname)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	statement := "DROP TABLE IF EXISTS " + m.database + "." + tname
	_, err = m.db.Exec(statement)
	if err != nil {
		m.logger.Error("failed to drop tdengine table", "table", tname, "statement", statement, "error", err)
		return fmt.Errorf("failed to drop tdengine table %w", err)
	}

	m.logger.Debug("tdengine table dropped", "table", tname)
	return nil
}

// get table name from context namespace.
func (m *TDEngineBackend) getTablename(ctx context.Context) (string, error) {
	ns, err := namespace.FromContext(ctx)
	if err != nil && err == namespace.ErrNoNamespace {
		ns = &namespace.Namespace{
			ID:   namespace.RootNamespaceID,
			Path: "",
		}
	} else if err != nil {
		return "", fmt.Errorf("namespace in ctx error: %w", err)
	}

	return m.database + `.` + tablename(ns), nil
}

// Get is used to fetch an entry.
func (m *TDEngineBackend) Get(ctx context.Context, key string) (*physical.Entry, error) {
	defer metrics.MeasureSince([]string{"tdengine", "get"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	tname, err := m.getTablename(ctx)
	if err != nil {
		m.logger.Error("failed to set namespace", "error", err)
		return nil, err
	}

	statement := `SELECT v FROM ` + tname + ` WHERE k="` + key + `" ORDER BY ts DESC LIMIT 1`
	var result []byte
	err = m.db.QueryRowContext(ctx, statement).Scan(&result)
	if err == sql.ErrNoRows {
		m.logger.Debug("tdengine get", "table", tname, "key", key, "record", "not found")
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to query %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	m.logger.Debug("tdengine get", "table", tname, "key", key)

	return &physical.Entry{
		Key:   key,
		Value: result,
	}, nil
}

// Put is used to insert or update an entry.
func (m *TDEngineBackend) Put(ctx context.Context, entry *physical.Entry) error {
	defer metrics.MeasureSince([]string{"tdengine", "put"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	tname, err := m.getTablename(ctx)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	key := entry.Key
	statement := `SELECT ts FROM ` + tname + ` WHERE k="` + key + `"`
	rows, err := m.db.QueryContext(ctx, statement)
	if err == sql.ErrNoRows {
		statement := fmt.Sprintf(`INSERT INTO %s VALUES (now, '%s', "\x%x")`, tname, key, entry.Value)
		_, err = m.db.ExecContext(ctx, statement)
		m.logger.Debug("tdengine put", "table", tname, "key", key, "possible error", err)
		return err
	} else if err != nil {
		return fmt.Errorf("failed to query %w", err)
	}
	defer rows.Close()

	var arr []string
	for rows.Next() {
		var ts time.Time
		err = rows.Scan(&ts)
		if err != nil {
			return fmt.Errorf("failed to scan ts %w", err)
		}
		arr = append(arr, fmt.Sprintf("%s", ts))
	}

	statement = fmt.Sprintf(`INSERT INTO %s VALUES (now, '%s', "\x%x")`, tname, key, entry.Value)
	_, err = m.db.ExecContext(ctx, statement)
	if err != nil {
		m.logger.Error("failed to insert", "table", tname, "error", err)
		return fmt.Errorf("failed to insert %w", err)
	}

	for _, ts := range arr {
		s := strings.Split(ts, " ")
		_, err = m.db.ExecContext(ctx, `DELETE FROM `+tname+` WHERE ts="`+strings.Join(s[:2], " ")+`"`)
		if err != nil {
			m.logger.Error("failed to delete old", "statement", statement, "ts", ts, "error", err)
			return fmt.Errorf("failed to delete %w in update", err)
		}
	}

	m.logger.Debug("tdengine put", "table", tname, "key", key)
	return nil
}

// Delete is used to permanently delete an entry
func (m *TDEngineBackend) Delete(ctx context.Context, key string) error {
	defer metrics.MeasureSince([]string{"tdengine", "delete"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	tname, err := m.getTablename(ctx)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	statement := `SELECT ts FROM ` + tname + ` WHERE k="` + key + `"`
	rows, err := m.db.QueryContext(ctx, statement)
	if err == sql.ErrNoRows {
		return fmt.Errorf("key %s not found", key)
	} else if err != nil {
		return fmt.Errorf("failed to query %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var ts time.Time
		err = rows.Scan(&ts)
		if err != nil {
			return fmt.Errorf("failed to scan ts %w", err)
		}
		s := strings.Split(fmt.Sprintf("%s", ts), " ")
		_, err = m.db.ExecContext(ctx, `DELETE FROM `+tname+` WHERE ts="`+strings.Join(s[:2], " ")+`"`)
		if err != nil {
			m.logger.Error("failed to delete", "statement", statement, "key", key, "error", err)
			return fmt.Errorf("failed to delete %w", err)
		}
	}

	m.logger.Debug("tdengine delete", "table", tname, "key", key)

	return nil
}

// List is used to list all the keys under a given
// prefix, up to the next prefix.
func (m *TDEngineBackend) List(ctx context.Context, prefix string) ([]string, error) {
	defer metrics.MeasureSince([]string{"tdengine", "list"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	tname, err := m.getTablename(ctx)
	if err != nil {
		return nil, err
	}

	statement := `SELECT k FROM ` + tname
	if prefix != "" {
		// Add the % wildcard to the prefix to do the prefix search
		statement += ` WHERE k LIKE "` + prefix + `%"`
	}
	rows, err := m.db.QueryContext(ctx, statement)
	if err != nil {
		m.logger.Error("failed to list", "table", tname, "statement", statement, "error", err)
		return nil, fmt.Errorf("failed to list %w", err)
	}
	defer rows.Close()

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
	m.logger.Debug("tdengine list", "table", tname, "prefix", prefix, "keys", keys)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	sort.Strings(keys)
	return keys, nil
}

func (m *TDEngineBackend) ListPage(ctx context.Context, prefix string, after string, limit int) ([]string, error) {
	defer metrics.MeasureSince([]string{"tdengine", "list_page"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	tname, err := m.getTablename(ctx)
	if err != nil {
		return nil, err
	}

	statement := `SELECT k FROM ` + tname
	if prefix != "" {
		// Add the % wildcard to the prefix to do the prefix search
		statement = `SELECT k FROM ` + tname + ` WHERE k LIKE "` + prefix + `%"`
	}
	rows, err := m.db.QueryContext(ctx, statement)
	if err != nil {
		return nil, fmt.Errorf("failed to list page: %w", err)
	}
	defer rows.Close()

	var keys []string
	trigger := false
	n := 0
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
		if n >= limit {
			break
		}

		key = strings.TrimPrefix(key, prefix)
		if i := strings.Index(key, "/"); i == -1 {
			// Add objects only from the current 'folder'
			keys = append(keys, key)
		} else if i != -1 {
			// Add truncated 'folder' paths
			keys = strutil.AppendIfMissing(keys, string(key[:i+1]))
		}
		n++
	}
	m.logger.Debug("tdengine list_page", "table", tname, "prefix", prefix, "after", after, "limit", limit, "keys", keys)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return keys, nil
}
