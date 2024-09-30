/*
CREATE DATABASE openbao PRECISION 'ns' KEEP 3650 DURATION 10 BUFFER 16;

USE openbao;

CREATE STABLE superbao  (
    ts timestamp,
	k  VARCHAR(4096),
	v  VARBINARY(60000)
) TAGS (
    NamespaceID VARCHAR(1024),
    NamespacePath VARCHAR(64)
);

CREATE STABLE supermount  (
    ts timestamp,
	k  VARCHAR(1024),
	v  VARCHAR(128)
) TAGS (
    NamespaceID VARCHAR(1024),
    NamespacePath VARCHAR(64)
);
*/

package tdengine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	metrics "github.com/armon/go-metrics"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-secure-stdlib/strutil"
	"github.com/openbao/openbao/api/v2"
	"github.com/openbao/openbao/helper/namespace"
	"github.com/openbao/openbao/sdk/v2/physical"
	_ "github.com/taosdata/driver-go/v3/taosSql"
)

const (
	errTableExist    = "failed to check tdengine table exist"
	errTableCreate   = "failed to create tdengine table"
	errTableDrop     = "failed to drop tdengine table"
	errParentExist   = "failed to check tdengine parent table exist"
	errChildrenExist = "failed to check tdengine children exist"
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
	sTables    map[string][]string
	sTable     string
	logger     hclog.Logger
	permitPool *physical.PermitPool
	conf       map[string]string

	updateLock sync.Mutex
	mountLock  sync.RWMutex
}

func NewTDEnginePhysicalBackend(conf map[string]string, logger hclog.Logger) (physical.Backend, error) {
	return NewTDEngineBackend(conf, logger)
}

// NewTDEngineBackend constructs a TDEngine backend using the given API client and
// server address and credential for accessing tdengine database.
func NewTDEngineBackend(conf map[string]string, logger hclog.Logger) (*TDEngineBackend, error) {
	connURL := api.ReadBaoVariable("TDENGINE_CONNECTION_URL")
	if v, ok := conf["connection_url"]; ok {
		connURL = v
	}
	if connURL == "" {
		return nil, fmt.Errorf("missing connection_url parameter")
	}

	database := api.ReadBaoVariable("TDENGINE_DATABASE")
	if v, ok := conf["database"]; ok {
		database = v
	}
	if database == "" {
		return nil, fmt.Errorf("missing database parameter")
	}

	db, err := sql.Open("taosSql", connURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect TDEngine: %w", err)
	}

	maxParInt := physical.DefaultParallelOperations
	if v, ok := conf["max_parallel"]; ok {
		maxParInt, err = strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("failed to convert max_parallel to int: %w", err)
		}
	}
	db.SetMaxOpenConns(maxParInt)
	if v, ok := conf["max_idle_connections"]; ok {
		maxIdleConns, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("failed to convert max_idle_connections to int: %w", err)
		}
		db.SetMaxIdleConns(maxIdleConns)
	}

	sTables := map[string][]string{
		"stable": {"superbao", "root", "ts timestamp, k VARCHAR(4096), v VARBINARY(60000)"},
		"smount": {"supermount", "mount", "ts timestamp, k VARCHAR(1024), v VARCHAR(128)"},
	}

	var sTable string
	if v, ok := conf["stable"]; ok {
		sTable = v
	} else if v, ok := conf["stables"]; ok {
		err = json.Unmarshal([]byte(v), &sTables)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal stables: %w", err)
		}
	}

	if logger == nil {
		logger = hclog.Default()
	}

	// Setup the backend.
	m := &TDEngineBackend{
		db:         db,
		database:   database,
		sTables:    sTables,
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

	created2 := func(stable, statement, tname string) error {
		stableExist, err := m.existingStable(stable)
		if err != nil {
			logger.Error("failed to check tdengine stable exist", "stable", stable, "error", err)
			return fmt.Errorf("failed to check tdengine stable exist: %w", err)
		} else if !stableExist {
			if _, err = db.Exec(statement); err != nil {
				return fmt.Errorf("failed to create tdengine stable %s: %w", stable, err)
			}
			logger.Debug("tdengine stable created", "stable", stable)
		}

		tableExist, err := m.existingTable(tname)
		if err != nil {
			return fmt.Errorf("failed to check tdengine root table exist: %w", err)
		} else if !tableExist {
			id := namespace.RootNamespaceID
			path := ""
			if v, ok := conf["ns_id"]; ok {
				id = v
			}
			if v, ok := conf["ns_path"]; ok {
				path = v
			}
			statement := "CREATE TABLE IF NOT EXISTS " + m.database + "." + tname + " USING " + m.database + "." + stable + ` (NamespaceID, NamespacePath) TAGS ("` + id + `", "` + path + `")`
			if _, err = m.db.Exec(statement); err != nil {
				return fmt.Errorf("%s %s: %w", errTableCreate, tname, err)
			}
			logger.Debug("tdengine root table created", "table", tname)
		}
		return nil
	}

	if v, ok := conf["create_"+sTable]; ok {
		err = created2(sTable, v, conf["table"])
		if err != nil {
			return nil, err
		}
	} else {
		for _, item := range sTables {
			err = created2(item[0], `CREATE STABLE IF NOT EXISTS `+m.database+`.`+item[0]+` ( `+item[2]+` ) TAGS ( NamespaceID VARCHAR(1024), NamespacePath VARCHAR(64) )`, item[1])
			if err != nil {
				return nil, err
			}
		}
	}

	return m, err
}

func quote(s string) string {
	return strings.ReplaceAll(s, `;`, ``)
}

// tablename returns the table name for the current namespace.
func tablename(ns *namespace.Namespace, change ...string) string {
	x := strings.ReplaceAll(strings.ReplaceAll(ns.ID, "-", ""), "/", "_")
	if len(x) < len(namespace.RootNamespaceID) {
		x = namespace.RootNamespaceID
	}
	if len(change) > 0 {
		x = change[0] + x[len(namespace.RootNamespaceID):]
	}
	return quote(x)
}

// get table name from context namespace.
func (m *TDEngineBackend) getTablename(ctx context.Context, change ...string) (string, error) {
	ns, err := namespace.FromContext(ctx)
	if err != nil && err == namespace.ErrNoNamespace {
		root := namespace.RootNamespaceID
		if len(change) > 0 {
			root = change[0]
		}
		if m.conf != nil && m.conf["ns_path"] != "" {
			return m.database + `.` + root + "_" + m.conf["ns_path"], nil
		}
		return m.database + `.` + root, nil
	} else if err != nil {
		return "", fmt.Errorf("namespace in ctx error: %w", err)
	}

	if m.conf != nil && m.conf["ns_path"] != "" {
		path := strings.ReplaceAll(m.conf["ns_path"], "-", "")
		return m.database + `.` + tablename(ns, change...) + "_" + path, nil
	}
	return m.database + `.` + tablename(ns, change...), nil
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
	defer metrics.MeasureSince([]string{"tdengine", "existing"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	tableRows, err := m.db.Query(statement)
	if err != nil {
		return false, err
	}
	defer tableRows.Close()
	return tableRows.Next(), nil
}

// CreateIfNotExists creates the table if it does not exist.
func (m *TDEngineBackend) CreateIfNotExists(ctx context.Context, ns1 string) error {
	ns0, err := namespace.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to get namespace from context: %w", err)
	}

	parent := tablename(ns0)
	parentExist, err := m.existingTable(parent)
	if err != nil {
		m.logger.Error(errParentExist, "parent", parent, "error", err)
		return fmt.Errorf("%s: %w", errParentExist, err)
	} else if !parentExist {
		m.logger.Error("parent namespace not found", "parent", parent)
		return fmt.Errorf("parent namespace not found")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	ns1 = strings.Trim(ns1, "/")
	if strings.Contains(ns1, "_") {
		return fmt.Errorf("invalid namespace path %s", ns1)
	}

	tname := parent + "_" + ns1
	tname = strings.ReplaceAll(tname, "-", "")
	id := ns0.ID + "/" + ns1
	p := ns0.Path

	// Create the table if it does not exist.
	if m.sTable != "" {
		statement := "CREATE TABLE IF NOT EXISTS " + m.database + "." + tname + " USING " + m.database + "." + m.sTable + ` ( NamespaceID, NamespacePath ) TAGS ( "` + id + `", "` + p + `" )`
		_, err = m.db.Exec(statement)
		if err != nil {
			m.logger.Error(errTableCreate, "table", tname, "error", err)
			return fmt.Errorf("%s %s: %w", errTableCreate, tname, err)
		}
	} else {
		for k, item := range m.sTables {
			var t string
			switch k {
			case "smount":
				t = "mount" + tname[len(namespace.RootNamespaceID):]
			default:
				t = tname
			}
			statement := "CREATE TABLE IF NOT EXISTS " + m.database + "." + t + " USING " + m.database + "." + item[0] + ` ( NamespaceID, NamespacePath ) TAGS ( "` + id + `", "` + p + `" )`
			_, err = m.db.Exec(statement)
			if err != nil {
				m.logger.Error(errTableCreate, "table", t, "error", err)
				return fmt.Errorf("%s %s: %w", errTableCreate, t, err)
			}
		}
	}
	m.logger.Debug("tdengine table created", "table", tname)
	return nil
}

// DropIfExists drop the table if it exists.
func (m *TDEngineBackend) DropIfExists(ctx context.Context, ns1 string) error {
	ns0, err := namespace.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to get namespace from context: %w", err)
	}
	parent := tablename(ns0)

	ns1 = strings.Trim(ns1, "/")
	if strings.Contains(ns1, "_") {
		return fmt.Errorf("invalid namespace path %s", ns1)
	}
	tname := parent + "_" + ns1
	tname = strings.ReplaceAll(tname, "-", "")

	tableExist, err := m.existingTable(tname)
	if err != nil {
		m.logger.Error(errTableExist, "tname", tname, "error", err)
		return fmt.Errorf("%s: %w", errParentExist, err)
	} else if !tableExist {
		m.logger.Error("namespace not found", "tname", tname)
		return fmt.Errorf("namespace not found %s", ns1)
	}

	tagExist, err := m.existingChildren(tname)
	if err != nil {
		m.logger.Error(errChildrenExist, "table", tname, "error", err)
		return fmt.Errorf("%s: %w", errChildrenExist, err)
	} else if tagExist {
		m.logger.Error("children namespace found", "table", tname)
		return fmt.Errorf("children namespace found %s", ns1)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if m.sTable != "" {
		statement := "DROP TABLE IF EXISTS " + m.database + "." + tname
		_, err = m.db.Exec(statement)
		if err != nil {
			m.logger.Error(errTableDrop, "table", tname, "statement", statement, "error", err)
			return fmt.Errorf("%s %w", errTableDrop, err)
		}
	} else {
		for k := range m.sTables {
			var t string
			switch k {
			case "smount":
				t = "mount" + tname[len(namespace.RootNamespaceID):]
			default:
				t = tname
			}
			statement := "DROP TABLE IF EXISTS " + m.database + "." + t
			_, err = m.db.Exec(statement)
			if err != nil {
				m.logger.Error(errTableDrop, "table", t, "statement", statement, "error", err)
				return fmt.Errorf("%s %w", errTableDrop, err)
			}
		}
	}

	m.logger.Debug("tdengine table dropped", "table", tname)
	return nil
}

// Get is used to fetch an entry.
func (m *TDEngineBackend) GetWithDuration(ctx context.Context, key string, duration int64) (*physical.Entry, error) {
	defer metrics.MeasureSince([]string{"tdengine", "get"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	tname, err := m.getTablename(ctx)
	if err != nil {
		m.logger.Error("failed to set namespace", "error", err)
		return nil, err
	}

	statement := `SELECT ts, v FROM ` + tname + ` WHERE k="` + key + `"`
	if duration > 0 {
		statement += ` AND ts > now`
	}
	statement += ` ORDER BY ts DESC LIMIT 1`

	var ts time.Time
	var value []byte
	err = m.db.QueryRowContext(ctx, statement).Scan(&ts, &value)
	if err == sql.ErrNoRows {
		m.logger.Debug("tdengine get", "table", statement, "key", key, "record", "not found")
		return nil, nil
	} else if err != nil {
		m.logger.Error("failed to query", "table", statement, "key", key, "error", err)
		return nil, fmt.Errorf("failed to query %w", err)
	}

	m.logger.Debug("tdengine get", "table", statement, "key", key)

	bs, err := ts.MarshalBinary()
	return &physical.Entry{
		Key:       key,
		Value:     value,
		ValueHash: bs,
	}, err
}

// Get is used to fetch an entry.
func (m *TDEngineBackend) Get(ctx context.Context, key string) (*physical.Entry, error) {
	entry, err := m.GetWithDuration(ctx, key, 0)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}

	return &physical.Entry{
		Key:   key,
		Value: entry.Value,
	}, nil
}

// Put is used to insert or update an entry.
func (m *TDEngineBackend) AddWithDuration(ctx context.Context, entry *physical.Entry, d int64, patch int) error {
	defer metrics.MeasureSince([]string{"tdengine", "put"}, time.Now())

	if patch > 0 && d > 0 {
		err := m.DeleteExpired(ctx)
		if err != nil {
			return fmt.Errorf("failed to delete expired %w", err)
		}
	}

	m.updateLock.Lock()
	defer m.updateLock.Unlock()

	key := entry.Key
	err := m.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete %w", err)
	}

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

	var statement string
	if d > 0 {
		nano := time.Now().UnixNano()
		statement = fmt.Sprintf(`INSERT INTO %s VALUES (%d, '%s', "\x%x")`, tname, nano+d, key, entry.Value)
	} else {
		statement = fmt.Sprintf(`INSERT INTO %s VALUES (now, '%s', "\x%x")`, tname, key, entry.Value)
	}
	_, err = m.db.ExecContext(ctx, statement)
	if err != nil {
		m.logger.Error("tdengine failed to put", "key", key, "error", err)
		return fmt.Errorf("failed to put %w", err)
	}
	m.logger.Debug("tdengine put", "table", tname, "key", key)

	return nil
}

// Put is used to insert or update an entry.
func (m *TDEngineBackend) Put(ctx context.Context, entry *physical.Entry) error {
	return m.AddWithDuration(ctx, entry, 0, 0)
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
		m.logger.Debug("tdengine delete", "key not found", key)
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to query %s: %w", statement, err)
	}
	defer rows.Close()

	for rows.Next() {
		var ts time.Time
		err = rows.Scan(&ts)
		if err != nil {
			return fmt.Errorf("failed to scan ts %w", err)
		}
		s := strings.Split(ts.String(), " ")
		statement = `DELETE FROM ` + tname + ` WHERE ts="` + strings.Join(s[:2], " ") + `"`
		_, err = m.db.ExecContext(ctx, statement)
		if err != nil {
			m.logger.Error("failed to delete", "statement", statement, "key", key, "error", err)
			return fmt.Errorf("failed to delete %w", err)
		}
	}

	m.logger.Debug("tdengine delete", "table", tname, "key", key)

	return nil
}

// DeleteExpired is used to delete all the expired entries.
func (m *TDEngineBackend) DeleteExpired(ctx context.Context) error {
	defer metrics.MeasureSince([]string{"tdengine", "expired"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	tname, err := m.getTablename(ctx)
	if err != nil {
		return err
	}

	_, err = m.db.ExecContext(ctx, "DELETE FROM "+tname+" WHERE ts < now")
	if err != nil {
		m.logger.Debug("tdengine delete expired", "table", tname)
	}
	return err
}

// Flush is used to cleanup
func (m *TDEngineBackend) Flush(ctx context.Context) error {
	defer metrics.MeasureSince([]string{"tdengine", "flush"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	tname, err := m.getTablename(ctx)
	if err != nil {
		return err
	}

	_, err = m.db.ExecContext(ctx, "DELETE FROM "+tname)
	if err != nil {
		m.logger.Debug("tdengine delete all", "table", tname)
	}
	return err
}

// Items lists all entries
func (m *TDEngineBackend) Items(ctx context.Context) ([]*physical.Entry, error) {
	defer metrics.MeasureSince([]string{"tdengine", "list"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	tname, err := m.getTablename(ctx)
	if err != nil {
		return nil, err
	}

	statement := `SELECT ts, k, v FROM ` + tname
	rows, err := m.db.QueryContext(ctx, statement)
	if err != nil {
		m.logger.Error("failed to itemize", "table", tname, "statement", statement, "error", err)
		return nil, fmt.Errorf("failed to itemize %w", err)
	}
	defer rows.Close()

	var items []*physical.Entry
	for rows.Next() {
		var ts, key string
		var value []byte
		err = rows.Scan(&ts, &key, &value)
		if err != nil {
			return nil, fmt.Errorf("failed to scan rows: %w", err)
		}
		items = append(items, &physical.Entry{
			Key:       key,
			Value:     value,
			ValueHash: []byte(ts),
		})
	}

	m.logger.Debug("tdengine itemize", "table", tname)
	return items, nil
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

func (m *TDEngineBackend) ExistingMount(ctx context.Context, mount string, longest ...bool) (bool, error) {
	if len(longest) > 0 && longest[0] {
		arr, err := m.ListMounts(ctx)
		if err != nil {
			return false, err
		}
		for _, v := range arr {
			if strings.HasPrefix(mount, v) {
				return true, nil
			}
		}
		return false, nil
	}

	tname, err := m.getTablename(ctx, "mount")
	if err != nil {
		return false, err
	}
	return m.existing(`SELECT k FROM ` + tname + ` WHERE k="` + quote(mount) + `"`)
}

func (m *TDEngineBackend) GetMount(ctx context.Context, mount string) (string, error) {
	defer metrics.MeasureSince([]string{"tdengine", "get_mount"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	tname, err := m.getTablename(ctx, "mount")
	if err != nil {
		return "", err
	}

	mount = quote(mount)
	statement := `SELECT v FROM ` + tname + ` WHERE k="` + mount + `"`
	var value string
	err = m.db.QueryRowContext(ctx, statement).Scan(&value)
	if err == sql.ErrNoRows {
		m.logger.Debug("tdengine get mount", "table", statement, "mount", mount, "record", "not found")
		return "", nil
	} else if err != nil {
		m.logger.Error("failed to query", "table", statement, "mount", mount, "error", err)
		return "", fmt.Errorf("failed to query mount %w", err)
	}

	m.logger.Debug("tdengine get mount", "table", statement, "mount", mount, "value", value)
	return value, nil
}

func (m *TDEngineBackend) AddMount(ctx context.Context, mount, typ string) error {
	defer metrics.MeasureSince([]string{"tdengine", "mount"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	m.mountLock.Lock()
	defer m.mountLock.Unlock()

	err := m.RemoveMount(ctx, mount)
	if err != nil {
		return err
	}

	tname, err := m.getTablename(ctx, "mount")
	if err != nil {
		return err
	}

	var ts time.Time
	statement := `SELECT ts FROM ` + tname + ` WHERE k="` + quote(mount) + `" AND v="` + quote(typ) + `"`
	err = m.db.QueryRowContext(ctx, statement).Scan(&ts)
	if err == nil {
		m.logger.Debug("tdengine mount", "table", statement, "mount", mount, "record", "already exists")
		return nil
	} else if err != sql.ErrNoRows {
		m.logger.Error("failed to check existing", "table", tname, "mount", mount, "error", err)
		return fmt.Errorf("failed to check existing %w", err)
	}

	statement = `INSERT INTO ` + tname + ` VALUES (now, "` + mount + `", "` + typ + `")`
	_, err = m.db.ExecContext(ctx, statement)
	if err != nil {
		m.logger.Error("failed to mount", "table", statement, "mount", mount, "error", err)
		return fmt.Errorf("failed to mount %w", err)
	}

	m.logger.Debug("tdengine mount", "table", tname, "mount", mount)
	return nil
}

func (m *TDEngineBackend) RemoveMount(ctx context.Context, path string, typ ...string) error {
	defer metrics.MeasureSince([]string{"tdengine", "unmount"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	tname, err := m.getTablename(ctx, "mount")
	if err != nil {
		return err
	}

	var ts time.Time
	statement := `SELECT ts FROM ` + tname + ` WHERE k="` + quote(path) + `"`
	if len(typ) > 0 {
		statement += ` AND v="` + quote(typ[0]) + `"`
	}
	err = m.db.QueryRowContext(ctx, statement).Scan(&ts)
	if err == sql.ErrNoRows {
		m.logger.Debug("tdengine unmount", "table", statement, "mount", path, "record", "not found")
		return nil
	} else if err != nil {
		m.logger.Error("failed to check existing", "table", tname, "mount", path, "error", err)
		return fmt.Errorf("failed to check existing %w", err)
	}

	s := strings.Split(ts.String(), " ")
	statement = `DELETE FROM ` + tname + ` WHERE ts="` + strings.Join(s[:2], " ") + `"`
	_, err = m.db.ExecContext(ctx, statement)
	if err != nil {
		m.logger.Error("failed to unmount", "table", statement, "mount", path, "error", err)
		return fmt.Errorf("failed to unmount %w", err)
	}

	m.logger.Debug("tdengine unmount", "table", statement, "mount", path)
	return nil
}

func (m *TDEngineBackend) ListMounts(ctx context.Context, path ...string) ([]string, error) {
	defer metrics.MeasureSince([]string{"tdengine", "list_mount"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	tname, err := m.getTablename(ctx, "mount")
	if err != nil {
		return nil, err
	}

	statement := `SELECT k FROM `
	if len(path) == 0 {
		statement += tname
	} else {
		statement += m.database + `.mount WHERE k = "` + quote(path[0]) + `"`
	}
	rows, err := m.db.QueryContext(ctx, statement)
	if err != nil {
		m.logger.Error("failed to list mount", "table", statement, "error", err)
		return nil, fmt.Errorf("failed to list mount %w", err)
	}
	defer rows.Close()

	var mounts []string
	for rows.Next() {
		var mount string
		err = rows.Scan(&mount)
		if err != nil {
			return nil, fmt.Errorf("failed to scan rows: %w", err)
		}
		mounts = append(mounts, mount)
	}

	m.logger.Debug("tdengine list mount", "table", tname, "mounts", mounts)
	return mounts, nil
}
