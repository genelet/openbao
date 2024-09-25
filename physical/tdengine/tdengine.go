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

CREATE TABLE root
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
	"github.com/hashicorp/go-hclog"
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

func NewTDEnginePhysicalBackend(conf map[string]string, logger log.Logger) (physical.Backend, error) {
	return NewTDEngineBackend(conf, logger)
}

// NewTDEngineBackend constructs a TDEngine backend using the given API client and
// server address and credential for accessing tdengine database.
func NewTDEngineBackend(conf map[string]string, logger log.Logger) (*TDEngineBackend, error) {
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

	if logger == nil {
		logger = hclog.Default()
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
		createSTable := conf["create_stable"]
		if createSTable == "" {
			createSTable = `CREATE STABLE IF NOT EXISTS ` + m.database + "." + sTable + ` ( ts timestamp, k VARCHAR(4096), v VARBINARY(60000) ) TAGS ( NamespaceID VARCHAR(1024), NamespacePath VARCHAR(64) )`
		}
		if _, err = db.Exec(createSTable); err != nil {
			return nil, fmt.Errorf("failed to create tdengine stable %s: %w", sTable, err)
		}
		logger.Debug("tdengine stable created", "stable", sTable)
	}

	tname, ok := conf["table"]
	if !ok {
		tname = tablename(namespace.RootNamespace)
		if path := conf["ns_path"]; path != "" {
			tname += "_" + path
		}
	}
	tableExist, err := m.existingTable(tname)
	if err != nil {
		return nil, fmt.Errorf("failed to check tdengine root table exist: %w", err)
	} else if !tableExist {
		id, ok := conf["ns_id"]
		if !ok {
			id = namespace.RootNamespaceID
		}
		id = strings.Trim(id, "/")
		path := conf["ns_path"]
		tname = strings.ReplaceAll(tname, "-", "")
		statement := "CREATE TABLE IF NOT EXISTS " + m.database + "." + tname + " USING " + m.database + "." + m.sTable + ` ( NamespaceID, NamespacePath ) TAGS ( "` + id + `", "` + path + `" )`
		if _, err = m.db.Exec(statement); err != nil {
			return nil, fmt.Errorf("failed to create tdengine table %s: %w", statement, err)
		}
		logger.Debug("tdengine root table created", "table", tname)
	}

	return m, err
}

func quote(s string) string {
	return strings.ReplaceAll(s, `;`, ``)
}

// tablename returns the table name for the current namespace.
func tablename(ns *namespace.Namespace) string {
	return quote(strings.ReplaceAll(strings.ReplaceAll(ns.ID, "-", ""), "/", "_"))
}

// get table name from context namespace.
func (m *TDEngineBackend) getTablename(ctx context.Context) (string, error) {
	ns, err := namespace.FromContext(ctx)
	if err != nil && err == namespace.ErrNoNamespace {
		if m.conf != nil && m.conf["ns_path"] != "" {
			return m.database + `.` + namespace.RootNamespaceID + "_" + m.conf["ns_path"], nil
		}
		return m.database + `.` + namespace.RootNamespaceID, nil
	} else if err != nil {
		return "", fmt.Errorf("namespace in ctx error: %w", err)
	}

	if m.conf != nil && m.conf["ns_path"] != "" {
		path := strings.ReplaceAll(m.conf["ns_path"], "-", "")
		return m.database + `.` + tablename(ns) + "_" + path, nil
	}
	return m.database + `.` + tablename(ns), nil
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
func (m *TDEngineBackend) CreateIfNotExists(ctx context.Context, ns1 string) error {
	ns0, err := namespace.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to get namespace from context: %w", err)
	}

	parent := tablename(ns0)
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

	ns1 = strings.Trim(ns1, "/")
	if strings.Contains(ns1, "_") {
		return fmt.Errorf("invalid namespace path %s", ns1)
	}

	tname := parent + "_" + ns1
	tname = strings.ReplaceAll(tname, "-", "")
	id := ns0.ID + "/" + ns1
	p := ns0.Path

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
		m.logger.Error("failed to check tdengine table exist", "tname", tname, "error", err)
		return fmt.Errorf("failed to check tdengine parent table exist: %w", err)
	} else if !tableExist {
		m.logger.Error("namespace not found", "tname", tname)
		return fmt.Errorf("namespace not found %s", ns1)
	}

	tagExist, err := m.existingChildren(tname)
	if err != nil {
		m.logger.Error("failed to check tdengine children exist", "table", tname, "error", err)
		return fmt.Errorf("failed to check tdengine children exist: %w", err)
	} else if tagExist {
		m.logger.Error("children namespace found", "table", tname)
		return fmt.Errorf("children namespace found %s", ns1)
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
		m.logger.Debug("tdengine get", "table", tname, "key", key, "record", "not found")
		return nil, nil
	} else if err != nil {
		m.logger.Error("failed to query", "table", tname, "key", key, "error", err)
		return nil, fmt.Errorf("failed to query %w", err)
	}

	m.logger.Debug("tdengine get", "table", tname, "key", key)

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
	var ts time.Time
	err = m.db.QueryRowContext(ctx, `SELECT ts FROM `+tname+` WHERE k="`+key+`" ORDER BY ts DESC LIMIT 1`).Scan(&ts)
	if err != nil && err != sql.ErrNoRows {
		m.logger.Error("failed to check existing", "table", tname, "key", key, "error", err)
		return fmt.Errorf("failed to check existing %w", err)
	} else if err != sql.ErrNoRows { // existing
		if patch == 1 {
			return nil
		} else {
			s := strings.Split(fmt.Sprintf("%s", ts), " ")
			_, err = m.db.ExecContext(ctx, `DELETE FROM `+tname+` WHERE ts="`+strings.Join(s[:2], " ")+`"`)
			if err != nil {
				m.logger.Error("failed to delete", "table", tname, "key", key, "error", err)
				return fmt.Errorf("failed to delete %w", err)
			}
		}
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
		m.logger.Error("failed to item", "table", tname, "statement", statement, "error", err)
		return nil, fmt.Errorf("failed to item %w", err)
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

	m.logger.Debug("tdengine item", "table", tname)
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
