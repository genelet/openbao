package taosdata

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/armon/go-metrics"
	log "github.com/hashicorp/go-hclog"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/openbao/openbao/api/v2"
	"github.com/openbao/openbao/sdk/v2/database/helper/dbutil"
	"github.com/openbao/openbao/sdk/v2/physical"
)

const (

	// The lock TTL matches the default that Consul API uses, 15 seconds.
	// Used as part of SQL commands to set/extend lock expiry time relative to
	// database clock.
	TaosdataLockTTLSeconds = 15

	// The amount of time to wait between the lock renewals
	TaosdataLockRenewInterval = 5 * time.Second

	// TaosdataLockRetryInterval is the amount of time to wait
	// if a lock fails before trying again.
	TaosdataLockRetryInterval = time.Second
)

// Verify TaosdataBackend satisfies the correct interfaces
var _ physical.Backend = (*TaosdataBackend)(nil)

// Taosdata Backend is a physical backend that stores data
// within a Taosdata database.
type TaosdataBackend struct {
	table                   string
	client                  *sql.DB
	put_query               string
	get_query               string
	delete_query            string
	list_query              string
	list_page_query         string
	list_page_limited_query string

	upsert_function string

	logger     log.Logger
	permitPool *physical.PermitPool
}

// NewTaosdataBackend constructs a Taosdata backend using the given
// API client, server address, credentials, and database.
func NewTaosdataBackend(conf map[string]string, logger log.Logger) (physical.Backend, error) {
	// Get the Taosdata credentials to perform read/write operations.
	connURL := connectionURL(conf)
	if connURL == "" {
		return nil, fmt.Errorf("missing connection_url")
	}

	unquoted_table, ok := conf["table"]
	if !ok {
		unquoted_table = "openbao_kv_store"
	}
	quoted_table := dbutil.QuoteIdentifier(unquoted_table)

	unquoted_upsert_function, ok := conf["upsert_function"]
	if !ok {
		unquoted_upsert_function = "openbao_kv_put"
	}
	quoted_upsert_function := dbutil.QuoteIdentifier(unquoted_upsert_function)

	maxParStr, ok := conf["max_parallel"]
	var maxParInt int
	var err error
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

	maxIdleConnsStr, maxIdleConnsIsSet := conf["max_idle_connections"]
	var maxIdleConns int
	if maxIdleConnsIsSet {
		maxIdleConns, err = strconv.Atoi(maxIdleConnsStr)
		if err != nil {
			return nil, fmt.Errorf("failed parsing max_idle_connections parameter: %w", err)
		}
		if logger.IsDebug() {
			logger.Debug("max_idle_connections set", "max_idle_connections", maxIdleConnsStr)
		}
	}

	// Create Taosdata handle for the database.
	db, err := sql.Open("pgx", connURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}
	db.SetMaxOpenConns(maxParInt)

	if maxIdleConnsIsSet {
		db.SetMaxIdleConns(maxIdleConns)
	}

	// Determine if we should use a function to work around lack of upsert (versions < 9.5)
	var upsertAvailable bool
	upsertAvailableQuery := "SELECT current_setting('server_version_num')::int >= 90500"
	if err := db.QueryRow(upsertAvailableQuery).Scan(&upsertAvailable); err != nil {
		return nil, fmt.Errorf("failed to check for native upsert: %w", err)
	}

	// Setup our put strategy based on the presence or absence of a native
	// upsert.
	var put_query string
	if !upsertAvailable {
		put_query = "SELECT " + quoted_upsert_function + "($1, $2, $3, $4)"
	} else {
		put_query = "INSERT INTO " + quoted_table + " VALUES($1, $2, $3, $4)" +
			" ON CONFLICT (path, key) DO " +
			" UPDATE SET (parent_path, path, key, value) = ($1, $2, $3, $4)"
	}

	// Setup the backend.
	m := &TaosdataBackend{
		table:        quoted_table,
		client:       db,
		put_query:    put_query,
		get_query:    "SELECT value FROM " + quoted_table + " WHERE path = $1 AND key = $2",
		delete_query: "DELETE FROM " + quoted_table + " WHERE path = $1 AND key = $2",
		list_query: "SELECT key FROM " + quoted_table + " WHERE path = $1" +
			" UNION ALL SELECT DISTINCT substring(substr(path, length($1)+1) from '^.*?/') FROM " + quoted_table +
			" WHERE parent_path LIKE $1 || '%'" +
			" ORDER BY key",
		list_page_query: "SELECT key FROM " + quoted_table + " WHERE path = $1 AND key > $2" +
			" UNION ALL SELECT DISTINCT substring(substr(path, length($1)+1) from '^.*?/') FROM " + quoted_table +
			" WHERE parent_path LIKE $1 || '%' AND substring(substr(path, length($1)+1) from '^.*?/') > $2" +
			" ORDER BY key",
		list_page_limited_query: "SELECT key FROM " + quoted_table + " WHERE path = $1 AND key > $2" +
			" UNION ALL SELECT DISTINCT substring(substr(path, length($1)+1) from '^.*?/') FROM " + quoted_table +
			" WHERE parent_path LIKE $1 || '%' AND substring(substr(path, length($1)+1) from '^.*?/') > $2" +
			" ORDER BY key LIMIT $3",
		logger:          logger,
		permitPool:      physical.NewPermitPool(maxParInt),
		upsert_function: quoted_upsert_function,
	}

	return m, nil
}

// connectionURL first check the environment variables for a connection URL. If
// no connection URL exists in the environment variable, the Vault config file is
// checked. If neither the environment variables or the config file set the connection
// URL for the Postgres backend, because it is a required field, an error is returned.
func connectionURL(conf map[string]string) string {
	connURL := conf["connection_url"]
	if envURL := api.ReadBaoVariable("BAO_PG_CONNECTION_URL"); envURL != "" {
		connURL = envURL
	}

	return connURL
}

// splitKey is a helper to split a full path key into individual
// parts: parentPath, path, key
func (m *TaosdataBackend) splitKey(fullPath string) (string, string, string) {
	var parentPath string
	var path string

	pieces := strings.Split(fullPath, "/")
	depth := len(pieces)
	key := pieces[depth-1]

	if depth == 1 {
		parentPath = ""
		path = "/"
	} else if depth == 2 {
		parentPath = "/"
		path = "/" + pieces[0] + "/"
	} else {
		parentPath = "/" + strings.Join(pieces[:depth-2], "/") + "/"
		path = "/" + strings.Join(pieces[:depth-1], "/") + "/"
	}

	return parentPath, path, key
}

// Put is used to insert or update an entry.
func (m *TaosdataBackend) Put(ctx context.Context, entry *physical.Entry) error {
	defer metrics.MeasureSince([]string{"postgres", "put"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	parentPath, path, key := m.splitKey(entry.Key)

	_, err := m.client.ExecContext(ctx, m.put_query, parentPath, path, key, entry.Value)
	if err != nil {
		return err
	}
	return nil
}

// Get is used to fetch and entry.
func (m *TaosdataBackend) Get(ctx context.Context, fullPath string) (*physical.Entry, error) {
	defer metrics.MeasureSince([]string{"postgres", "get"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	_, path, key := m.splitKey(fullPath)

	var result []byte
	err := m.client.QueryRowContext(ctx, m.get_query, path, key).Scan(&result)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	ent := &physical.Entry{
		Key:   fullPath,
		Value: result,
	}
	return ent, nil
}

// Delete is used to permanently delete an entry
func (m *TaosdataBackend) Delete(ctx context.Context, fullPath string) error {
	defer metrics.MeasureSince([]string{"postgres", "delete"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	_, path, key := m.splitKey(fullPath)

	_, err := m.client.ExecContext(ctx, m.delete_query, path, key)
	if err != nil {
		return err
	}
	return nil
}

// List is used to list all the keys under a given
// prefix, up to the next prefix.
func (m *TaosdataBackend) List(ctx context.Context, prefix string) ([]string, error) {
	defer metrics.MeasureSince([]string{"postgres", "list"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	rows, err := m.client.QueryContext(ctx, m.list_query, "/"+prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		err = rows.Scan(&key)
		if err != nil {
			return nil, fmt.Errorf("failed to scan rows: %w", err)
		}

		keys = append(keys, key)
	}

	return keys, nil
}

// ListPage is used to list all the keys under a given
// prefix, after the given key, up to the given key.
func (m *TaosdataBackend) ListPage(ctx context.Context, prefix string, after string, limit int) ([]string, error) {
	defer metrics.MeasureSince([]string{"postgres", "list-page"}, time.Now())

	m.permitPool.Acquire()
	defer m.permitPool.Release()

	var rows *sql.Rows
	var err error
	if limit <= 0 {
		rows, err = m.client.QueryContext(ctx, m.list_page_query, "/"+prefix, after)
	} else {
		rows, err = m.client.QueryContext(ctx, m.list_page_limited_query, "/"+prefix, after, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		err = rows.Scan(&key)
		if err != nil {
			return nil, fmt.Errorf("failed to scan rows: %w", err)
		}

		keys = append(keys, key)
	}

	return keys, nil
}
