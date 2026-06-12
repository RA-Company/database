package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ra-company/database"
	"github.com/ra-company/logging"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Config struct {
	Hosts           string
	User            string
	Password        string
	DB              string
	Settings        clickhouse.Settings
	DoNotLogQueries bool
	TLS             *tls.Config
}

type ClickHouseClient struct {
	logging.CustomLogger
	client          driver.Conn
	doNotLogQueries bool         // If true, queries will not be logged
	inFlight        atomic.Int64 // Number of in-flight queries
}

// Start initializes the ClickHouse client with the provided configuration.
// It connects to the ClickHouse server using the specified username, password, host, and database.
// If the connection fails, it logs an error and exits the application.
// The function also sets various connection settings such as maximum execution time, insert quorum, and compression method.
// It logs the connection details and the ClickHouse server version upon successful connection.
//
// Parameters:
//   - ctx (context.Context): The context for the connection.
//   - config (*Config): A Config struct containing the necessary information to connect to the ClickHouse database, including Hosts, User, Password, DB, Settings, and optional TLS configuration.
//   - tls (*tls.Config): The TLS configuration for secure connections.
func (dst *ClickHouseClient) Start(ctx context.Context, config *Config) {
	var err error
	if config.Settings == nil {
		config.Settings = clickhouse.Settings{
			"max_execution_time":    60,
			"insert_quorum":         2,
			"insert_quorum_timeout": 60000,
		}
	}
	dst.doNotLogQueries = config.DoNotLogQueries
	dst.client, err = clickhouse.Open(&clickhouse.Options{
		Addr: strings.Split(config.Hosts, ","),
		Auth: clickhouse.Auth{
			Database: config.DB,
			Username: config.User,
			Password: config.Password,
		},
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
		Debug: false,
		Debugf: func(format string, v ...any) {
			fmt.Printf(format, v...)
		},
		Settings: config.Settings,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:          time.Second * 30,
		MaxOpenConns:         50,
		MaxIdleConns:         25,
		ConnMaxLifetime:      time.Duration(10) * time.Minute,
		ConnOpenStrategy:     clickhouse.ConnOpenInOrder,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
		TLS:                  config.TLS,
	})

	if err != nil {
		dst.Fatal(ctx, "ClickHouse connection error: %v", err)
	}

	v, err := dst.client.ServerVersion()

	if err != nil {
		dst.Fatal(ctx, "ClickHouse connection error: %v", err)
	}

	dst.Infof(ctx, "Connected to ClickHouse Database: hosts - %v, database - %v, user - %v", config.Hosts, config.DB, config.User)
	dst.Infof(ctx, "ClickHouse Server Version: %v", v)
}

// Stop closes the ClickHouse client connection and logs a message indicating disconnection.
// It does not return any error, as the disconnection is expected to be successful.
// This function is typically called when the application is shutting down or when the ClickHouse client is no longer needed.
// It ensures that the client connection is properly closed to free up resources.
//
// Parameters:
//   - ctx (context.Context): The context for the operation.
func (dst *ClickHouseClient) Stop(ctx context.Context) {
	if dst.client != nil {
		dst.client.Close()
	}
	dst.Info(ctx, "Disconnected from ClickHouse Database")
}

// Insert executes an insert query on the ClickHouse database.
// It takes a context, a model name, and a query string as parameters.
// The function logs the execution time and the query, and returns any error encountered during execution.
// The model name is used for logging purposes to identify the operation being performed.
//
// Parameters:
//   - ctx (context.Context): The context for the operation.
//   - model (string): The name of the model being inserted.
//   - query (string): The SQL query to be executed.
//
// Returns:
//   - error: An error if the execution fails, or nil if it succeeds.
func (dst *ClickHouseClient) Insert(ctx context.Context, model string, query string) error {
	start := time.Now()
	dst.inFlight.Add(1)
	defer dst.inFlight.Add(-1)

	err := dst.client.Exec(ctx, query)
	dst.LogInfo(ctx, model, "Create", query, start)
	return err
}

// Update executes an update query on the ClickHouse database.
// It takes a context, a model name, and a query string as parameters.
// The function logs the execution time and the query, and returns the number of affected rows and any error encountered during execution.
// The model name is used for logging purposes to identify the operation being performed.
//
// Parameters:
//   - ctx (context.Context): The context for the operation.
//   - model (string): The name of the model being updated.
//   - query (string): The SQL query to be executed.
//
// Returns:
//   - uint: The number of affected rows (always 0 for ClickHouse).
//   - error: An error if the execution fails, or nil if it succeeds.
func (dst *ClickHouseClient) Update(ctx context.Context, model string, query string) (uint, error) {
	start := time.Now()
	dst.inFlight.Add(1)
	defer dst.inFlight.Add(-1)

	err := dst.client.Exec(ctx, query)
	dst.LogWarning(ctx, model, "Update", query, start)
	return 0, err
}

// Count executes a count query on the ClickHouse database.
// It takes a context, a model name, and a query string as parameters.
// The function logs the execution time and the query, and returns the count of rows and any error encountered during execution.
// The model name is used for logging purposes to identify the operation being performed.
//
// Parameters:
//   - ctx (context.Context): The context for the operation.
//   - model (string): The name of the model being counted.
//   - query (string): The SQL query to be executed.
//
// Returns:
//   - uint64: The count of rows returned by the query.
//   - error: An error if the execution fails, or nil if it succeeds.
func (dst *ClickHouseClient) Count(ctx context.Context, model string, query string) (uint64, error) {
	start := time.Now()
	dst.inFlight.Add(1)
	defer dst.inFlight.Add(-1)

	var n uint64
	err := dst.client.QueryRow(ctx, query).Scan(&n)
	dst.LogDefault(ctx, model, "Count", query, start)
	if err != nil {
		return 0, err
	}

	return n, nil
}

// Scan executes a query on the ClickHouse database and scans the result into the provided destination variables.
// It takes a context, a model name, a query string, and a variadic list of destination variables.
// The function logs the execution time and the query, and returns any error encountered during execution.
// The model name is used for logging purposes to identify the operation being performed.
//
// Parameters:
//   - ctx (context.Context): The context for the operation.
//   - model (string): The name of the model being queried.
//   - query (string): The SQL query to be executed.
//   - dest (...any): A variadic list of destination variables to scan the result into.
//
// Returns:
//   - error: An error if the execution fails, or nil if it succeeds.
func (dst *ClickHouseClient) Scan(ctx context.Context, model string, query string, dest ...any) error {
	start := time.Now()
	dst.inFlight.Add(1)
	defer dst.inFlight.Add(-1)

	err := dst.client.QueryRow(ctx, query).Scan(dest...)
	dst.LogDefault(ctx, model, "Scan", query, start)

	return err
}

// Select executes a select query on the ClickHouse database.
// It takes a context, a model name, a query string, and a pointer to a data structure to hold the results.
// The function logs the execution time and the query, and returns any error encountered during execution.
// The model name is used for logging purposes to identify the operation being performed.
//
// Parameters:
//   - ctx (context.Context): The context for the operation.
//   - model (string): The name of the model being queried.
//   - query (string): The SQL query to be executed.
//   - data (any): A pointer to a data structure where the results will be stored.
//
// Returns:
//   - error: An error if the execution fails, or nil if it succeeds.
func (dst *ClickHouseClient) Select(ctx context.Context, model string, query string, data any) error {
	start := time.Now()
	dst.inFlight.Add(1)
	defer dst.inFlight.Add(-1)

	err := dst.client.Select(ctx, data, query)
	dst.LogDefault(ctx, model, "Load", query, start)

	return err
}

// Put query string to the log with default colors
// The function logs the SQL query string along with the time taken for the query execution.
// It is typically used for debugging purposes to track the performance of SQL queries.
//
// Parameters:
//   - ctx (context.Context): The context for the operation.
//   - model (string): The name of the model being queried.
//   - action (string): The action being performed.
//   - query (string): The SQL query to be executed.
//   - start (time.Time): The start time of the query execution.
func (dst *ClickHouseClient) LogDefault(ctx context.Context, model, action, query string, start time.Time) {
	dst.logQuery(ctx, "\033[1m\033[36mCH %s %s (%.2f ms)\033[1m \033[34m%s\033[0m", model, action, float64(time.Since(start))/1000000, database.OneLine(query))
}

// Put query string to the log with green color
// The function logs the SQL query string along with the time taken for the query execution in green color.
// It is typically used for debugging purposes to highlight successful or non-problematic SQL queries..
//
// Parameters:
//   - ctx (context.Context): The context for the operation.
//   - model (string): The name of the model being queried.
//   - action (string): The action being performed, used for logging.
//   - query (string): The SQL query to be executed.
//   - start (time.Time): The start time of the query execution.
func (dst *ClickHouseClient) LogInfo(ctx context.Context, model, action, query string, start time.Time) {
	dst.logQuery(ctx, "\033[1m\033[36mCH %s %s (%.2f ms)\033[1m \033[32m%s\033[0m", model, action, float64(time.Since(start))/1000000, database.OneLine(query))
}

// Put query string to the log with yellow color
// The function logs the SQL query string along with the time taken for the query execution in yellow color.
// It is typically used for debugging purposes to highlight warnings or non-critical issues in SQL queries.
//
// Parameters:
//   - ctx (context.Context): The context for the operation.
//   - model (string): The name of the model being queried.
//   - action (string): The action being performed, used for logging.
//   - query (string): The SQL query to be executed.
//   - start (time.Time): The start time of the query execution.
func (dst *ClickHouseClient) LogWarning(ctx context.Context, model, action, query string, start time.Time) {
	dst.logQuery(ctx, "\033[1m\033[36mCH %s %s (%.2f ms)\033[1m \033[33m%s\033[0m", model, action, float64(time.Since(start))/1000000, database.OneLine(query))
}

func (dst *ClickHouseClient) logQuery(args ...any) {
	ctx, ok := args[0].(context.Context)
	if !ok {
		dst.Debugf(args...)
		return
	}
	var str string
	if len(args) > 1 {
		if format, ok := args[1].(string); ok {
			str = fmt.Sprintf(format, args[2:]...)
		} else {
			str = fmt.Sprint(args[1:]...)
		}
	} else {
		str = fmt.Sprint(args[1:]...)
	}
	if dst.doNotLogQueries {
		return
	}
	dst.Debugf(ctx, str)
}

// Client returns the underlying ClickHouse client instance.
// This function is useful for accessing the raw ClickHouse client methods directly,
// allowing for more advanced operations that may not be covered by the ClickHouseClient methods.
// It returns a pointer to the driver.Conn type, which is the ClickHouse client connection.
//
// Returns:
//   - *driver.Conn: A pointer to the ClickHouse client connection.
func (dst *ClickHouseClient) Client() driver.Conn {
	// Returns the underlying ClickHouse client instance.
	return dst.client
}

// InFlightQueries returns the number of queries currently being processed by the ClickHouse client.
// This function is useful for monitoring the query load and understanding how many queries are in progress.
// It returns an int64 representing the number of in-flight queries, which is tracked using an atomic counter.
//
// Returns:
//   - int64: The number of in-flight queries to the ClickHouse database.
func (dst *ClickHouseClient) InFlightQueries() int64 {
	return dst.inFlight.Load()
}
