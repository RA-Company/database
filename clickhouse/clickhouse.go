package clickhouse

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/ra-company/logging"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var (
	CH ClickHouseClient
)

type ClickHouseClient struct {
	logging.CustomLogger
	client          driver.Conn
	DoNotLogQueries bool   // If true, queries will not be logged
	lastQuery       string // Last executed query
}

// Start initializes the ClickHouse client with the provided configuration.
// It connects to the ClickHouse server using the specified username, password, host, and database.
// If the connection fails, it logs an error and exits the application.
// The function also sets various connection settings such as maximum execution time, insert quorum, and compression method.
// It logs the connection details and the ClickHouse server version upon successful connection.
//
// Parameters:
//   - ctx (context.Context): The context for the connection.
//   - hosts (string): The host addresses of the ClickHouse server.
//   - username (string): The username for authentication.
//   - password (string): The password for authentication.
//   - db (string): The name of the database to connect to.
func (dst *ClickHouseClient) Start(ctx context.Context, hosts, username, password, db string) {
	var err error
	dialCount := 0
	dst.client, err = clickhouse.Open(&clickhouse.Options{
		Addr: strings.Split(hosts, ","),
		Auth: clickhouse.Auth{
			Database: db,
			Username: username,
			Password: password,
		},
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			dialCount++
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
		Debug: false,
		Debugf: func(format string, v ...any) {
			fmt.Printf(format, v)
		},
		Settings: clickhouse.Settings{
			"max_execution_time":    60,
			"insert_quorum":         2,
			"insert_quorum_timeout": 60000,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:          time.Second * 30,
		MaxOpenConns:         5,
		MaxIdleConns:         5,
		ConnMaxLifetime:      time.Duration(10) * time.Minute,
		ConnOpenStrategy:     clickhouse.ConnOpenInOrder,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
	})

	if err != nil {
		dst.Fatal(ctx, "ClickHouse connection error: %v", err)
		os.Exit(1)
	}

	v, err := dst.client.ServerVersion()

	if err != nil {
		dst.Fatal(ctx, "ClickHouse connection error: %v", err)
		os.Exit(1)
	}

	dst.Info(ctx, "Connected to ClickHouse Database: hosts - %v, database - %v, user - %v", hosts, db, username)
	dst.Info(ctx, "ClickHouse Server Version: %v", v)
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

	err := dst.client.Exec(ctx, query)
	dst.logQuery(ctx, "\033[1m\033[36mCH %s Create (%.2f ms)\033[1m \033[32m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", " "), "\t", ""))
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

	err := dst.client.Exec(ctx, query)
	dst.logQuery(ctx, "\033[1m\033[36mCH %s Update (%.2f ms)\033[1m \033[33m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", " "), "\t", ""))
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

	var n uint64
	err := dst.client.QueryRow(context.Background(), query).Scan(&n)

	if dst.logQuery(ctx, "\033[1m\033[36mCH %s Count (%.2f ms)\033[1m \033[34m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", " "), "\t", "")); err != nil {
		return 0, err
	}

	return n, nil
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

	err := dst.client.Select(ctx, data, query)
	dst.logQuery(ctx, "\033[1m\033[36mCH %s Load (%.2f ms)\033[1m \033[34m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", " "), "\t", ""))

	return err
}

// LogSelect puts a query to the log with a specific format.
// It takes a context, a model name, a query string, and the start time of the query execution.
// The function formats the log message to include the model name, execution time, and the query itself.
// This function is useful for debugging and monitoring purposes, allowing you to see the performance of your queries.
//
// Parameters:
//   - ctx (context.Context): The context for the operation.
//   - model (string): The name of the model being queried.
//   - query (string): The SQL query to be executed.
//   - start (time.Time): The start time of the query execution.
func (dst *ClickHouseClient) LogSelect(ctx context.Context, model string, query string, start time.Time) {
	dst.Debug(ctx, "\033[1m\033[36mCH %s Load (%.2f ms)\033[1m \033[34m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", " "), "\t", ""))
}

func (dst *ClickHouseClient) logQuery(args ...any) {
	if len(args) > 1 {
		dst.lastQuery = fmt.Sprintf(args[1].(string), args[2:]...)
	} else {
		dst.lastQuery = fmt.Sprint(args[1:]...)
	}
	if dst.DoNotLogQueries {
		return
	}
	dst.Debug(args[0].(context.Context), dst.lastQuery)
}

// LastQuery returns the last executed Redis query as a string.
// This function is useful for debugging purposes, allowing you to see the last query executed by the RedisClient.
//
// Returns:
//   - A string containing the last executed query.
func (dst *ClickHouseClient) LastQuery() string {
	// Returns the last executed query as a string.
	return dst.lastQuery
}

// Client returns the underlying ClickHouse client instance.
// This function is useful for accessing the raw ClickHouse client methods directly,
// allowing for more advanced operations that may not be covered by the ClickHouseClient methods.
// It returns a pointer to the driver.Conn type, which is the ClickHouse client connection.
//
// Returns:
//   - *driver.Conn: A pointer to the ClickHouse client connection.
func (dst *ClickHouseClient) Client() *driver.Conn {
	// Returns the underlying ClickHouse client instance.
	return &dst.client
}
