package postgres

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/google/uuid"
	"github.com/ra-company/database"
	"github.com/ra-company/logging"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const startErrorMessage = "PostgreSQL start error: %v"

type Config struct {
	Hosts           string
	User            string
	Password        string
	DB              string
	TLS             *tls.Config
	DoNotLogQueries bool
}

type PostgresClient struct {
	logging.CustomLogger               // CustomLogger: is an embedded field that allows the PostgresClient to use custom logging functionality.
	client               *pgxpool.Pool // client: is a pointer to the PostgreSQL connection pool.
	doNotLogQueries      bool          // doNotLogQueries: is a boolean flag that indicates whether SQL queries should be logged or not. If set to true, queries will not be logged.
}

// Start initializes the PostgreSQL connection pool with the provided credentials and database information.
// It logs an error and exits the application if the connection fails.
// The connection string is formatted as "postgres://username:password@host/db"
// Function detects if the host contains multiple addresses separated by commas and appends "?target_session_attrs=read-write" to the connection string for cluster setups.
// This allows the client to connect to a primary server for read-write operations.
// It also pings the database to ensure the connection is established.
// If the connection is successful, it logs the connection details.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - config: A Config struct containing the necessary information to connect to the PostgreSQL database, including Host, Username, Password, DB, and optional TLS configuration.
func (dst *PostgresClient) Start(ctx context.Context, config *Config) {
	var err error
	var connectionString string

	dst.doNotLogQueries = config.DoNotLogQueries

	if strings.Contains(config.Hosts, ",") {
		connectionString = fmt.Sprintf("postgres://%s@%s/%s?target_session_attrs=read-write", url.UserPassword(config.User, config.Password), config.Hosts, config.DB)
	} else {
		connectionString = fmt.Sprintf("postgres://%s@%s/%s", url.UserPassword(config.User, config.Password), config.Hosts, config.DB)
	}

	cfg, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		dst.Fatalf(ctx, startErrorMessage, err)
		return
	}

	if config.TLS != nil {
		cfg.ConnConfig.TLSConfig = config.TLS
	}

	dst.client, err = pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		dst.Fatalf(ctx, startErrorMessage, err)
		return
	}

	err = dst.client.Ping(ctx)
	if err != nil {
		dst.Fatalf(ctx, startErrorMessage, err)
		return
	}

	if strings.Contains(config.Hosts, ",") {
		dst.Infof(ctx, "Connected to PostgreSQL Database: cluster - %v, database - %v, user - %v", config.Hosts, config.DB, config.User)
	} else {
		dst.Infof(ctx, "Connected to PostgreSQL Database: host - %v, database - %v, user - %v", config.Hosts, config.DB, config.User)
	}

	var fullVersion string
	err = dst.client.QueryRow(ctx, "SELECT version()").Scan(&fullVersion)
	if err != nil {
		dst.Fatalf(ctx, "Query failed: %v", err)
	}
	dst.Infof(ctx, "PostgreSQL version %s", fullVersion)
}

// Stop closes the PostgreSQL connection pool and logs a message indicating that the disconnection was successful.
// It does not return any error.
// It is typically called when the application is shutting down to ensure that all resources are released properly.
// It is important to call this function to avoid resource leaks and ensure that the application exits cleanly.
// It is recommended to call this function in a deferred manner after the connection pool is successfully created.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
func (dst *PostgresClient) Stop(ctx context.Context) {
	if dst.client != nil {
		dst.client.Close()
	}
	dst.Info(ctx, "Disconnected from PostgreSQL Database")
}

// Select data from database and scan into data structure
// The function executes a SQL query to select data from the database and scans the result into the provided data structure.
// It logs the time taken for the query execution and the query itself for debugging purposes.
// If the query execution is successful, it returns nil.
// If an error occurs during the query execution, it returns the error.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - model: The name of the model being queried, used for logging.
//   - query: The SQL query string to be executed in the database.
//   - data: A pointer to the data structure where the result will be scanned into.
func (dst *PostgresClient) Select(ctx context.Context, model string, query string, data any) error {
	start := time.Now()

	err := pgxscan.Select(ctx, dst.client, data, query)
	dst.LogDefault(ctx, model, "Load", query, start)

	return err
}

// Insert data into database and return inserted IDs
// The function starts a transaction, executes the insert query, and returns the IDs of the inserted records.
// If an error occurs during the transaction, it rolls back the transaction and returns the error.
// If the transaction is successful, it commits the transaction and returns the IDs of the inserted records.
// The function logs the time taken for each step of the transaction for debugging purposes.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - model: The name of the model being inserted, used for logging.
//   - query: The SQL query string for inserting data into the database.
//
// Returns:
//   - A slice of uint containing the IDs of the inserted records.
//   - An error if the operation fails, or nil if it succeeds.
func (dst *PostgresClient) Insert(ctx context.Context, model string, query string) ([]uint, error) {
	ids := []uint{}
	tx, err := dst.BeginTransaction(ctx)
	if err != nil {
		return ids, err
	}

	start := time.Now()

	var res pgx.Rows
	res, err = tx.Query(ctx, query+" RETURNING id")
	dst.LogWarning(ctx, model, "Create", query, start)
	if err != nil {
		dst.Errorf(ctx, "Failed to execute insert query: %v", err)
		dst.RollbackTransaction(ctx, tx)
		return ids, err
	}

	var n uint
	_, err = pgx.ForEachRow(res, []any{&n}, func() error {
		ids = append(ids, n)
		return nil
	})

	if err != nil {
		dst.Errorf(ctx, "Failed to execute insert query: %v", err)
		dst.RollbackTransaction(ctx, tx)
		return ids, err
	}

	if err = dst.CommitTransaction(ctx, tx); err != nil {
		dst.Errorf(ctx, "Failed to commit transaction: %v", err)
		return ids, err
	}

	return ids, nil
}

// InsertUUID data into database and return inserted UUIDs
// The function starts a transaction, executes the insert query, and returns the UUIDs of the inserted records.
// If an error occurs during the transaction, it rolls back the transaction and returns the error.
// If the transaction is successful, it commits the transaction and returns the UUIDs of the inserted records.
// The function logs the time taken for each step of the transaction for debugging purposes.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - model: The name of the model being inserted, used for logging.
//   - query: The SQL query string for inserting data into the database.
//
// Returns:
//   - A slice of uuid.UUID containing the UUIDs of the inserted records.
//   - An error if the operation fails, or nil if it succeeds.
func (dst *PostgresClient) InsertUUID(ctx context.Context, model string, query string) ([]uuid.UUID, error) {
	ids := []uuid.UUID{}
	tx, err := dst.BeginTransaction(ctx)
	if err != nil {
		return ids, err
	}

	start := time.Now()

	var res pgx.Rows
	res, err = tx.Query(ctx, query+" RETURNING id")
	dst.LogInfo(ctx, model, "Create", query, start)
	if err != nil {
		dst.Errorf(ctx, "Failed to execute insert query: %v", err)
		dst.RollbackTransaction(ctx, tx)
		return ids, err
	}

	var n uuid.UUID
	_, err = pgx.ForEachRow(res, []any{&n}, func() error {
		ids = append(ids, n)
		return nil
	})

	if err != nil {
		dst.Errorf(ctx, "Failed to execute insert query: %v", err)
		dst.RollbackTransaction(ctx, tx)
		return ids, err
	}

	if err = dst.CommitTransaction(ctx, tx); err != nil {
		dst.Errorf(ctx, "Failed to commit transaction: %v", err)
		return ids, err
	}

	return ids, nil
}

// Update data in database and return affected rows count
// The function starts a transaction, executes the update query, and returns the number of affected rows.
// If an error occurs during the transaction, it rolls back the transaction and returns the error.
// If the transaction is successful, it commits the transaction and returns the number of affected rows.
// The function logs the time taken for each step of the transaction for debugging purposes.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - model: The name of the model being updated, used for logging.
//   - query: The SQL query string for updating data in the database.
//
// Returns:
//   - A uint representing the number of affected rows.
//   - An error if the operation fails, or nil if it succeeds.
func (dst *PostgresClient) Update(ctx context.Context, model string, query string) (uint, error) {
	if query == "" {
		return 0, database.ErrorIncorrectRequest
	}

	tx, err := dst.BeginTransaction(ctx)
	if err != nil {
		return 0, err
	}

	start := time.Now()

	var res pgconn.CommandTag
	res, err = tx.Exec(ctx, query)
	dst.LogWarning(ctx, model, "Update", query, start)
	if err != nil {
		dst.Errorf(ctx, "Failed to execute update query: %v", err)
		dst.RollbackTransaction(ctx, tx)
		return 0, err
	}

	if !res.Update() {
		dst.Errorf(ctx, "Failed to update record: %v", database.ErrorIncorrectRequest)
		dst.RollbackTransaction(ctx, tx)
		return 0, database.ErrorIncorrectRequest
	}

	if err = dst.CommitTransaction(ctx, tx); err != nil {
		return 0, err
	}

	return uint(res.RowsAffected()), nil
}

// Delete data from database
// The function starts a transaction, executes the delete query, and returns the number of affected rows.
// If an error occurs during the transaction, it rolls back the transaction and returns the error.
// If the transaction is successful, it commits the transaction and returns the number of affected rows.
// The function logs the time taken for each step of the transaction for debugging purposes.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - model: The name of the model being deleted, used for logging.
//   - query: The SQL query string for deleting data from the database.
//
// Returns:
//   - A uint representing the number of affected rows.
//   - An error if the operation fails, or nil if it succeeds.
func (dst *PostgresClient) Delete(ctx context.Context, model string, query string) (uint, error) {
	tx, err := dst.BeginTransaction(ctx)
	if err != nil {
		return 0, err
	}

	start := time.Now()

	var res pgconn.CommandTag
	res, err = tx.Exec(ctx, query)
	dst.LogDanger(ctx, model, "Delete", query, start)
	if err != nil {
		dst.RollbackTransaction(ctx, tx)
		return 0, err
	}

	if !res.Delete() {
		dst.Errorf(ctx, "Failed to delete record: %v", database.ErrorIncorrectRequest)
		dst.RollbackTransaction(ctx, tx)
		return 0, database.ErrorIncorrectRequest
	}

	if err := dst.CommitTransaction(ctx, tx); err != nil {
		return 0, err
	}

	return uint(res.RowsAffected()), nil
}

// Return records count in database
// The function executes a SQL query to count the number of records in a specified model.
// It logs the time taken for the query execution and the query itself for debugging purposes.
// If the query execution is successful, it returns the count of records as a uint64.
// If an error occurs during the query execution, it returns an error.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - model: The name of the model being counted, used for logging.
//   - query: The SQL query string for counting records in the database.
//
// Returns:
//   - A uint64 representing the number of records in the specified model.
//   - An error if the operation fails, or nil if it succeeds.
func (dst *PostgresClient) Count(ctx context.Context, model string, query string) (uint64, error) {
	start := time.Now()

	var n uint64
	err := dst.client.QueryRow(ctx, query).Scan(&n)
	dst.LogDefault(ctx, model, "Count", query, start)
	if err != nil {
		return 0, err
	}

	return n, nil
}

// Return maximum field value in database
// The function executes a SQL query to find the maximum value of a specified field in a model.
// It logs the time taken for the query execution and the query itself for debugging purposes.
// If the query execution is successful, it returns the maximum value as a uint64.
// If an error occurs during the query execution, it returns an error.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - model: The name of the model being queried, used for logging.
//   - query: The SQL query string for finding the maximum value in the database.
//
// Returns:
//   - A uint64 representing the maximum value of the specified field in the model.
//   - An error if the operation fails, or nil if it succeeds.
func (dst *PostgresClient) Max(ctx context.Context, model, query string) (uint64, error) {
	start := time.Now()

	var n uint64
	err := dst.client.QueryRow(ctx, query).Scan(&n)
	dst.LogDefault(ctx, model, "MAX", query, start)
	if err != nil {
		return 0, err
	}

	return n, nil
}

// Execute query without result
// The function executes a SQL query without expecting any result.
// It logs the time taken for the query execution and the query itself for debugging purposes.
// If the query execution is successful, it returns nil.
// If an error occurs during the query execution, it returns the error.
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - model: The name of the model being queried, used for logging.
//   - query: The SQL query string to be executed in the database.
//
// Returns:
//   - An error if the operation fails, or nil if it succeeds.
func (dst *PostgresClient) Exec(ctx context.Context, model string, query string) error {
	start := time.Now()

	_, err := dst.client.Exec(ctx, query)
	dst.LogDefault(ctx, model, "Exec", query, start)
	if err != nil {
		return err
	}

	return nil
}

// Client returns the PostgreSQL connection pool.
// It is used to access the underlying pgxpool.Pool instance for executing queries and transactions.
// This function is typically called when you need to perform operations directly on the PostgreSQL database.
func (dst *PostgresClient) Client() *pgxpool.Pool {
	return dst.client
}

// BeginTransaction starts a new transaction and returns the transaction object.
// It logs the time taken to begin the transaction for debugging purposes.
// If an error occurs while starting the transaction, it returns the error.
// The caller is responsible for committing or rolling back the transaction after performing the necessary operations.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//
// Returns:
//   - A pointer to the pgx.Tx transaction object if the transaction is successfully started.
//   - An error if there is an issue starting the transaction.
func (dst *PostgresClient) BeginTransaction(ctx context.Context) (pgx.Tx, error) {
	start := time.Now()
	tx, err := dst.client.Begin(ctx)
	if !dst.doNotLogQueries {
		dst.Debugf(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[35mBEGIN\033[0m", float64(time.Since(start))/1000000)
	}
	if err != nil {
		return nil, err
	}
	return tx, nil
}

// RollbackTransaction rolls back the given transaction and logs the time taken for the rollback operation.
// If an error occurs during the rollback, it logs the error.
// This function is typically called when an error occurs during a transaction and you want to undo any changes made during that transaction.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - tx: The pgx.Tx transaction object that needs to be rolled back.
func (dst *PostgresClient) RollbackTransaction(ctx context.Context, tx pgx.Tx) {
	start := time.Now()
	err := tx.Rollback(ctx)
	if !dst.doNotLogQueries {
		dst.Debugf(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[31mROLLBACK\033[0m", float64(time.Since(start))/1000000)
	}
	if err != nil {
		dst.Errorf(ctx, "Failed to rollback transaction: %v", err)
	}
}

// CommitTransaction commits the given transaction and logs the time taken for the commit operation.
// If an error occurs during the commit, it logs the error and returns it.
// This function is typically called after successfully performing all necessary operations within a transaction to save the changes to the database.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - tx: The pgx.Tx transaction object that needs to be committed.
//
// Returns:
//   - An error if there is an issue committing the transaction, or nil if the commit is successful.
func (dst *PostgresClient) CommitTransaction(ctx context.Context, tx pgx.Tx) error {
	start := time.Now()

	err := tx.Commit(ctx)
	if !dst.doNotLogQueries {
		dst.Debugf(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[35mCOMMIT\033[0m", float64(time.Since(start))/1000000)
	}
	if err != nil {
		dst.Errorf(ctx, "Failed to commit transaction: %v", err)
	}
	return err
}

// Put query string to the log with default colors
// The function logs the SQL query string along with the time taken for the query execution.
// It is typically used for debugging purposes to track the performance of SQL queries.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - model: The name of the model being queried, used for logging.
//   - action: The action being performed, used for logging.
//   - query: The SQL query string to be logged.
func (dst *PostgresClient) LogDefault(ctx context.Context, model, action, query string, start time.Time) {
	if dst.doNotLogQueries {
		return
	}
	dst.Debugf(ctx, "\033[1m\033[36mPG %s %s (%.2f ms)\033[1m \033[34m%s\033[0m", model, action, float64(time.Since(start))/1000000, database.OneLine(query))
}

// Put query string to the log with red color
// The function logs the SQL query string along with the time taken for the query execution in red color.
// It is typically used for debugging purposes to highlight potentially dangerous or problematic SQL queries.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - model: The name of the model being queried, used for logging.
//   - action: The action being performed, used for logging.
//   - query: The SQL query string to be logged.
func (dst *PostgresClient) LogDanger(ctx context.Context, model, action, query string, start time.Time) {
	if dst.doNotLogQueries {
		return
	}
	dst.Debugf(ctx, "\033[1m\033[36mPG %s %s (%.2f ms)\033[1m \033[31m%s\033[0m", model, action, float64(time.Since(start))/1000000, database.OneLine(query))
}

// Put query string to the log with yellow color
// The function logs the SQL query string along with the time taken for the query execution in yellow color.
// It is typically used for debugging purposes to highlight warnings or non-critical issues in SQL queries.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - model: The name of the model being queried, used for logging.
//   - action: The action being performed, used for logging.
//   - query: The SQL query string to be logged.
func (dst *PostgresClient) LogWarning(ctx context.Context, model, action, query string, start time.Time) {
	if dst.doNotLogQueries {
		return
	}
	dst.Debugf(ctx, "\033[1m\033[36mPG %s %s (%.2f ms)\033[1m \033[33m%s\033[0m", model, action, float64(time.Since(start))/1000000, database.OneLine(query))
}

// Put query string to the log with green color
// The function logs the SQL query string along with the time taken for the query execution in green color.
// It is typically used for debugging purposes to highlight successful or non-problematic SQL queries.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - model: The name of the model being queried, used for logging.
//   - action: The action being performed, used for logging.
//   - query: The SQL query string to be logged.
func (dst *PostgresClient) LogInfo(ctx context.Context, model, action, query string, start time.Time) {
	if dst.doNotLogQueries {
		return
	}
	dst.Debugf(ctx, "\033[1m\033[36mPG %s %s (%.2f ms)\033[1m \033[32m%s\033[0m", model, action, float64(time.Since(start))/1000000, database.OneLine(query))
}
