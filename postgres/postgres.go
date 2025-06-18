package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/ra-company/database"
	"github.com/ra-company/logging"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	PG PostgresClient // PG: is a global variable that holds the PostgreSQL client instance.
)

type PostgresClient struct {
	logging.CustomLogger               // CustomLogger: is an embedded field that allows the PostgresClient to use custom logging functionality.
	client               *pgxpool.Pool // client: is a pointer to the PostgreSQL connection pool.
}

// Start initializes the PostgreSQL connection pool with the provided credentials and database information.
// It logs an error and exits the application if the connection fails.
// The connection string is formatted as "postgres://username:password@host:port/dbName".
// It also pings the database to ensure the connection is established.
// If the connection is successful, it logs the connection details.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - username: The username for the PostgreSQL database.
//   - password: The password for the PostgreSQL database.
//   - host: The host where the PostgreSQL database is running.
//   - port: The port on which the PostgreSQL database is listening.
//   - db: The name of the PostgreSQL database to connect to.
func (dst *PostgresClient) Start(ctx context.Context, username, password, host string, port int, db string) {
	var err error
	connectionString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", username, password, host, port, db)

	dst.client, err = pgxpool.New(ctx, connectionString)
	if err != nil {
		dst.Fatal(ctx, "PostgreSQL connection error: %v", err)
	}

	err = dst.client.Ping(ctx)
	if err != nil {
		dst.Fatal(ctx, "PostgreSQL connection error: %v", err)
	}

	dst.Info(ctx, "Connected to PostgreSQL Database: host - %v, port - %v, database - %v, user - %v", host, port, db, username)
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
	dst.client.Close()
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
	dst.Debug(ctx, "\033[1m\033[36mPG %s Load (%.2f ms)\033[1m \033[34m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", " "), "\t", ""))

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
	start := time.Now()

	ids := []uint{}
	tx, err := dst.client.Begin(ctx)
	dst.Debug(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[35mBEGIN\033[0m", float64(time.Since(start))/1000000)
	if err != nil {
		return ids, err
	}

	start = time.Now()

	var res pgx.Rows
	res, err = tx.Query(ctx, query+" RETURNING id")
	dst.Debug(ctx, "\033[1m\033[36mPG %s Create (%.2f ms)\033[1m \033[32m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", ""), "\t", ""))
	if err != nil {
		tx.Rollback(ctx)
		dst.Error(ctx, err)
		dst.Debug(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[31mROLLBACK\033[0m", float64(time.Since(start))/1000000)
		return ids, err
	}

	var n uint
	_, err = pgx.ForEachRow(res, []any{&n}, func() error {
		ids = append(ids, n)
		return nil
	})

	if err != nil {
		tx.Rollback(ctx)
		dst.Debug(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[31mROLLBACK\033[0m", float64(time.Since(start))/1000000)
		return ids, err
	}

	start = time.Now()

	err = tx.Commit(ctx)
	dst.Debug(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[35mCOMMIT\033[0m", float64(time.Since(start))/1000000)
	if err != nil {
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
		return 0, nil
	}

	start := time.Now()

	tx, err := dst.client.Begin(ctx)
	dst.Debug(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[35mBEGIN\033[0m", float64(time.Since(start))/1000000)
	if err != nil {
		return 0, err
	}

	start = time.Now()

	var res pgconn.CommandTag
	res, err = tx.Exec(ctx, query)
	dst.Debug(ctx, "\033[1m\033[36mPG %s Update (%.2f ms)\033[1m \033[33m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", ""), "\t", ""))
	if err != nil {
		tx.Rollback(ctx)
		dst.Error(ctx, err)
		dst.Debug(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[31mROLLBACK\033[0m", float64(time.Since(start))/1000000)
		return 0, err
	}

	if !res.Update() {
		tx.Rollback(ctx)
		dst.Error(ctx, database.ErrorIncorrectRequest)
		dst.Debug(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[31mROLLBACK\033[0m", float64(time.Since(start))/1000000)
		return 0, database.ErrorIncorrectRequest
	}

	start = time.Now()

	err = tx.Commit(ctx)
	dst.Debug(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[35mCOMMIT\033[0m", float64(time.Since(start))/1000000)
	if err != nil {
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
	start := time.Now()

	tx, err := dst.client.Begin(ctx)
	dst.Debug(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[35mBEGIN\033[0m", float64(time.Since(start))/1000000)
	if err != nil {
		return 0, err
	}

	start = time.Now()

	var res pgconn.CommandTag
	res, err = tx.Exec(ctx, query)
	dst.Debug(ctx, "\033[1m\033[36mPG %s Delete (%.2f ms)\033[1m \033[31m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", " "), "\t", ""))
	if err != nil {
		tx.Rollback(ctx)
		dst.Error(ctx, err)
		dst.Debug(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[31mROLLBACK\033[0m", float64(time.Since(start))/1000000)
		return 0, err
	}

	if !res.Delete() {
		tx.Rollback(ctx)
		dst.Error(ctx, database.ErrorIncorrectRequest)
		dst.Debug(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[31mROLLBACK\033[0m", float64(time.Since(start))/1000000)
		return 0, database.ErrorIncorrectRequest
	}

	start = time.Now()

	err = tx.Commit(ctx)
	dst.Debug(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[35mCOMMIT\033[0m", float64(time.Since(start))/1000000)
	if err != nil {
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
	dst.Debug(ctx, "\033[1m\033[36mPG %s Count (%.2f ms)\033[1m \033[34m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", " "), "\t", ""))
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
	dst.Debug(ctx, "\033[1m\033[36mPG %s MAX (%.2f ms)\033[1m \033[34m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", " "), "\t", ""))
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
	dst.Debug(ctx, "\033[1m\033[36mPG %s Exec (%.2f ms)\033[1m \033[34m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", " "), "\t", ""))
	if err != nil {
		return err
	}

	return nil
}

// Put query string to the log
// The function logs the SQL query string along with the time taken for the query execution.
// It is typically used for debugging purposes to track the performance of SQL queries.
//
// Parameters:
//   - ctx: The context for the operation, used for cancellation and timeout.
//   - model: The name of the model being queried, used for logging.
//   - query: The SQL query string to be logged.
func (dst *PostgresClient) LogSelect(ctx context.Context, model string, query string, start time.Time) {
	dst.Debug(ctx, "\033[1m\033[36mPG %s Load (%.2f ms)\033[1m \033[34m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", " "), "\t", ""))
}

// Client returns the PostgreSQL connection pool.
// It is used to access the underlying pgxpool.Pool instance for executing queries and transactions.
// This function is typically called when you need to perform operations directly on the PostgreSQL database.
func (dst *PostgresClient) Client() *pgxpool.Pool {
	return dst.client
}

// ToStr escapes single quotes in a string for use in SQL queries.
// It replaces single quotes with two single quotes to prevent SQL injection attacks.
// This is a common practice in SQL to handle string literals safely.
//
// Parameters:
//   - str: The input string that may contain single quotes.
//
// Returns:
//   - A string with single quotes replaced by two single quotes.
func ToStr(str string) string {
	return strings.Replace(str, "'", "''", -1)
}

// ArrayToString converts a slice of any slice type to a PostgreSQL array string representation.
// It formats the slice into a string that can be used in SQL queries as an array.
// If the input slice is empty, it returns "[]".
// Each element in the slice is also processed by the ToStr function to escape single quotes.
// This is useful for constructing SQL queries that require array parameters.
//
// Parameters:
//   - arr: A slice of strings to be converted to a PostgreSQL array string representation.
//
// Returns:
//   - A string representing the PostgreSQL array format of the input slice.
func ArrayToString(v any) string {
	if v == nil {
		return "[]"
	}

	str := reflect.TypeOf(v).String()

	if strings.Index(str, "[]") == -1 {
		return "[]"
	}

	if str == "[]string" {
		return StringsToString(v.([]string))
	}

	val, _ := json.Marshal(v)
	if string(val) == "null" {
		return "[]"
	}
	return string(val)
}

// StringsToString converts a slice of strings to a PostgreSQL array string representation.
// It formats the slice into a string that can be used in SQL queries as an array.
// The resulting string is enclosed in square brackets and each element is enclosed in single quotes.
// If the input slice is empty, it returns "[]".
// Each element in the slice is processed by the ToStr function to escape single quotes.
// This is useful for constructing SQL queries that require array parameters.
//
// Parameters:
//   - arr: A slice of strings to be converted to a PostgreSQL array string representation.
//
// Returns:
//   - A string representing the PostgreSQL array format of the input slice.
//
// Note: This function assumes that the input slice contains only strings and does not handle other types.
func StringsToString(arr []string) string {
	if len(arr) == 0 {
		return "[]"
	}
	for i, v := range arr {
		arr[i] = ToStr(v)
	}
	return "['" + strings.Join(arr, "','") + "']"
}
