package postgres

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ra-company/database"
	"github.com/ra-company/logging"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	PgSQL *pgxpool.Pool
)

// Start initializes the PostgreSQL connection pool with the provided credentials and database information.
// It logs an error and exits the application if the connection fails.
// The connection string is formatted as "postgres://username:password@host:port/dbName".
// It also pings the database to ensure the connection is established.
// If the connection is successful, it logs the connection details.
//
// Parameters:
//   - username: The username for the PostgreSQL database.
//   - password: The password for the PostgreSQL database.
//   - host: The host where the PostgreSQL database is running.
//   - port: The port on which the PostgreSQL database is listening.
//   - db: The name of the PostgreSQL database to connect to.
func Start(username, password, host string, port int, db string) {
	var err error
	connectionString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", username, password, host, port, db)

	PgSQL, err = pgxpool.New(context.Background(), connectionString)
	if err != nil {
		logging.Logs.Fatalf("PostgreSQL connection error: %v", err)
		os.Exit(1)
	}

	err = PgSQL.Ping(context.Background())
	if err != nil {
		logging.Logs.Fatalf("PostgreSQL connection error: %v", err)
		os.Exit(1)
	}

	logging.Logs.Infof("Connected to PostgreSQL Database: host - %v, port - %v, database - %v, user - %v", host, port, db, username)
}

// Stop closes the PostgreSQL connection pool and logs a message indicating that the disconnection was successful.
// It does not return any error.
// It is typically called when the application is shutting down to ensure that all resources are released properly.
// It is important to call this function to avoid resource leaks and ensure that the application exits cleanly.
// It is recommended to call this function in a deferred manner after the connection pool is successfully created.
func Stop() {
	PgSQL.Close()
	logging.Logs.Info("Disconnected from PostgreSQL Database")
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
func Insert(ctx context.Context, model string, query string) ([]uint, error) {
	start := time.Now()

	ids := []uint{}
	tx, err := PgSQL.Begin(context.Background())
	logging.Logs.Debugf(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[35mBEGIN\033[0m", float64(time.Since(start))/1000000)
	if err != nil {
		return ids, err
	}

	start = time.Now()

	var res pgx.Rows
	res, err = tx.Query(context.Background(), query+" RETURNING id")
	logging.Logs.Debugf(ctx, "\033[1m\033[36mPG %s Create (%.2f ms)\033[1m \033[32m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", ""), "\t", ""))
	if err != nil {
		tx.Rollback(ctx)
		logging.Logs.Errorf(ctx, err)
		logging.Logs.Debugf(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[31mROLLBACK\033[0m", float64(time.Since(start))/1000000)
		return ids, err
	}

	var n uint
	_, err = pgx.ForEachRow(res, []any{&n}, func() error {
		ids = append(ids, n)
		return nil
	})

	if err != nil {
		tx.Rollback(ctx)
		logging.Logs.Debugf(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[31mROLLBACK\033[0m", float64(time.Since(start))/1000000)
		return ids, err
	}

	start = time.Now()

	err = tx.Commit(ctx)
	logging.Logs.Debugf(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[35mCOMMIT\033[0m", float64(time.Since(start))/1000000)
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
func Update(ctx context.Context, model string, query string) (uint, error) {
	if query == "" {
		return 0, nil
	}

	start := time.Now()

	tx, err := PgSQL.Begin(context.Background())
	logging.Logs.Debugf(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[35mBEGIN\033[0m", float64(time.Since(start))/1000000)
	if err != nil {
		return 0, err
	}

	start = time.Now()

	var res pgconn.CommandTag
	res, err = tx.Exec(ctx, query)
	logging.Logs.Debugf(ctx, "\033[1m\033[36mPG %s Update (%.2f ms)\033[1m \033[33m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", ""), "\t", ""))
	if err != nil {
		tx.Rollback(ctx)
		logging.Logs.Errorf(ctx, err)
		logging.Logs.Debugf(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[31mROLLBACK\033[0m", float64(time.Since(start))/1000000)
		return 0, err
	}

	if !res.Update() {
		tx.Rollback(ctx)
		logging.Logs.Errorf(ctx, database.ErrorIncorrectRequest)
		logging.Logs.Debugf(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[31mROLLBACK\033[0m", float64(time.Since(start))/1000000)
		return 0, database.ErrorIncorrectRequest
	}

	start = time.Now()

	err = tx.Commit(ctx)
	logging.Logs.Debugf(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[35mCOMMIT\033[0m", float64(time.Since(start))/1000000)
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
func Delete(ctx context.Context, model string, query string) (uint, error) {
	start := time.Now()

	tx, err := PgSQL.Begin(ctx)
	logging.Logs.Debugf(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[35mBEGIN\033[0m", float64(time.Since(start))/1000000)
	if err != nil {
		return 0, err
	}

	start = time.Now()

	var res pgconn.CommandTag
	res, err = tx.Exec(ctx, query)
	logging.Logs.Debugf(ctx, "\033[1m\033[36mPG %s Delete (%.2f ms)\033[1m \033[31m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", " "), "\t", ""))
	if err != nil {
		tx.Rollback(ctx)
		logging.Logs.Errorf(ctx, err)
		logging.Logs.Debugf(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[31mROLLBACK\033[0m", float64(time.Since(start))/1000000)
		return 0, err
	}

	if !res.Delete() {
		tx.Rollback(ctx)
		logging.Logs.Errorf(ctx, database.ErrorIncorrectRequest)
		logging.Logs.Debugf(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[31mROLLBACK\033[0m", float64(time.Since(start))/1000000)
		return 0, database.ErrorIncorrectRequest
	}

	start = time.Now()

	err = tx.Commit(ctx)
	logging.Logs.Debugf(ctx, "\033[1m\033[36mPG TRANSACTION (%.2f ms)\033[0m \033[1m\033[35mCOMMIT\033[0m", float64(time.Since(start))/1000000)
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
func Count(ctx context.Context, model string, query string) (uint64, error) {
	start := time.Now()

	var n uint64
	err := PgSQL.QueryRow(context.Background(), query).Scan(&n)
	logging.Logs.Debugf(ctx, "\033[1m\033[36mPG %s Count (%.2f ms)\033[1m \033[34m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", " "), "\t", ""))
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
func Max(ctx context.Context, model, query string) (uint64, error) {
	start := time.Now()

	var n uint64
	err := PgSQL.QueryRow(context.Background(), query).Scan(&n)
	logging.Logs.Debugf(ctx, "\033[1m\033[36mPG %s MAX (%.2f ms)\033[1m \033[34m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", " "), "\t", ""))
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
func Exec(ctx context.Context, model string, query string) error {
	start := time.Now()

	_, err := PgSQL.Exec(ctx, query)
	logging.Logs.Debugf(ctx, "\033[1m\033[36mPG %s Exec (%.2f ms)\033[1m \033[34m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", " "), "\t", ""))
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
func LogSelect(ctx context.Context, model string, query string, start time.Time) {
	logging.Logs.Debugf(ctx, "\033[1m\033[36mPG %s Load (%.2f ms)\033[1m \033[34m%s\033[0m", model, float64(time.Since(start))/1000000, strings.ReplaceAll(strings.ReplaceAll(query, "\n", " "), "\t", ""))
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

// ArrayToString converts a slice of strings to a PostgreSQL array string representation.
// It formats the slice into a string that can be used in SQL queries as an array.
// The resulting string is enclosed in square brackets and each element is enclosed in single quotes.
// If the input slice is empty, it returns "[]".
// Each element in the slice is also processed by the ToStr function to escape single quotes.
// This is useful for constructing SQL queries that require array parameters.
//
// Parameters:
//   - arr: A slice of strings to be converted to a PostgreSQL array string representation.
//
// Returns:
//   - A string representing the PostgreSQL array format of the input slice.
func ArrayToString(arr []string) string {
	if len(arr) == 0 {
		return "[]"
	}
	for i, v := range arr {
		arr[i] = ToStr(v)
	}
	return "['" + strings.Join(arr, "','") + "']"
}
