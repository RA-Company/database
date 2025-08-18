package clickhouse

import (
	"fmt"
	"strings"
	"time"
)

// TimeToString converts a time.Time object to a string representation suitable for ClickHouse queries.
// It formats the time in UTC, replacing the 'Z' with an empty string and 'T' with a space.
// The resulting string is in the format 'YYYY-MM-DD HH:MM:SS.ssssss'.
// The function uses a precision of 6 decimal places for the seconds, which is common in ClickHouse.
// This is useful for inserting or querying datetime values in ClickHouse databases.
//
// Parameters:
//   - t (time.Time): The time.Time object to be converted.
//
// Returns:
//   - A string representing the time in ClickHouse format, e.g., "toDateTime64('2023-10-01 12:34:56.789012', 6, 'UTC')".
func TimeToString(t time.Time) string {
	return fmt.Sprintf("toDateTime64('%s', 6, 'UTC')", strings.ReplaceAll(strings.Replace(t.UTC().Format(time.RFC3339Nano), "Z", "", -1), "T", " "))
}

// TimeToString32 converts a time.Time object to a string representation suitable for ClickHouse queries.
// It formats the time in UTC, replacing the 'Z' with an empty string and 'T' with a space.
// The resulting string is in the format 'YYYY-MM-DD HH:MM:SS'.
// This function is useful for inserting or querying datetime values in ClickHouse databases without fractional seconds.
//
// Parameters:
//   - t (time.Time): The time.Time object to be converted.
//
// Returns:
//   - A string representing the time in ClickHouse format, e.g., "2023-10-01 12:34:56".
func TimeToString32(t time.Time) string {
	return strings.ReplaceAll(strings.ReplaceAll(t.UTC().Format(time.RFC3339), "Z", ""), "T", " ")
}
