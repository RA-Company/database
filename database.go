package database

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

var (
	ErrorIncorrectParameters = fmt.Errorf("incorrect parameters")
	ErrorIncorrectRequest    = fmt.Errorf("incorrect request")
	ErrorDatabaseError       = fmt.Errorf("database error")
	ErrorNotFound            = fmt.Errorf("not found")
	ErrorIncorrectID         = fmt.Errorf("incorrect ID")
)

// ArrayToString converts a slice of any slice type to a Database array string representation.
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

	if !strings.Contains(str, "[]") {
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

func OneLine(str string) string {
	var spaceRe = regexp.MustCompile(`\s+`)
	str = spaceRe.ReplaceAllString(str, " ")
	return strings.TrimSpace(str)
}
