package postgres

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

// FieldValue is a structure to build a PostgreSQL update query.
// It collects field names and their new values, and can generate an SQL update statement.
// It also tracks the last update time.
// The structure is designed to be used in a context where changes to fields need to be recorded
type FieldValue struct {
	Fields []string
	Values []string
}

// String compares two string values and adds the new value to the Fields and Values slices if they differ.
//
// Parameters:
//   - was: the original string value.
//   - is: the new string value.
//   - field: the name of the field being compared, which will be added to the Fields slice.
func (dst *FieldValue) String(was, is, field string) {
	if was != is {
		dst.AddString(is, field)
	}
}

// AddString adds a new string value to the Fields and Values slices.
// It formats the string value to be suitable for a PostgreSQL query by wrapping it in single quotes.
//
// Parameters:
//   - is: the new string value to be added.
//   - field: the name of the field being added, which will be appended to the Fields slice.
func (dst *FieldValue) AddString(is, field string) {
	dst.Fields = append(dst.Fields, field)
	dst.Values = append(dst.Values, fmt.Sprintf("'%s'", ToStr(is)))
}

// UInt8 compares two uint8 values and adds the new value to the Fields and Values slices if they differ.
//
// Parameters:
//   - was: the original uint8 value.
//   - is: the new uint8 value.
//   - field: the name of the field being compared, which will be added to the Fields slice.
func (dst *FieldValue) UInt8(was, is uint8, field string) {
	dst.Int64(int64(was), int64(is), field)
}

// UInt16 compares two uint16 values and adds the new value to the Fields and Values slices if they differ.
//
// Parameters:
//   - was: the original uint16 value.
//   - is: the new uint16 value.
//   - field: the name of the field being compared, which will be added to the Fields slice.
func (dst *FieldValue) UInt16(was, is uint16, field string) {
	dst.Int64(int64(was), int64(is), field)
}

// UInt32 compares two uint32 values and adds the new value to the Fields and Values slices if they differ.
//
// Parameters:
//   - was: the original uint32 value.
//   - is: the new uint32 value.
//   - field: the name of the field being compared, which will be added to the Fields slice.
func (dst *FieldValue) UInt32(was, is uint32, field string) {
	dst.Int64(int64(was), int64(is), field)
}

// UInt64 compares two uint64 values and adds the new value to the Fields and Values slices if they differ.
//
// Parameters:
//   - was: the original uint64 value.
//   - is: the new uint64 value.
//   - field: the name of the field being compared, which will be added to the Fields slice.
func (dst *FieldValue) UInt64(was, is uint64, field string) {
	if was != is {
		dst.AddUInt64(is, field)
	}
}

// Int8 compares two int8 values and adds the new value to the Fields and Values slices if they differ.
//
// Parameters:
//   - was: the original int8 value.
//   - is: the new int8 value.
//   - field: the name of the field being compared, which will be added to the Fields slice.
func (dst *FieldValue) Int8(was, is int8, field string) {
	if was != is {
		dst.AddInt64(int64(is), field)
	}
}

// Int16 compares two int16 values and adds the new value to the Fields and Values slices if they differ.
//
// Parameters:
//   - was: the original int16 value.
//   - is: the new int16 value.
//   - field: the name of the field being compared, which will be added to the Fields slice.
func (dst *FieldValue) Int16(was, is int16, field string) {
	if was != is {
		dst.AddInt64(int64(is), field)
	}
}

// Int32 compares two int32 values and adds the new value to the Fields and Values slices if they differ.
//
// Parameters:
//   - was: the original int32 value.
//   - is: the new int32 value.
//   - field: the name of the field being compared, which will be added to the Fields slice.
func (dst *FieldValue) Int32(was, is int32, field string) {
	if was != is {
		dst.AddInt64(int64(is), field)
	}
}

// Int64 compares two int64 values and adds the new value to the Fields and Values slices if they differ.
//
// Parameters:
//   - was: the original int64 value.
//   - is: the new int64 value.
//   - field: the name of the field being compared, which will be added to the Fields slice.
func (dst *FieldValue) Int64(was, is int64, field string) {
	if was != is {
		dst.AddInt64(is, field)
	}
}

// AddUInt64 adds a new int64 value to the Fields and Values slices.
// It formats the int64 value as a string for use in a PostgreSQL query.
//
// Parameters:
//   - is: the new uint64 value to be added.
//   - field: the name of the field being added, which will be appended to the Fields slice.
func (dst *FieldValue) AddInt64(is int64, field string) {
	dst.Fields = append(dst.Fields, field)
	dst.Values = append(dst.Values, fmt.Sprintf("%d", is))
}

// AddUInt64 adds a new uint64 value to the Fields and Values slices.
// It formats the uint64 value as a string suitable for a PostgreSQL query.
//
// Parameters:
//   - is: the new uint64 value to be added.
//   - field: the name of the field being added, which will be appended to the Fields slice.
func (dst *FieldValue) AddUInt64(is uint64, field string) {
	dst.Fields = append(dst.Fields, field)
	dst.Values = append(dst.Values, fmt.Sprintf("%d", is))
}

// Bool compares two boolean values and adds the new value to the Fields and Values slices if they differ.
//
// Parameters:
//   - was: the original boolean value.
//   - is: the new boolean value.
//   - field: the name of the field being compared, which will be added to the Fields slice.
func (dst *FieldValue) Bool(was, is bool, field string) {
	if was != is {
		dst.AddBool(is, field)
	}
}

// AddBool adds a new boolean value to the Fields and Values slices.
// It formats the boolean value as a string suitable for a PostgreSQL query.
//
// Parameters:
//   - is: the new boolean value to be added.
//   - field: the name of the field being added, which will be appended to the Fields slice.
func (dst *FieldValue) AddBool(is bool, field string) {
	dst.Fields = append(dst.Fields, field)
	dst.Values = append(dst.Values, fmt.Sprintf("%t", is))
}

// Time compares two time.Time values and adds the new value to the Fields and Values slices if they differ.
//
// Parameters:
//   - was: the original time.Time value.
//   - is: the new time.Time value.
//   - field: the name of the field being compared, which will be added to the Fields slice.
func (dst *FieldValue) Time(was, is time.Time, field string) {
	if was != is {
		dst.AddTime(is, field)
	}
}

// AddTime adds a new time.Time value to the Fields and Values slices.
// It formats the time value in UTC and as a string suitable for a PostgreSQL query.
// Parameters:
//   - is: the new time.Time value to be added.
//   - field: the name of the field being added, which will be appended to the Fields slice.
func (dst *FieldValue) AddTime(is time.Time, field string) {
	dst.Fields = append(dst.Fields, field)
	dst.Values = append(dst.Values, fmt.Sprintf("'%s'", is.UTC().Format(time.RFC3339Nano)))
}

// AddJSON adds a new JSON value to the Fields and Values slices.
// It marshals the input interface into JSON format and appends it to the Values slice.
// If the marshaling fails, it logs an error message.
//
// Parameters:
//   - is: the input interface to be marshaled into JSON.
//   - field: the name of the field being added, which will be appended to the Fields slice.
func (dst *FieldValue) AddJSON(is any, field string) error {
	data, err := json.Marshal(is)
	if err != nil {
		return err
	}
	dst.Fields = append(dst.Fields, field)
	dst.Values = append(dst.Values, fmt.Sprintf("'%s'", ToStr(string(data))))
	return nil
}

// Int8Slice compares two slices of int8 values and adds the new slice to the Fields and Values slices if they differ.
//
// Parameters:
//   - was: the original slice of int8 values.
//   - is: the new slice of int8 values.
//   - field: the name of the field being compared, which will be added to the Fields slice.
func (dst *FieldValue) Int8Slice(was, is []int8, field string) {
	if !slices.Equal(was, is) {
		dst.AddInt8Slice(is, field)
	}
}

// AddInt8Slice adds a new slice of int8 values to the Fields and Values slices.
// It formats the slice as a PostgreSQL array string and appends it to the Values slice.
//
// Parameters:
//   - is: the new slice of int8 values to be added.
//   - field: the name of the field being added, which will be appended to the Fields slice.
func (dst *FieldValue) AddInt8Slice(is []int8, field string) {
	dst.Fields = append(dst.Fields, field)
	dst.Values = append(dst.Values, fmt.Sprintf("ARRAY%s::SMALLINT[]", ArrayToString(is)))
}

// Int16Slice compares two slices of int16 values and adds the new slice to the Fields and Values slices if they differ.
//
// Parameters:
//   - was: the original slice of int16 values.
//   - is: the new slice of int16 values.
//   - field: the name of the field being compared, which will be added to the
func (dst *FieldValue) Int16Slice(was, is []int16, field string) {
	if !slices.Equal(was, is) {
		dst.AddInt16Slice(is, field)
	}
}

// AddInt16Slice adds a new slice of int16 values to the Fields and Values slices.
// It formats the slice as a PostgreSQL array string and appends it to the Values slice.
//
// Parameters:
//   - is: the new slice of int16 values to be added.
//   - field: the name of the field being added, which will be appended to the
func (dst *FieldValue) AddInt16Slice(is []int16, field string) {
	dst.Fields = append(dst.Fields, field)
	dst.Values = append(dst.Values, fmt.Sprintf("ARRAY%s::SMALLINT[]", ArrayToString(is)))
}

// Int32Slice compares two slices of int32 values and adds the new slice to the Fields and Values slices if they differ.
// Parameters:
//   - was: the original slice of int32 values.
//   - is: the new slice of int32 values.
//   - field: the name of the field being compared, which will be added to the Fields slice.
func (dst *FieldValue) Int32Slice(was, is []int32, field string) {
	if !slices.Equal(was, is) {
		dst.AddInt32Slice(is, field)
	}
}

// AddInt32Slice adds a new slice of int32 values to the Fields and Values slices.
// It formats the slice as a PostgreSQL array string and appends it to the Values slice.
//
// Parameters:
//   - is: the new slice of int32 values to be added.
//   - field: the name of the field being added, which will be appended to the
func (dst *FieldValue) AddInt32Slice(is []int32, field string) {
	dst.Fields = append(dst.Fields, field)
	dst.Values = append(dst.Values, fmt.Sprintf("ARRAY%s::INTEGER[]", ArrayToString(is)))
}

// Int64Slice compares two slices of int64 values and adds the new slice to the Fields and Values slices if they differ.
//
// Parameters:
//   - was: the original slice of int64 values.
//   - is: the new slice of int64 values.
//   - field: the name of the field being compared, which will be added to the Fields slice.
func (dst *FieldValue) Int64Slice(was, is []int64, field string) {
	if !slices.Equal(was, is) {
		dst.AddInt64Slice(is, field)
	}
}

// AddInt64Slice adds a new slice of int64 values to the Fields and Values slices.
// It formats the slice as a PostgreSQL array string and appends it to the Values slice.
//
// Parameters:
//   - is: the new slice of int64 values to be added.
//   - field: the name of the field being added, which will be appended to the Fields slice.
func (dst *FieldValue) AddInt64Slice(is []int64, field string) {
	dst.Fields = append(dst.Fields, field)
	dst.Values = append(dst.Values, fmt.Sprintf("ARRAY%s::BIGINT[]", ArrayToString(is)))
}

// StringSlice compares two slices of string values and adds the new slice to the Fields and Values slices if they differ.
//
// Parameters:
//   - was: the original slice of string values.
//   - is: the new slice of string values.
//   - field: the name of the field being compared, which will be added to the Fields slice.
func (dst *FieldValue) StringSlice(was, is []string, field string) {
	if !slices.Equal(was, is) {
		dst.AddStringSlice(is, field)
	}
}

// AddStringSlice adds a new slice of string values to the Fields and Values slices.
// It formats the slice as a PostgreSQL array string and appends it to the Values slice.
//
// Parameters:
//   - is: the new slice of string values to be added.
//   - field: the name of the field being added, which will be appended to the Fields slice.
func (dst *FieldValue) AddStringSlice(is []string, field string) {
	dst.Fields = append(dst.Fields, field)
	dst.Values = append(dst.Values, fmt.Sprintf("ARRAY%s::VARCHAR[]", ArrayToString(is)))
}

// UpdateQuery generates a PostgreSQL update query string using the collected fields and values.
// It returns the query string and the current time as the update timestamp.
// If no fields are set, it returns an empty string and the zero time value.
//
// Parameters:
//   - table: the name of the table to update.
//   - id: the identifier of the record to update.
//
// Returns:
//   - string: the generated SQL update query.
//   - time.Time: the current time in UTC, representing the update timestamp.
func (dst *FieldValue) UpdateQuery(table string, id any) (string, time.Time) {
	if len(dst.Fields) == 0 {
		return "", time.Time{}
	}

	update := time.Now().UTC()
	fields := fmt.Sprintf("%s,updated_at", strings.Join(dst.Fields, ","))
	values := fmt.Sprintf("%s,'%s'", strings.Join(dst.Values, ","), update.Format(time.RFC3339Nano))
	switch v := id.(type) {
	case uint:
		return fmt.Sprintf("UPDATE %s SET (%s) = (%s) WHERE id = %d", table, fields, values, v), update
	case uuid.UUID:
		return fmt.Sprintf("UPDATE %s SET (%s) = (%s) WHERE id = '%s'", table, fields, values, v), update
	default:
		return fmt.Sprintf("UPDATE %s SET (%s) = (%s) WHERE id = '%s'", table, fields, values, v), update
	}
}
