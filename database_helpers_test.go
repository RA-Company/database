package database

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestToStr(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"no quotes", "hello world", "hello world"},
		{"single quote", "it's", "it''s"},
		{"multiple quotes", "a'b'c", "a''b''c"},
		{"empty string", "", ""},
		{"only quote", "'", "''"},
		{"double quote untouched", `say "hi"`, `say "hi"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ToStr(tt.input))
		})
	}
}

func TestOneLine(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"single line", "hello world", "hello world"},
		{"leading/trailing spaces", "  hello  ", "hello"},
		{"multiple spaces", "a   b   c", "a b c"},
		{"newlines and tabs", "a\n\t b\r\nc", "a b c"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, OneLine(tt.input))
		})
	}
}

func TestStringsToString(t *testing.T) {
	tests := []struct {
		name  string
		input []string
		want  string
	}{
		{"empty", []string{}, "[]"},
		{"one element", []string{"a"}, "['a']"},
		{"multiple elements", []string{"a", "b", "c"}, "['a','b','c']"},
		{"element with single quote", []string{"it's"}, "['it''s']"},
		{"element with multiple quotes", []string{"a'b", "c'd"}, "['a''b','c''d']"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, StringsToString(tt.input))
		})
	}
}

func TestUUIDsToString(t *testing.T) {
	u1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	u2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")

	tests := []struct {
		name  string
		input []uuid.UUID
		want  string
	}{
		{"empty", []uuid.UUID{}, "[]"},
		{"one uuid", []uuid.UUID{u1}, "['00000000-0000-0000-0000-000000000001']"},
		{"two uuids", []uuid.UUID{u1, u2}, "['00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000002']"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, UUIDsToString(tt.input))
		})
	}
}

func TestArrayToStringExtended(t *testing.T) {
	u1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	tests := []struct {
		name  string
		input any
		want  string
	}{
		{"nil string slice", ([]string)(nil), "[]"},
		{"uuid slice", []uuid.UUID{u1}, "['00000000-0000-0000-0000-000000000001']"},
		{"int64 slice", []int64{10, 20}, "[10,20]"},
		{"not a slice", 42, "[]"},
		{"string with quote", []string{"it's"}, "['it''s']"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ArrayToString(tt.input))
		})
	}
}
