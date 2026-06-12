package postgres

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// --- helpers ---

func emptyFV() *FieldValue { return &FieldValue{} }

// --- String / AddString ---

func TestFieldValue_String(t *testing.T) {
	fv := emptyFV()
	fv.String("old", "old", "col")
	require.Empty(t, fv.Fields, "no change — nothing added")

	fv.String("old", "new", "col")
	require.Equal(t, []string{"col"}, fv.Fields)
	require.Equal(t, []string{"'new'"}, fv.Values)
}

func TestFieldValue_AddString_EscapesQuotes(t *testing.T) {
	fv := emptyFV()
	fv.AddString("it's", "col")
	require.Equal(t, "'it''s'", fv.Values[0])
}

// --- Bool / AddBool ---

func TestFieldValue_Bool(t *testing.T) {
	fv := emptyFV()
	fv.Bool(true, true, "flag")
	require.Empty(t, fv.Fields)

	fv.Bool(false, true, "flag")
	require.Equal(t, []string{"flag"}, fv.Fields)
	require.Equal(t, []string{"true"}, fv.Values)
}

// --- Integer variants ---

func TestFieldValue_Integers(t *testing.T) {
	tests := []struct {
		name string
		fn   func(*FieldValue)
		want string
	}{
		{"Int8", func(fv *FieldValue) { fv.Int8(1, 2, "c") }, "2"},
		{"Int16", func(fv *FieldValue) { fv.Int16(1, 2, "c") }, "2"},
		{"Int32", func(fv *FieldValue) { fv.Int32(1, 2, "c") }, "2"},
		{"Int64", func(fv *FieldValue) { fv.Int64(1, 2, "c") }, "2"},
		{"UInt8", func(fv *FieldValue) { fv.UInt8(1, 2, "c") }, "2"},
		{"UInt16", func(fv *FieldValue) { fv.UInt16(1, 2, "c") }, "2"},
		{"UInt32", func(fv *FieldValue) { fv.UInt32(1, 2, "c") }, "2"},
		{"UInt64", func(fv *FieldValue) { fv.UInt64(1, 2, "c") }, "2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fv := emptyFV()
			tt.fn(fv)
			require.Equal(t, []string{"c"}, fv.Fields)
			require.Equal(t, []string{tt.want}, fv.Values)
		})
	}
}

func TestFieldValue_Integer_NoChangeSkipped(t *testing.T) {
	fv := emptyFV()
	fv.Int64(5, 5, "col")
	require.Empty(t, fv.Fields)
}

// --- Time / AddTime ---

func TestFieldValue_Time(t *testing.T) {
	t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

	fv := emptyFV()
	fv.Time(t1, t1, "col")
	require.Empty(t, fv.Fields)

	fv.Time(t1, t2, "col")
	require.Equal(t, []string{"col"}, fv.Fields)
	require.Contains(t, fv.Values[0], "2024-06-01")
}

func TestFieldValue_Date(t *testing.T) {
	t1 := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 1, 1, 22, 0, 0, 0, time.UTC) // same date, different time
	t3 := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	fv := emptyFV()
	fv.Date(t1, t2, "col")
	require.Empty(t, fv.Fields, "same date — nothing added")

	fv.Date(t1, t3, "col")
	require.Equal(t, "'2024-01-02'", fv.Values[0])
}

// --- UUID / AddUUID ---

func TestFieldValue_UUID(t *testing.T) {
	u1 := uuid.New()
	u2 := uuid.New()

	fv := emptyFV()
	fv.UUID(u1, u1, "col", false)
	require.Empty(t, fv.Fields)

	fv.UUID(u1, u2, "col", false)
	require.Equal(t, []string{"col"}, fv.Fields)
	require.Equal(t, "'"+u2.String()+"'", fv.Values[0])
}

func TestFieldValue_AddUUID_Nullable(t *testing.T) {
	fv := emptyFV()
	fv.AddUUID(uuid.Nil, "col", true)
	require.Equal(t, "NULL", fv.Values[0])

	fv2 := emptyFV()
	fv2.AddUUID(uuid.Nil, "col", false)
	require.Equal(t, "'"+uuid.Nil.String()+"'", fv2.Values[0])
}

// --- AddJSON ---

func TestFieldValue_AddJSON(t *testing.T) {
	fv := emptyFV()
	data := map[string]int{"a": 1}
	err := fv.AddJSON(data, "meta")
	require.NoError(t, err)
	require.Equal(t, []string{"meta"}, fv.Fields)

	var decoded map[string]int
	// strip wrapping single quotes
	raw := fv.Values[0]
	raw = strings.TrimPrefix(raw, "'")
	raw = strings.TrimSuffix(raw, "'")
	err = json.Unmarshal([]byte(raw), &decoded)
	require.NoError(t, err)
	require.Equal(t, data, decoded)
}

// --- Slice types ---

func TestFieldValue_StringSlice(t *testing.T) {
	fv := emptyFV()
	fv.StringSlice([]string{"a"}, []string{"a"}, "col")
	require.Empty(t, fv.Fields)

	fv.StringSlice([]string{"a"}, []string{"b", "c"}, "col")
	require.Equal(t, "ARRAY['b','c']::VARCHAR[]", fv.Values[0])
}

func TestFieldValue_Int32Slice(t *testing.T) {
	fv := emptyFV()
	fv.Int32Slice([]int32{1}, []int32{2, 3}, "col")
	require.Equal(t, "ARRAY[2,3]::INTEGER[]", fv.Values[0])
}

func TestFieldValue_UUIDSlice(t *testing.T) {
	u := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	fv := emptyFV()
	fv.AddUUIDSlice([]uuid.UUID{u}, "col")
	require.Equal(t, "ARRAY['00000000-0000-0000-0000-000000000001']::UUID[]", fv.Values[0])
}

// --- UpdateQuery / CustomUpdateQuery ---

func TestFieldValue_UpdateQuery_Empty(t *testing.T) {
	fv := emptyFV()
	q, ts := fv.UpdateQuery("users", uint(1))
	require.Empty(t, q)
	require.True(t, ts.IsZero())
}

func TestFieldValue_UpdateQuery_UintID(t *testing.T) {
	fv := emptyFV()
	fv.AddString("alice", "name")

	q, ts := fv.UpdateQuery("users", uint(42))
	require.NotEmpty(t, q)
	require.False(t, ts.IsZero())
	require.Contains(t, q, "UPDATE users SET")
	require.Contains(t, q, "WHERE id = 42")
	require.Contains(t, q, "name")
	require.Contains(t, q, "'alice'")
	require.Contains(t, q, "updated_at")
}

func TestFieldValue_UpdateQuery_UUIDID(t *testing.T) {
	u := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	fv := emptyFV()
	fv.AddBool(true, "active")

	q, _ := fv.UpdateQuery("users", u)
	require.Contains(t, q, "WHERE id = '00000000-0000-0000-0000-000000000001'")
}

func TestFieldValue_CustomUpdateColumn(t *testing.T) {
	fv := &FieldValue{UpdateColumn: "modified_at"}
	fv.AddString("bob", "name")
	q, _ := fv.CustomUpdateQuery("items", "id = 1")
	require.Contains(t, q, "modified_at")
	require.NotContains(t, q, "updated_at")
}

func TestFieldValue_MultipleFields(t *testing.T) {
	fv := emptyFV()
	fv.AddString("alice", "name")
	fv.AddBool(true, "active")
	fv.AddInt64(30, "age")

	q, _ := fv.UpdateQuery("users", uint(1))
	require.Contains(t, q, "name")
	require.Contains(t, q, "active")
	require.Contains(t, q, "age")
}
