package postgres

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ra-company/logging"
)

type FieldValue struct {
	Fields []string
	Values []string
	Update time.Time
}

func (dst *FieldValue) String(was, is, field string) {
	if was != is {
		dst.AddString(is, field)
	}
}

func (dst *FieldValue) AddString(is, field string) {
	dst.Fields = append(dst.Fields, field)
	dst.Values = append(dst.Values, fmt.Sprintf("'%s'", ToStr(is)))
}

func (dst *FieldValue) UInt8(was, is uint8, field string) {
	if was != is {
		dst.AddInt64(int64(is), field)
	}
}

func (dst *FieldValue) Int8(was, is int8, field string) {
	if was != is {
		dst.AddInt64(int64(is), field)
	}
}

func (dst *FieldValue) Int16(was, is int16, field string) {
	if was != is {
		dst.AddInt64(int64(is), field)
	}
}

func (dst *FieldValue) Int32(was, is int32, field string) {
	if was != is {
		dst.AddInt64(int64(is), field)
	}
}

func (dst *FieldValue) Int64(was, is int64, field string) {
	if was != is {
		dst.AddInt64(is, field)
	}
}

func (dst *FieldValue) AddInt64(is int64, field string) {
	dst.Fields = append(dst.Fields, field)
	dst.Values = append(dst.Values, fmt.Sprintf("%d", is))
}

func (dst *FieldValue) Bool(was, is bool, field string) {
	if was != is {
		dst.AddBool(is, field)
	}
}

func (dst *FieldValue) AddBool(is bool, field string) {
	dst.Fields = append(dst.Fields, field)
	dst.Values = append(dst.Values, fmt.Sprintf("%t", is))
}

func (dst *FieldValue) Time(was, is time.Time, field string) {
	if was != is {
		dst.AddTime(is, field)
	}
}

func (dst *FieldValue) AddTime(is time.Time, field string) {
	dst.Fields = append(dst.Fields, field)
	dst.Values = append(dst.Values, fmt.Sprintf("'%s'", is.UTC().Format(time.RFC3339Nano)))
}

func (dst *FieldValue) AddJSON(is interface{}, field string) {
	data, err := json.Marshal(is)
	if err != nil {
		logging.Logs.Errorf("json.Marshal() error: %+v", err)
		return
	}
	dst.Fields = append(dst.Fields, field)
	dst.Values = append(dst.Values, fmt.Sprintf("'%s'", ToStr(string(data))))
}

func (dst *FieldValue) Int64Slice(was, is []int64, field string) {
	if !slices.Equal(was, is) {
		dst.AddInt64Slice(is, field)
	}
}

func (dst *FieldValue) AddInt64Slice(is []int64, field string) {
	dst.Fields = append(dst.Fields, field)
	dst.Values = append(dst.Values, fmt.Sprintf("ARRAY%s::BIGINT[]", ArrayToString(is)))
}

func (dst *FieldValue) StringSlice(was, is []string, field string) {
	if !slices.Equal(was, is) {
		dst.AddStringSlice(is, field)
	}
}

func (dst *FieldValue) AddStringSlice(is []string, field string) {
	dst.Fields = append(dst.Fields, field)
	dst.Values = append(dst.Values, fmt.Sprintf("ARRAY%s::VARCHAR[]", ArrayToString(is)))
}

func (dst *FieldValue) IsChanged() bool {
	if len(dst.Fields) == 0 {
		return false
	}

	dst.Update = time.Now().UTC()
	dst.Fields = append(dst.Fields, "updated_at")
	dst.Values = append(dst.Values, fmt.Sprintf("'%s'", dst.Update.Format(time.RFC3339Nano)))
	return true
}

func (dst *FieldValue) UpdateQuery(table string, id int64) (string, time.Time) {
	if !dst.IsChanged() {
		return "", time.Time{}
	}

	return fmt.Sprintf("UPDATE %s SET (%s) = (%s) WHERE id = %d", table, strings.Join(dst.Fields, ","), strings.Join(dst.Values, ","), id), dst.Update
}
