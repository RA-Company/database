package clickhouse

import (
	"fmt"
	"strings"
	"time"
)

func TimeToString(t time.Time) string {
	return fmt.Sprintf("toDateTime64('%s', 6, 'UTC')", strings.Replace(strings.Replace(t.UTC().Format(time.RFC3339Nano), "Z", "", -1), "T", " ", -1))
}

func TimeToString32(t time.Time) string {
	return strings.Replace(strings.Replace(t.UTC().Format(time.RFC3339), "Z", "", -1), "T", " ", -1)
}
