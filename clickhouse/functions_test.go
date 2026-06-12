package clickhouse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimeToString(t *testing.T) {
	locPlus5 := time.FixedZone("UTC+5", 5*60*60)

	tests := []struct {
		name  string
		input time.Time
		want  string
	}{
		{
			name:  "UTC time with microseconds",
			input: time.Date(2024, 3, 15, 12, 30, 45, 123456000, time.UTC),
			want:  "toDateTime64('2024-03-15 12:30:45.123456', 6, 'UTC')",
		},
		{
			name:  "non-UTC time converted to UTC",
			input: time.Date(2024, 3, 15, 15, 0, 0, 0, locPlus5),
			want:  "toDateTime64('2024-03-15 10:00:00', 6, 'UTC')",
		},
		{
			name:  "zero time",
			input: time.Time{},
			want:  "toDateTime64('0001-01-01 00:00:00', 6, 'UTC')",
		},
		{
			name:  "nanosecond precision preserved in format",
			input: time.Date(2024, 1, 1, 0, 0, 0, 999999999, time.UTC),
			want:  "toDateTime64('2024-01-01 00:00:00.999999999', 6, 'UTC')",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TimeToString(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestTimeToString32(t *testing.T) {
	locPlus5 := time.FixedZone("UTC+5", 5*60*60)

	tests := []struct {
		name  string
		input time.Time
		want  string
	}{
		{
			name:  "UTC time",
			input: time.Date(2024, 3, 15, 12, 30, 45, 0, time.UTC),
			want:  "2024-03-15 12:30:45",
		},
		{
			name:  "non-UTC time converted to UTC",
			input: time.Date(2024, 3, 15, 15, 0, 0, 0, locPlus5),
			want:  "2024-03-15 10:00:00",
		},
		{
			name:  "subseconds stripped",
			input: time.Date(2024, 6, 1, 8, 0, 0, 999999999, time.UTC),
			want:  "2024-06-01 08:00:00",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TimeToString32(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}
