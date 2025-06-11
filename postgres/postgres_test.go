package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArrayToString(t *testing.T) {
	tests := []struct {
		name string
		args any
		want string
	}{
		{
			name: "nil",
			args: nil,
			want: "[]",
		},
		{
			name: "empty slice",
			args: []string{},
			want: "[]",
		},
		{
			name: "int slice",
			args: []int{1, 2, 3},
			want: "[1,2,3]",
		},
		{
			name: "string slice",
			args: []string{"a", "b", "c"},
			want: "['a','b','c']",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ArrayToString(tt.args)
			require.Equal(t, tt.want, got, "ArrayToString()")
		})
	}
}
