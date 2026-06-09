package clickhouse

import (
	"testing"

	"github.com/ra-company/env"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	ctx := t.Context()

	config := Config{
		Hosts:    env.GetEnvStr("CH_HOSTS", ""),
		User:     env.GetEnvStr("CH_USER", ""),
		Password: env.GetEnvStr("CH_PWD", ""),
		DB:       env.GetEnvStr("CH_DB", ""),
	}
	require.NotEmpty(t, config.Hosts, "CH_HOSTS environment variable must be set for ClickHouse tests")
	require.NotEmpty(t, config.User, "CH_USER environment variable must be set for ClickHouse tests")
	require.NotEmpty(t, config.DB, "CH_DB environment variable must be set for ClickHouse tests")

	CH := ClickHouseClient{}

	CH.Start(ctx, &config)
}
