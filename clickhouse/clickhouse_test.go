package clickhouse

import (
	"testing"

	"github.com/ra-company/env"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	ctx := t.Context()
	password := env.GetEnvStr("CH_PWD", "")
	db := env.GetEnvStr("CH_DB", "")
	require.NotEmpty(t, db, "CH_DB environment variable must be set for ClickHouse tests")
	host := env.GetEnvStr("CH_HOSTS", "")
	require.NotEmpty(t, host, "CH_HOSTS environment variable must be set for ClickHouse tests")
	user := env.GetEnvStr("CH_USER", "")
	require.NotEmpty(t, user, "CH_USER environment variable must be set for ClickHouse tests")

	CH.Start(ctx, host, user, password, db)

}
