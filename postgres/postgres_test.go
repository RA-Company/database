package postgres

import (
	"testing"

	"github.com/ra-company/env"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	ctx := t.Context()
	password := env.GetEnvStr("PG_PWD", "")
	db := env.GetEnvStr("PG_DB", "")
	require.NotEmpty(t, db, "PG_DB environment variable must be set for Postgress tests")
	host := env.GetEnvStr("PG_HOSTS", "")
	require.NotEmpty(t, host, "PG_HOSTS environment variable must be set for Postgress tests")
	user := env.GetEnvStr("PG_USER", "")
	require.NotEmpty(t, user, "PG_USER environment variable must be set for Postgress tests")

	PG.Start(ctx, host, user, password, db)
	PG.client.Close()
}
