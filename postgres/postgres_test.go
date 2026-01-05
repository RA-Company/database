package postgres

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/brianvoe/gofakeit/v7"
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

	query := `CREATE TABLE IF NOT EXISTS test_table (
		id SERIAL PRIMARY KEY,
		name TEXT NOT NULL,
		settings JSON NOT NULL);`
	_, err := PG.Client().Exec(ctx, query)
	require.NoError(t, err, "failed to create test_table")

	defer func() {
		_, err := PG.Client().Exec(ctx, `DROP TABLE IF EXISTS test_table;`)
		require.NoError(t, err, "failed to drop test_table")
		PG.Stop(ctx)
	}()

	type testStruct struct {
		Age     int    `json:"age" db:"age"`
		City    string `json:"city" db:"city"`
		Country string `json:"country" db:"country"`
	}

	type testModel struct {
		ID       int        `json:"id" db:"id"`
		Name     string     `json:"name" db:"name"`
		Settings testStruct `json:"settings" db:"settings"`
	}

	faker := gofakeit.New(0)

	Model := testModel{}

	err = faker.Struct(&Model)
	require.NoError(t, err, "failed to generate fake data for testModel")

	js, err := json.Marshal(Model.Settings)
	require.NoError(t, err, "failed to marshal settings to JSON")

	insertQuery := fmt.Sprintf(`INSERT INTO test_table (name, settings) VALUES ('%s', '%s')`, Model.Name, string(js))
	ids, err := PG.Insert(ctx, "Model", insertQuery)
	require.NoError(t, err, "failed to insert data into test_table")
	require.NotZero(t, len(ids), "inserted model ID should not be zero")
	Model.ID = int(ids[0])

	var fetchedModel []testModel
	selectQuery := fmt.Sprintf(`SELECT id, name, settings FROM test_table WHERE id = %d;`, Model.ID)
	err = PG.Select(ctx, "Model", selectQuery, &fetchedModel)
	require.NoError(t, err, "failed to fetch data from test_table")

	require.Equal(t, Model.ID, fetchedModel[0].ID, "fetched model ID does not match inserted model ID")
	require.Equal(t, Model.Name, fetchedModel[0].Name, "fetched model Name does not match inserted model Name")
	require.Equal(t, Model.Settings.Age, fetchedModel[0].Settings.Age, "fetched model Settings do not match inserted model Settings")
	require.Equal(t, Model.Settings.City, fetchedModel[0].Settings.City, "fetched model Settings do not match inserted model Settings")
	require.Equal(t, Model.Settings.Country, fetchedModel[0].Settings.Country, "fetched model Settings do not match inserted model Settings")
}
