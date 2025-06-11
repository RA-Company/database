package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/ra-company/env"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	host := env.GetEnvStr("REDIS_HOST", "localhost")
	port := env.GetEnvInt("REDIS_PORT", 6379)
	password := env.GetEnvStr("REDIS_PASSWORD", "")
	db := env.GetEnvInt("REDIS_DB", 0)
	Start(host, port, password, db)

	faker := gofakeit.New(0)
	ctx := context.Background()

	key := faker.Word()
	value := faker.LetterN(30)
	defaultValue := faker.LetterN(30)

	t.Run("1 Get()", func(t *testing.T) {
		Redis.Del(ctx, key)
		got, err := Get(ctx, key, defaultValue)
		require.NoError(t, err, "Get()")
		require.Equal(t, defaultValue, got, "Get()")

		Redis.Set(ctx, key, value, time.Duration(10)*time.Second)
		defer Redis.Del(ctx, key)

		got, err = Get(ctx, key, defaultValue)
		require.NoError(t, err, "Get()")
		require.Equal(t, value, got, "Get()")
	})

	t.Run("2 Set()", func(t *testing.T) {
		err := Set(ctx, key, value, 10)
		require.NoError(t, err, "Set()")
		defer Redis.Del(ctx, key)

		got, err := Redis.Get(ctx, key).Result()
		require.NoError(t, err, "redis.Get()")
		require.Equal(t, value, got, "redis.Get()")
	})

	t.Run("3 Set with timer", func(t *testing.T) {
		err := Set(ctx, key, value, 1)
		require.NoError(t, err, "Set()")
		defer Redis.Del(ctx, key)

		got, err := Redis.Get(ctx, key).Result()
		require.NoError(t, err, "redis.Get()")
		require.Equal(t, value, got, "redis.Get()")

		time.Sleep(time.Duration(2) * time.Second)
		_, err = Redis.Get(ctx, key).Result()
		require.Error(t, err, "redis.Get()")
		require.Equal(t, err.Error(), "redis: nil", "redis.Get()")
	})

	t.Run("4 Keys()", func(t *testing.T) {
		pattern := faker.Word()
		key1 := fmt.Sprintf("%s:%s", pattern, faker.Word())
		key2 := fmt.Sprintf("%s:%s", pattern, faker.Word())

		Redis.Set(ctx, key1, faker.Word(), time.Duration(10)*time.Second)
		defer Redis.Del(ctx, key1)
		Redis.Set(ctx, key2, faker.Word(), time.Duration(10)*time.Second)
		defer Redis.Del(ctx, key2)

		keys, err := Keys(ctx, fmt.Sprintf("%s:*", pattern))
		require.NoError(t, err, "Keys()")
		require.Contains(t, keys, key1)
		require.Contains(t, keys, key2)
		require.Equal(t, int(2), len(keys))
	})

	t.Run("5 Del()", func(t *testing.T) {
		err := Del(ctx, key)
		require.NoError(t, err, "Del()")

		Redis.Set(ctx, key, value, time.Duration(10)*time.Second)
		defer Redis.Del(ctx, key)

		err = Del(ctx, key)
		require.NoError(t, err, "Del()")

		_, err = Redis.Get(ctx, key).Result()
		require.Error(t, err, "redis.Get()")
		require.Equal(t, err.Error(), "redis: nil", "redis.Get()")
	})

	t.Run("6 SinglePush()", func(t *testing.T) {
		key := faker.Word()
		value := faker.Word()

		err := SinglePush(ctx, key, value)
		require.NoError(t, err, "SinglePush()")

		err = SinglePush(ctx, key, value)
		require.ErrorIs(t, err, ErrorListIsNotEmpty, "SinglePush()")
	})
}
