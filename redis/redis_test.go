package redis

import (
	"fmt"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/ra-company/env"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	password := env.GetEnvStr("REDIS_PASSWORD", "")
	db := env.GetEnvInt("REDIS_DB", 0)

	t.Run("1 Single Redis tests", func(t *testing.T) {
		hosts := env.GetEnvStr("REDIS_HOST", "")
		require.NotEmpty(t, hosts, "REDIS_HOST environment variable must be set for Redis tests")
		tests(t, hosts, password, db)
	})
	t.Run("2 Redis Cluster tests", func(t *testing.T) {
		hosts := env.GetEnvStr("REDIS_CLUSTER", "")
		require.NotEmpty(t, hosts, "REDIS_CLUSTER environment variable must be set for Redis cluster tests")
		tests(t, hosts, password, db)
	})
}

func tests(t *testing.T, hosts, password string, db int) {
	ctx := t.Context()
	faker := gofakeit.New(0)

	key := faker.Word()
	value := faker.LetterN(30)
	defaultValue := faker.LetterN(30)

	Redis.Start(ctx, hosts, password, db)

	t.Run("1 Get()", func(t *testing.T) {
		if Redis.client == nil {
			Redis.cluster.Del(ctx, key)
		} else {
			Redis.client.Del(ctx, key)
		}
		got, err := Redis.Get(ctx, key, defaultValue)
		require.NoError(t, err, "Get()")
		require.Equal(t, defaultValue, got, "Get()")

		if Redis.client == nil {
			Redis.cluster.Set(ctx, key, value, time.Duration(10)*time.Second)
			defer Redis.cluster.Del(ctx, key)
		} else {
			Redis.client.Set(ctx, key, value, time.Duration(10)*time.Second)
			defer Redis.client.Del(ctx, key)
		}

		got, err = Redis.Get(ctx, key, defaultValue)
		require.NoError(t, err, "Get()")
		require.Equal(t, value, got, "Get()")
	})

	t.Run("2 Set()", func(t *testing.T) {
		var got string
		err := Redis.Set(ctx, key, value, 10)
		require.NoError(t, err, "Set()")
		if Redis.client == nil {
			defer Redis.cluster.Del(ctx, key)
			got, err = Redis.cluster.Get(ctx, key).Result()
		} else {
			defer Redis.client.Del(ctx, key)
			got, err = Redis.client.Get(ctx, key).Result()
		}
		require.NoError(t, err, "redis.Get()")
		require.Equal(t, value, got, "redis.Get()")
	})

	t.Run("3 Set with timer", func(t *testing.T) {
		var got string
		err := Redis.Set(ctx, key, value, 1)
		require.NoError(t, err, "Set()")
		if Redis.client == nil {
			defer Redis.cluster.Del(ctx, key)
			got, err = Redis.cluster.Get(ctx, key).Result()
		} else {
			defer Redis.client.Del(ctx, key)
			got, err = Redis.client.Get(ctx, key).Result()
		}

		require.NoError(t, err, "redis.Get()")
		require.Equal(t, value, got, "redis.Get()")

		time.Sleep(time.Duration(2) * time.Second)
		if Redis.client == nil {
			_, err = Redis.cluster.Get(ctx, key).Result()
		} else {
			_, err = Redis.client.Get(ctx, key).Result()
		}

		require.Error(t, err, "redis.Get()")
		require.Equal(t, err.Error(), "redis: nil", "redis.Get()")
	})

	t.Run("4 Keys()", func(t *testing.T) {
		pattern := faker.Word()
		key1 := fmt.Sprintf("%s:%s", pattern, faker.Word())
		key2 := fmt.Sprintf("%s:%s", pattern, faker.Word())

		if Redis.client == nil {
			t.Skip("Keys() not supported in Redis cluster mode")
		}

		Redis.client.Set(ctx, key1, faker.Word(), time.Duration(10)*time.Second)
		defer Redis.client.Del(ctx, key1)
		Redis.client.Set(ctx, key2, faker.Word(), time.Duration(10)*time.Second)
		defer Redis.client.Del(ctx, key2)

		keys, err := Redis.Keys(ctx, fmt.Sprintf("%s:*", pattern))
		require.NoError(t, err, "Keys()")
		require.Contains(t, keys, key1)
		require.Contains(t, keys, key2)
		require.Equal(t, int(2), len(keys))
	})

	t.Run("5 Del()", func(t *testing.T) {
		err := Redis.Del(ctx, key)
		require.NoError(t, err, "Del()")

		if Redis.client == nil {
			Redis.cluster.Set(ctx, key, value, time.Duration(10)*time.Second)
			defer Redis.cluster.Del(ctx, key)
		} else {
			Redis.client.Set(ctx, key, value, time.Duration(10)*time.Second)
			defer Redis.client.Del(ctx, key)
		}

		err = Redis.Del(ctx, key)
		require.NoError(t, err, "Del()")

		if Redis.client == nil {
			_, err = Redis.cluster.Get(ctx, key).Result()
		} else {
			_, err = Redis.client.Get(ctx, key).Result()
		}

		require.Error(t, err, "redis.Get()")
		require.Equal(t, err.Error(), "redis: nil", "redis.Get()")
	})

	t.Run("6 SinglePush()", func(t *testing.T) {
		key := faker.Word()
		value := faker.Word()

		err := Redis.SinglePush(ctx, key, value)
		require.NoError(t, err, "SinglePush()")

		if Redis.client == nil {
			defer Redis.cluster.Del(ctx, key)
		} else {
			defer Redis.client.Del(ctx, key)
		}

		err = Redis.SinglePush(ctx, key, value)
		require.ErrorIs(t, err, ErrorListIsNotEmpty, "SinglePush()")
	})

	t.Run("7 MultiSet()", func(t *testing.T) {
		sets := []Set{
			{Key: fmt.Sprintf("{test}:%s", faker.Word()), Value: faker.Word(), TTL: 10},
			{Key: fmt.Sprintf("{test}:%s", faker.Word()), Value: faker.Word(), TTL: 10},
			{Key: fmt.Sprintf("{test}:%s", faker.Word()), Value: []byte(faker.Word()), TTL: 10},
		}

		err := Redis.MultiSet(ctx, &sets)
		require.NoError(t, err, "MultiSet()")

		for _, set := range sets {
			var got string
			if Redis.client == nil {
				got, err = Redis.cluster.Get(ctx, set.Key).Result()
				defer Redis.cluster.Del(ctx, set.Key)
			} else {
				got, err = Redis.client.Get(ctx, set.Key).Result()
				defer Redis.client.Del(ctx, set.Key)
			}
			require.NoError(t, err, "redis.Get()")
			val := fmt.Sprintf("%s", set.Value)
			require.Equal(t, val, got, "redis.Get()")
		}
	})
}
