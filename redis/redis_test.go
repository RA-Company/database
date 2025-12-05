package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	"github.com/ra-company/env"
	"github.com/ra-company/logging"
	"github.com/redis/go-redis/v9"
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

	t.Run("2 Set() parallel", func(t *testing.T) {
		for i := range 10 {
			t.Run(fmt.Sprintf("Set parallel %d", i), func(t *testing.T) {
				t.Parallel()
				key := faker.Word()
				new_ctx := context.WithValue(ctx, logging.CtxKeyUUID, uuid.New().String())
				var got string
				err := Redis.Set(new_ctx, key, value, 10)
				require.NoError(t, err, "Set()")
				if Redis.client == nil {
					defer Redis.cluster.Del(new_ctx, key)
					got, err = Redis.cluster.Get(new_ctx, key).Result()
				} else {
					defer Redis.client.Del(new_ctx, key)
					got, err = Redis.client.Get(new_ctx, key).Result()
				}
				require.NoError(t, err, "redis.Get()")
				require.Equal(t, value, got, "redis.Get()")
			})
		}
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

		if Redis.client == nil {
			Redis.cluster.Del(ctx, key).Result()
		} else {
			Redis.client.Del(ctx, key).Result()
		}

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

	t.Run("8 Streams", func(t *testing.T) {
		stream := faker.Word()
		group := faker.Word()

		err := Redis.XGroupCreateMkStream(ctx, stream, group, "$")
		require.NoError(t, err, "XGroupCreateMkStream()")

		defer Redis.XGroupDestroy(ctx, stream, group)

		err = Redis.XGroupCreateMkStream(ctx, stream, group, "$")
		require.ErrorIs(t, err, ErrorGroupAlreadyExists, "XGroupCreateMkStream()")

		field1 := faker.Word()
		field2 := faker.Word()
		add := redis.XAddArgs{
			Stream: stream,
			Values: map[string]any{
				"field1": field1,
				"field2": field2,
			},
			NoMkStream: true,
		}

		str, err := Redis.XAdd(ctx, &add)
		require.NoError(t, err, "XAdd()")
		require.NotEmpty(t, str, "XAdd()")

		readArgs := redis.XReadGroupArgs{
			Group:    group,
			Consumer: "test-consumer",
			Streams:  []string{stream, ">"},
			Count:    1,
			Block:    500 * time.Millisecond,
		}
		data, err := Redis.XReadGroup(ctx, &readArgs)
		require.NoError(t, err, "XReadGroup()")
		require.Len(t, data, 1, "XReadGroup()")

		data, err = Redis.XReadGroup(ctx, &readArgs)
		require.NoError(t, err, "XReadGroup()")
		require.Nil(t, data, "XReadGroup()")

		claimArgs := redis.XAutoClaimArgs{
			Stream:   stream,
			Group:    group,
			Consumer: "test-consumer",
			MinIdle:  1 * time.Millisecond,
			Count:    1,
			Start:    "0-0",
		}
		claimed, err := Redis.XAutoClaim(ctx, &claimArgs)
		require.NoError(t, err, "XAutoClaim()")
		require.Len(t, claimed, 1, "XAutoClaim()")
		require.Equal(t, claimed[0].ID, str, "XAutoClaim() message ID")

		ackCount, err := Redis.XAck(ctx, stream, group, str)
		require.NoError(t, err, "XAck()")
		require.Equal(t, int64(1), ackCount, "XAck()")

		claimed, err = Redis.XAutoClaim(ctx, &claimArgs)
		require.NoError(t, err, "XAutoClaim() after ack")
		require.Len(t, claimed, 0, "XAutoClaim() after ack should return no messages")

		ackCount, err = Redis.XAck(ctx, stream, group, str)
		require.NoError(t, err, "XAck()")
		require.Equal(t, int64(0), ackCount, "XAck()")

		add.Stream = stream + "-1"
		str, err = Redis.XAdd(ctx, &add)
		require.Error(t, err, "XAdd() with different stream")
		require.Empty(t, str, "XAdd() with different stream")
	})
}
