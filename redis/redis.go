package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ra-company/logging"

	"github.com/redis/go-redis/v9"
)

// Set represents a Redis set operation with a key, value, and time-to-live (TTL).
// It is used to add a value to a Redis set with an optional expiration time.
// The Key field is the Redis key for the set, the Value field is the value to be added to the set, and the TTL field is the time-to-live for the set in seconds.
type Set struct {
	Key   string // Key: is the Redis key for the set.
	Value any    // Value: is the value to be added to the set.
	TTL   uint64 // TTL: is the time-to-live for the set in seconds.
}

type RedisClient struct {
	logging.CustomLogger                      // CustomLogger: is an interface that allows the Redis client to use a custom logger for logging operations and errors.
	client               *redis.Client        // client: is the Redis client used to interact with the Redis server.
	cluster              *redis.ClusterClient // cluster: is a Redis cluster client used for connecting to a Redis cluster.
	singlePush           *redis.Script        // singlePush: is a Lua script used for atomic operations on Redis lists, specifically for pushing a value to a list only if the list is empty.
	db                   int                  // db: is the Redis database number, used for logging purposes.
	DoNotLogQueries      bool                 // DoNotLogQueries: is a flag that indicates whether to log Redis queries or not. If true, queries will not be logged, which can be useful for performance or security reasons.
	lastQuery            string               // LastQuery: is the last executed Redis query, used for debugging and logging purposes.
}

var (
	Redis RedisClient // Redis: is the global Redis client used to interact with the Redis server.

	ErrorListIsNotEmpty     = errors.New("list is not empty")
	ErrorGroupAlreadyExists = errors.New("group already exists")
)

// Start initializes the Redis client with the provided host, port, password, and database number.
// It connects to the Redis server and checks the connection by sending a PING command.
// If the connection fails, it logs a fatal error and exits the program.
//
// Parameters:
//   - ctx: The context for the operation, allowing for cancellation and timeouts.
//   - hosts: The Redis server hosts (single or comma-separated).
//   - password: The Redis server password.
//   - db: The Redis database number to use.
//
// This function also initializes a Lua script for single push operations on Redis lists.
// It checks if the Redis list is empty before pushing a new value to it.
// If the list is not empty, it returns an error indicating that the list is not empty.
// This function should be called at the start of the application to establish a connection to Redis.
// It sets the usedDB variable to the current database number for logging purposes.
func (dst *RedisClient) Start(ctx context.Context, hosts string, password string, db int) {
	if strings.Contains(hosts, ",") {
		// If the hosts contain a comma, it is a cluster of Redis nodes.
		dst.startCluster(ctx, hosts, password)
	} else {
		// If the hosts do not contain a comma, it is a single Redis node.
		dst.startSingle(ctx, hosts, password, db)
	}

	dst.singlePush = redis.NewScript(`
        if redis.call("LLEN", KEYS[1]) == 0 then
            return redis.call("LPUSH", KEYS[1], ARGV[1])
        else
            return -1
        end
    `)
}

func (dst *RedisClient) startSingle(ctx context.Context, host string, password string, db int) {
	dst.db = db // Set the usedDB variable to the current database number.

	dst.client = redis.NewClient(&redis.Options{
		Addr:     host,
		Password: password,
		DB:       dst.db,
	})
	res := dst.client.Ping(ctx)
	if res.Err() != nil {
		dst.Fatal(ctx, "Failed to connect to Redis database: %v", res.Err())
	}
	dst.Info(ctx, "Connected to Redis database: redis://%v/%v", host, dst.db)
	dst.cluster = nil // Ensure cluster is nil for single instance.
}

func (dst *RedisClient) startCluster(ctx context.Context, hosts string, password string) {
	dst.db = 0 // Set the usedDB variable to 0 for cluster, as clusters do not use a specific database number.

	dst.cluster = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    strings.Split(hosts, ","),
		Password: password,
	})
	res := dst.cluster.Ping(ctx)
	if res.Err() != nil {
		dst.Fatal(ctx, "Failed to connect to Redis cluster: %v", res.Err())
	}
	dst.Info(ctx, "Connected to Redis cluster: redis://%v", hosts)
	dst.client = nil // Ensure client is nil for cluster.
}

// Get returns value from Redis database by key. If key is not set, returns default value.
// If an error occurs, it returns an empty string and the error.
// If the key does not exist, it returns the default value without an error.
// If the key exists, it returns the value associated with the key.
//
// Parameters:
//   - ctx: The context for the operation.
//   - key: The key in Redis database.
//   - def: The default value to return if the key is not found.
//
// Returns:
//   - The value associated with the key, or the default value if the key is not found.
//   - An error if the operation fails.
func (dst *RedisClient) Get(ctx context.Context, key string, def string) (string, error) {
	start := time.Now()

	var str string
	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to get the value.
		str, err = dst.cluster.Get(ctx, key).Result()
	} else {
		// If using a single Redis instance, use the client to get the value.
		str, err = dst.client.Get(ctx, key).Result()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) GET (%.2f ms)\033[1m \033[34m%q\033[0m", dst.db, float64(time.Since(start))/1000000, key)
	if err != nil {
		if err == redis.Nil {
			return def, nil
		}
		return "", err
	}
	return str, nil
}

// LPos returns first position of value from Redis list by key.
// If the value is not found, it returns -1 without an error.
// If an error occurs, it returns -1 and the error.
//
// Parameters:
//   - ctx: The context for the operation.
//   - key: The key in Redis database.
//   - value: The value to search for in the list.
//
// Returns:
//   - The position of the value in the list, or -1 if the value is not found.
//   - An error if the operation fails.
func (dst *RedisClient) LPos(ctx context.Context, key string, value string) (int, error) {
	start := time.Now()

	var str int64
	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to find the position.
		str, err = dst.cluster.LPos(ctx, key, value, redis.LPosArgs{}).Result()
	} else {
		// If using a single Redis instance, use the client to find the position.
		str, err = dst.client.LPos(ctx, key, value, redis.LPosArgs{}).Result()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) LPOS (%.2f ms)\033[1m \033[34m%q\033[0m", dst.db, float64(time.Since(start))/1000000, key)
	if err != nil {
		if err == redis.Nil {
			return -1, nil
		}
		return -1, err
	}
	return int(str), nil
}

// MGet returns value from Redis database by couple of keys. If key is not set, returns nil.
// If an error occurs, it returns an empty slice and the error.
// If the keys do not exist, it returns a slice of empty strings without an error.
// If the keys exist, it returns a slice of values associated with the keys.
//
// Parameters:
//   - ctx: The context for the operation.
//   - keys: A slice of keys in Redis database.
//
// Returns:
//   - A slice of strings containing the values associated with the keys, or an empty slice if the keys are not found.
//   - An error if the operation fails.
func (dst *RedisClient) MGet(ctx context.Context, keys []string) ([]string, error) {
	start := time.Now()

	results := make([]string, len(keys))
	var strs []any
	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to get multiple values.
		strs, err = dst.cluster.MGet(ctx, keys...).Result()
	} else {
		// If using a single Redis instance, use the client to get multiple values.
		strs, err = dst.client.MGet(ctx, keys...).Result()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) MGET (%.2f ms)\033[1m \033[34m%q\033[0m", dst.db, float64(time.Since(start))/1000000, strings.Join(keys, ", "))
	if err != nil {
		return results, err
	}
	for i, str := range strs {
		if str != nil {
			results[i] = str.(string)
		}
	}
	return results, nil
}

// Set value into Redis database by key with expiration time in seconds.
// If an error occurs, it returns the error.
// If the operation is successful, it returns nil.
// This function sets a value in the Redis database with a specified expiration time.
// It uses the Redis SET command to store the value associated with the key.
// The expiration time is specified in seconds.
//
// Parameters:
//   - ctx: The context for the operation.
//   - key: The key in Redis database.
//   - value: The value to set in Redis database.
//   - expiration: The expiration time in seconds.
//
// Returns:
//   - An error if the operation fails, otherwise nil.
func (dst *RedisClient) Set(ctx context.Context, key string, value any, expiration int) error {
	start := time.Now()

	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to set the value.
		err = dst.cluster.Set(ctx, key, value, time.Duration(expiration)*time.Second).Err()
	} else {
		// If using a single Redis instance, use the client to set the value.
		err = dst.client.Set(ctx, key, value, time.Duration(expiration)*time.Second).Err()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) SET (%.2f ms)\033[1m \033[33m%q=%q\033[0m", dst.db, float64(time.Since(start))/1000000, key, strings.ReplaceAll(fmt.Sprintf("%s", value), "\n", " "))
	return err
}

// MultiSet sets multiple key-value pairs in Redis database with optional expiration times.
// It uses a transaction pipeline to execute multiple SET commands atomically.
// If an error occurs, it returns the error.
// If the operation is successful, it returns nil.
//
// Parameters:
//   - ctx: The context for the operation.
//   - sets: A slice of Set structs containing the keys, values, and optional expiration times.
//
// Returns:
//   - An error if the operation fails, otherwise nil.
func (dst *RedisClient) MultiSet(ctx context.Context, sets *[]Set) error {
	start := time.Now()
	vals := []string{}
	var pipe redis.Pipeliner
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to create a transaction pipeline.
		pipe = dst.cluster.TxPipeline()
	} else {
		// If using a single Redis instance, use the client to create a transaction pipeline.
		pipe = dst.client.TxPipeline()
	}
	for _, set := range *sets {
		if set.TTL > 0 {
			pipe.Set(ctx, set.Key, set.Value, time.Duration(set.TTL)*time.Second)
		} else {
			pipe.Set(ctx, set.Key, set.Value, 0)
		}
		vals = append(vals, fmt.Sprintf("%q=%q", set.Key, strings.ReplaceAll(fmt.Sprintf("%s", set.Value), "\n", " ")))
	}
	_, err := pipe.Exec(ctx)
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) MULTISET (%.2f ms)\033[1m \033[33m%s\033[0m", dst.db, float64(time.Since(start))/1000000, strings.Join(vals, ", "))
	if err != nil {
		return err
	}
	return nil
}

// LPush adds a value to the beginning of a Redis list by key.
// If the operation is successful, it returns the new length of the list.
// If an error occurs, it returns the error.
// This function uses the Redis LPUSH command to add a value to the list.
//
// Parameters:
//   - ctx: The context for the operation.
//   - key: The key in Redis database.
//   - value: The value to add to the list.
//
// Returns:
//   - The new length of the list after the operation.
//   - An error if the operation fails.
func (dst *RedisClient) LPush(ctx context.Context, key string, value any) (int64, error) {
	start := time.Now()

	var res int64
	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to push the value.
		res, err = dst.cluster.LPush(ctx, key, value).Result()
	} else {
		// If using a single Redis instance, use the client to push the value.
		res, err = dst.client.LPush(ctx, key, value).Result()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) LPUSH(%d) (%.2f ms)\033[1m \033[33m%q=%q\033[0m\033[0m", dst.db, res, float64(time.Since(start))/1000000, key, value)
	return res, err
}

// SinglePush adds a value to the beginning of a Redis list by key only if the list is empty.
// If the list is not empty, it returns an error indicating that the list is not empty.
// This function uses a Lua script to ensure atomicity of the operation.
// If the operation is successful, it returns nil.
//
// Parameters:
//   - ctx: The context for the operation.
//   - key: The key in Redis database.
//   - value: The value to add to the list.
//
// Returns:
//   - An error if the list is not empty or if the operation fails, otherwise nil.
func (dst *RedisClient) SinglePush(ctx context.Context, key string, value any) error {
	start := time.Now()

	var res any
	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to run the single push script
		res, err = dst.singlePush.Run(ctx, dst.cluster, []string{key}, value).Result()
	} else {
		// If using a single Redis instance, use the client to run the single push script
		res, err = dst.singlePush.Run(ctx, dst.client, []string{key}, value).Result()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) SINGLEPUSH (%.2f ms)\033[1m \033[34m%q=%q\033[0m", dst.db, float64(time.Since(start))/1000000, key, value)
	if err != nil {
		return err
	}
	if res.(int64) == -1 {
		return ErrorListIsNotEmpty
	}
	return nil
}

// Get array of values from Redis list by key.
// It retrieves all elements from the list stored at the specified key.
// If the list is empty, it returns an empty slice without an error.
// If an error occurs, it returns an empty slice and the error.
//
// Parameters:
//   - ctx: The context for the operation.
//   - key: The key in Redis database.
//   - def: The default value to return if the list is empty.
//
// Returns:
//   - A slice of strings containing the values from the list, or an empty slice if the list is empty.
//   - An error if the operation fails.
func (dst *RedisClient) LRange(ctx context.Context, key string, def string) ([]string, error) {
	start := time.Now()

	var res []string
	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to get the range of values.
		res, err = dst.cluster.LRange(ctx, key, 0, -1).Result()
	} else {
		// If using a single Redis instance, use the client to get the range of values.
		res, err = dst.client.LRange(ctx, key, 0, -1).Result()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) LRANGE (%.2f ms)\033[1m \033[34m%q\033[0m", dst.db, float64(time.Since(start))/1000000, key)

	return res, err
}

// LLen returns the length of a Redis list by key.
// It retrieves the number of elements in the list stored at the specified key.
// If the list is empty, it returns 0 without an error.
// If an error occurs, it returns 0 and the error.
//
// Parameters:
//   - ctx: The context for the operation.
//   - key: The key in Redis database.
//
// Returns:
//   - The length of the list, or 0 if the list is empty.
//   - An error if the operation fails.
func (dst *RedisClient) LLen(ctx context.Context, key string) (int64, error) {
	start := time.Now()

	var res int64
	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to get the length of the list.
		res, err = dst.cluster.LLen(ctx, key).Result()
	} else {
		// If using a single Redis instance, use the client to get the length of the list.
		res, err = dst.client.LLen(ctx, key).Result()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) LLEN (%.2f ms)\033[1m \033[34m%q\033[0m", dst.db, float64(time.Since(start))/1000000, key)

	return res, err
}

// LRem removes a specified number of occurrences of a value from a Redis list by key.
// It removes the first count occurrences of value from the list stored at the specified key.
// If count is positive, it removes from the head of the list.
// If count is negative, it removes from the tail of the list.
// If the list is empty, it does nothing and returns nil.
// If an error occurs, it returns the error.
//
// Parameters:
//   - ctx: The context for the operation.
//   - key: The key in Redis database.
//   - count: The number of occurrences to remove. If count is 0, it removes all occurrences.
//   - value: The value to remove from the list.
//
// Returns:
//   - An error if the operation fails, otherwise nil.
func (dst *RedisClient) LRem(ctx context.Context, key string, count int64, value string) error {
	start := time.Now()

	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to remove the value.
		_, err = dst.cluster.LRem(ctx, key, count, value).Result()
	} else {
		// If using a single Redis instance, use the client to remove the value.
		_, err = dst.client.LRem(ctx, key, count, value).Result()
	}

	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) LREM (%.2f ms)\033[1m \033[34m%q %d %q\033[0m", dst.db, float64(time.Since(start))/1000000, key, count, value)

	return err
}

// Get first value from Redis list by key and remove it from list. If list is empty, waits for value for ttl seconds.
// If the list is empty and the timeout expires, it returns the default value without an error.
// If an error occurs, it returns an empty string and the error.
//
// Parameters:
//   - ctx: The context for the operation.
//   - key: The key in Redis database.
//   - def: The default value to return if the list is empty.
//   - ttl: The time to wait for a value in seconds.
//
// Returns:
//   - The first value from the list, or the default value if the list is empty.
//   - An error if the operation fails.
func (dst *RedisClient) BLPop(ctx context.Context, key string, def string, ttl uint64) (string, error) {
	start := time.Now()

	var str []string
	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to block pop the value.
		str, err = dst.cluster.BLPop(ctx, time.Duration(ttl)*time.Second, key).Result()
	} else {
		// If using a single Redis instance, use the client to block pop the value.
		str, err = dst.client.BLPop(ctx, time.Duration(ttl)*time.Second, key).Result()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) BLPOP (%.2f ms)\033[1m \033[34m%q\033[0m", dst.db, float64(time.Since(start))/1000000, key)
	if err != nil {
		if err == redis.Nil {
			return def, nil
		}
		return "", err
	}
	return str[1], nil
}

// Get first value from Redis list by key and remove it from list and put it on the another list. If list is empty, waits for value for ttl seconds.
// If the list is empty and the timeout expires, it returns the default value without an error.
// If an error occurs, it returns an empty string and the error.
//
// Parameters:
//   - ctx: The context for the operation.
//   - source: The source key in Redis database.
//   - destination: The destination key in Redis database.
//   - srcpos: The position in the source list to pop from ("LEFT" or "RIGHT").
//   - dstpos: The position in the destination list to push to ("LEFT" or "RIGHT").
//   - def: The default value to return if the source list is empty.
//   - ttl: The time to wait for a value in seconds.
//
// Returns:
//   - The first value from the source list, or the default value if the source list is empty.
//   - An error if the operation fails.
func (dst *RedisClient) BLMove(ctx context.Context, source, destination, srcpos, dstpos string, def string, ttl uint64) (string, error) {
	start := time.Now()

	var str string
	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to block move the value.
		str, err = dst.cluster.BLMove(ctx, source, destination, srcpos, dstpos, time.Duration(ttl)*time.Second).Result()
	} else {
		// If using a single Redis instance, use the client to block move the value.
		str, err = dst.client.BLMove(ctx, source, destination, srcpos, dstpos, time.Duration(ttl)*time.Second).Result()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) BLMOVE (%.2f ms)\033[1m \033[34m%q (%s) -> %q (%s)\033[0m", dst.db, float64(time.Since(start))/1000000, source, srcpos, destination, dstpos)
	if err != nil {
		if err == redis.Nil {
			return def, nil
		}
		return "", err
	}
	return str, nil
}

// Sets the specified expiration time for redis key, in seconds.
// If the key does not exist, it returns an error.
// If the operation is successful, it returns nil.
// This function uses the Redis EXPIRE command to set the expiration time for the key.
//
// Parameters:
//   - ctx: The context for the operation.
//   - key: The key in Redis database.
//   - ttl: The time to live for the key in seconds.
//
// Returns:
//   - An error if the operation fails, otherwise nil.
func (dst *RedisClient) Expire(ctx context.Context, key string, ttl uint64) error {
	start := time.Now()

	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to set the expiration time.
		err = dst.cluster.Expire(ctx, key, time.Duration(ttl)*time.Second).Err()
	} else {
		// If using a single Redis instance, use the client to set the expiration time.
		err = dst.client.Expire(ctx, key, time.Duration(ttl)*time.Second).Err()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) EXPIRE (%.2f ms)\033[1m \033[34m%q %d\033[0m", dst.db, float64(time.Since(start))/1000000, key, ttl)
	return err
}

// Get the remaining time to live of a key that has a timeout.
// If the key does not exist or has no timeout, it returns an error.
// If the key exists and has a timeout, it returns the remaining time in seconds.
// This function uses the Redis TTL command to get the time to live of the key.
//
// Parameters:
//   - ctx: The context for the operation.
//   - key: The key in Redis database.
//
// Returns:
//   - The remaining time to live of the key in seconds, or an error if the key does not exist or has no timeout.
//   - An error if the operation fails.
func (dst *RedisClient) TTL(ctx context.Context, key string) (int64, error) {
	start := time.Now()

	var ttl time.Duration
	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to get the TTL.
		ttl, err = dst.cluster.TTL(ctx, key).Result()
	} else {
		// If using a single Redis instance, use the client to get the TTL.
		ttl, err = dst.client.TTL(ctx, key).Result()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) TTL (%.2f ms)\033[1m \033[34m%q\033[0m", dst.db, float64(time.Since(start))/1000000, key)
	return int64(ttl.Seconds()), err
}

// Keys returns all keys matching the given pattern in the Redis database.
// If no keys match the pattern, it returns an empty slice without an error.
//
// Parameters:
//   - ctx: The context for the operation.
//   - pattern: The pattern to match keys against (e.g., "prefix:*").
//
// Returns:
//   - A slice of strings containing the keys that match the pattern.
//   - An error if the operation fails or if there are no matching keys.
func (dst *RedisClient) Keys(ctx context.Context, pattern string) ([]string, error) {
	start := time.Now()

	var keys []string
	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to get the keys.
		keys, err = dst.cluster.Keys(ctx, pattern).Result()
	} else {
		// If using a single Redis instance, use the client to get the keys.
		keys, err = dst.client.Keys(ctx, pattern).Result()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) KEYS (%.2f ms)\033[1m \033[34m%q\033[0m", dst.db, float64(time.Since(start))/1000000, pattern)
	return keys, err
}

// Del removes a key from the Redis database.
// If the key does not exist, it does nothing and returns nil.
// If an error occurs, it returns the error.
// This function uses the Redis DEL command to delete the key.
//
// Parameters:
//   - ctx: The context for the operation.
//   - key: The key in Redis database.
//
// Returns:
//   - An error if the operation fails, otherwise nil.
func (dst *RedisClient) Del(ctx context.Context, key string) error {
	start := time.Now()

	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to delete the key.
		err = dst.cluster.Del(ctx, key).Err()
	} else {
		// If using a single Redis instance, use the client to delete the key.
		err = dst.client.Del(ctx, key).Err()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) DEL (%.2f ms)\033[1m \033[31m%q\033[0m", dst.db, float64(time.Since(start))/1000000, key)
	return err
}

// XGroupCreateMkStream creates a new stream group with the specified start command.
// If the stream does not exist, it creates the stream with the specified start command.
// If the group already exists, it returns an error.
// This function uses the Redis XGROUP CREATE command with the MKSTREAM option to create the group.
//
// Parameters:
//   - ctx: The context for the operation.
//   - stream: The name of the stream to create the group for.
//   - group: The name of the group to create.
//   - start: The start command for the group, typically "$" to start from the latest message.
//
// Returns:
//   - An error if the operation fails, or if the group already exists.
func (dst *RedisClient) XGroupCreateMkStream(ctx context.Context, stream, group, start string) error {
	startTime := time.Now()

	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to create the stream.
		err = dst.cluster.XGroupCreateMkStream(ctx, stream, group, start).Err()
	} else {
		// If using a single Redis instance, use the client to create the stream.
		err = dst.client.XGroupCreateMkStream(ctx, stream, group, start).Err()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) XGROUP CREATE MKSTREAM (%.2f ms)\033[1m \033[33m %q %q %q\033[0m", dst.db, float64(time.Since(startTime))/1000000, stream, group, start)
	if err != nil && err.Error() == "BUSYGROUP Consumer Group name already exists" {
		return ErrorGroupAlreadyExists
	}

	return err
}

// XGroupDestroy removes a stream group from Redis.
// If the group does not exist, it returns an error.
// This function uses the Redis XGROUP DESTROY command to remove the group.
//
// Parameters:
//   - ctx: The context for the operation.
//   - stream: The name of the stream to destroy the group from.
//   - group: The name of the group to destroy.
//
// Returns:
//   - An error if the operation fails, or if the group does not exist.
func (dst *RedisClient) XGroupDestroy(ctx context.Context, stream, group string) error {
	start := time.Now()

	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to destroy the group.
		err = dst.cluster.XGroupDestroy(ctx, stream, group).Err()
	} else {
		// If using a single Redis instance, use the client to destroy the group.
		err = dst.client.XGroupDestroy(ctx, stream, group).Err()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) XGROUP DESTROY (%.2f ms)\033[1m \033[31m%q %q\033[0m", dst.db, float64(time.Since(start))/1000000, stream, group)
	return err
}

// XGroupDelConsumer removes a consumer from a stream group in Redis.
// If the consumer does not exist, it returns an error.
// This function uses the Redis XGROUP DELCONSUMER command to delete the consumer.
//
// Parameters:
//   - ctx: The context for the operation.
//   - stream: The name of the stream to delete the consumer from.
//   - group: The name of the group to delete the consumer from.
//   - consumer: The name of the consumer to delete.
//
// Returns:
//   - An error if the operation fails, or if the consumer does not exist.
func (dst *RedisClient) XGroupDelConsumer(ctx context.Context, stream, group, consumer string) error {
	start := time.Now()

	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to delete the consumer.
		err = dst.cluster.XGroupDelConsumer(ctx, stream, group, consumer).Err()
	} else {
		// If using a single Redis instance, use the client to delete the consumer.
		err = dst.client.XGroupDelConsumer(ctx, stream, group, consumer).Err()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) XGROUP DEL CONSUMER (%.2f ms)\033[1m \033[31m%q %q %q\033[0m", dst.db, float64(time.Since(start))/1000000, stream, group, consumer)
	return err
}

func (dst *RedisClient) XAdd(ctx context.Context, args *redis.XAddArgs) (string, error) {
	start := time.Now()

	var id string
	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to add the message.
		id, err = dst.cluster.XAdd(ctx, args).Result()
	} else {
		// If using a single Redis instance, use the client to add the message.
		id, err = dst.client.XAdd(ctx, args).Result()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) XADD (%.2f ms)\033[1m \033[33m%q \"%s\"\033[36m | %s\033[0m", dst.db, float64(time.Since(start))/1000000, args.Stream, strings.ReplaceAll(fmt.Sprintf("%v", args.Values), "\n", " "), id)
	return id, err
}

// XReadGroup reads messages from a stream group in Redis.
// It retrieves messages from the specified stream and group, starting from the last acknowledged message.
// If the group does not exist, it returns an error.
// This function uses the Redis XREADGROUP command to read messages from the group.
//
// Parameters:
//   - ctx: The context for the operation.
//   - args: The arguments for reading from the group, including the group name, stream names, and count.
//
// Returns:
//   - A slice of XStream containing the messages read from the group.
//   - An error if the operation fails, or if the group does not exist.
func (dst *RedisClient) XReadGroup(ctx context.Context, args *redis.XReadGroupArgs) ([]redis.XStream, error) {
	start := time.Now()

	var messages []redis.XStream
	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to read from the group.
		messages, err = dst.cluster.XReadGroup(ctx, args).Result()
	} else {
		// If using a single Redis instance, use the client to read from the group.
		messages, err = dst.client.XReadGroup(ctx, args).Result()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) XREADGROUP (%.2f ms)\033[1m \033[34m%q \"%s\" %d\033[0m", dst.db, float64(time.Since(start))/1000000, args.Group, strings.Join(args.Streams, `" "`), args.Count)
	if err != nil && err.Error() == "redis: nil" {
		return nil, nil
	}
	return messages, err
}

// XAutoClaim auto claims messages from a stream group in Redis.
// It retrieves messages that have not been acknowledged by the specified group and reassigns them to the group.
// If the group does not exist, it returns an error.
// This function uses the Redis XAUTOCLAIM command to auto claim messages.
//
// Parameters:
//   - ctx: The context for the operation.
//   - args: The arguments for auto claiming messages, including the group name, stream name, and count.
//
// Returns:
//   - A slice of XMessage containing the messages that have been auto claimed.
//   - An error if the operation fails, or if the group does not exist.
func (dst *RedisClient) XAutoClaim(ctx context.Context, args *redis.XAutoClaimArgs) ([]redis.XMessage, error) {
	start := time.Now()

	var messages []redis.XMessage
	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to auto claim messages.
		messages, _, err = dst.cluster.XAutoClaim(ctx, args).Result()
	} else {
		// If using a single Redis instance, use the client to auto claim messages.
		messages, _, err = dst.client.XAutoClaim(ctx, args).Result()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) XAUTOCLAIM (%.2f ms)\033[1m \033[34m%q %q %d\033[0m", dst.db, float64(time.Since(start))/1000000, args.Group, args.Stream, args.Count)
	return messages, err
}

func (dst *RedisClient) XAck(ctx context.Context, stream, group string, ids ...string) (int64, error) {
	start := time.Now()

	var count int64
	var err error
	if dst.cluster != nil {
		// If using a Redis cluster, use the cluster client to acknowledge messages.
		count, err = dst.cluster.XAck(ctx, stream, group, ids...).Result()
	} else {
		// If using a single Redis instance, use the client to acknowledge messages.
		count, err = dst.client.XAck(ctx, stream, group, ids...).Result()
	}
	dst.logQuery(ctx, "\033[1m\033[36mRedis(%d) XACK (%.2f ms)\033[1m \033[33m%q %q \"%s\"\033[0m", dst.db, float64(time.Since(start))/1000000, stream, group, strings.Join(ids, " "))
	return count, err
}

func (dst *RedisClient) logQuery(args ...any) {
	if len(args) > 1 {
		dst.lastQuery = fmt.Sprintf(args[1].(string), args[2:]...)
	} else {
		dst.lastQuery = fmt.Sprint(args[1:]...)
	}
	if dst.DoNotLogQueries {
		return
	}
	dst.Debug(args[0].(context.Context), dst.lastQuery)
}

// Client returns the underlying Redis client instance.
// This function is useful for accessing additional Redis client methods that are not directly exposed by the RedisClient struct.
// It allows you to perform operations that are not defined in the RedisClient interface.
//
// Returns:
//   - A pointer to the redis.Client instance.
func (dst *RedisClient) Client() *redis.Client {
	return dst.client
}

// Cluster returns the underlying Redis cluster client instance.
// This function is useful for accessing additional Redis cluster client methods that are not directly exposed by the RedisClient struct.
// It allows you to perform operations that are specific to Redis clusters.
//
// Returns:
//   - A pointer to the redis.ClusterClient instance.
func (dst *RedisClient) Cluster() *redis.ClusterClient {
	return dst.cluster
}

// LastQuery returns the last executed Redis query as a string.
// This function is useful for debugging purposes, allowing you to see the last query executed by the RedisClient.
//
// Returns:
//   - A string containing the last executed query.
func (dst *RedisClient) LastQuery() string {
	// Returns the last executed query as a string.
	return dst.lastQuery
}
