package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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
	logging.CustomLogger               // CustomLogger: is an interface that allows the Redis client to use a custom logger for logging operations and errors.
	client               *redis.Client // client: is the Redis client used to interact with the Redis server.
	singlePush           *redis.Script // singlePush: is a Lua script used for atomic operations on Redis lists, specifically for pushing a value to a list only if the list is empty.
	db                   int           // db: is the Redis database number, used for logging purposes.
}

var (
	Redis RedisClient // Redis: is the global Redis client used to interact with the Redis server.

	ErrorListIsNotEmpty = errors.New("list is not empty")
)

// Start initializes the Redis client with the provided host, port, password, and database number.
// It connects to the Redis server and checks the connection by sending a PING command.
// If the connection fails, it logs a fatal error and exits the program.
//
// Parameters:
//   - ctx: The context for the operation, allowing for cancellation and timeouts.
//   - host: The Redis server host.
//   - port: The Redis server port.
//   - password: The Redis server password.
//   - db: The Redis database number to use.
//
// This function also initializes a Lua script for single push operations on Redis lists.
// It checks if the Redis list is empty before pushing a new value to it.
// If the list is not empty, it returns an error indicating that the list is not empty.
// This function should be called at the start of the application to establish a connection to Redis.
// It sets the usedDB variable to the current database number for logging purposes.
func (dst *RedisClient) Start(ctx context.Context, host string, port int, password string, db int) {
	dst.client = redis.NewClient(&redis.Options{
		Addr:     host + ":" + strconv.Itoa(port),
		Password: password,
		DB:       db,
	})
	dst.db = db // Set the usedDB variable to the current database number.
	res := dst.client.Ping(ctx)
	if res.Err() != nil {
		dst.Fatal(ctx, "Failed to connect to Redis database: %v", res.Err())
	}
	dst.Info(ctx, "Connected to Redis database: redis://%v:%v/%v", host, port, db)

	dst.singlePush = redis.NewScript(`
        if redis.call("LLEN", KEYS[1]) == 0 then
            return redis.call("LPUSH", KEYS[1], ARGV[1])
        else
            return -1
        end
    `)
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

	str, err := dst.client.Get(ctx, key).Result()
	dst.Debug(ctx, "\033[1m\033[36mRedis(%d) GET (%.2f ms)\033[1m \033[34m%q\033[0m", dst.db, float64(time.Since(start))/1000000, key)
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

	str, err := dst.client.LPos(ctx, key, value, redis.LPosArgs{}).Result()
	dst.Debug(ctx, "\033[1m\033[36mRedis(%d) LPOS (%.2f ms)\033[1m \033[34m%q\033[0m", dst.db, float64(time.Since(start))/1000000, key)
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
	strs, err := dst.client.MGet(ctx, keys...).Result()
	dst.Debug(ctx, "\033[1m\033[36mRedis(%d) MGET (%.2f ms)\033[1m \033[34m%q\033[0m", dst.db, float64(time.Since(start))/1000000, strings.Join(keys, ", "))
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

	err := dst.client.Set(ctx, key, value, time.Duration(expiration)*time.Second).Err()
	dst.Debug(ctx, "\033[1m\033[36mRedis(%d) SET (%.2f ms)\033[1m \033[33m%q=%q\033[0m", dst.db, float64(time.Since(start))/1000000, key, strings.ReplaceAll(value.(string), "\n", " "))
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
func (dst *RedisClient) MultiSet(ctx context.Context, sets []Set) error {
	start := time.Now()
	pipe := dst.client.TxPipeline()
	for _, set := range sets {
		if set.TTL > 0 {
			pipe.Set(ctx, set.Key, set.Value, time.Duration(set.TTL)*time.Second)
		} else {
			pipe.Set(ctx, set.Key, set.Value, 0)
		}
	}
	_, err := pipe.Exec(ctx)
	vals := []string{}
	for _, set := range sets {
		vals = append(vals, fmt.Sprintf("%q=%q", set.Key, strings.ReplaceAll(set.Value.(string), "\n", " ")))
	}
	dst.Debug(ctx, "\033[1m\033[36mRedis(%d) MULTISET (%.2f ms)\033[1m \033[33m%s\033[0m", dst.db, float64(time.Since(start))/1000000, strings.Join(vals, ", "))
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

	res, err := dst.client.LPush(ctx, key, value).Result()
	dst.Debug(ctx, "\033[1m\033[36mRedis(%d) LPUSH(%d) (%.2f ms)\033[1m \033[33m%q=%q\033[0m\033[0m", dst.db, res, float64(time.Since(start))/1000000, key, value)
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

	res, err := dst.singlePush.Run(ctx, dst.client, []string{key}, value).Result()
	dst.Debug(ctx, "\033[1m\033[36mRedis(%d) SINGLEPUSH (%.2f ms)\033[1m \033[34m%q=%q\033[0m", dst.db, float64(time.Since(start))/1000000, key, value)
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

	res, err := dst.client.LRange(ctx, key, 0, 0).Result()
	dst.Debug(ctx, "\033[1m\033[36mRedis(%d) LRANGE (%.2f ms)\033[1m \033[34m%q\033[0m", dst.db, float64(time.Since(start))/1000000, key)

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

	res, err := dst.client.LLen(ctx, key).Result()
	dst.Debug(ctx, "\033[1m\033[36mRedis(%d) LLEN (%.2f ms)\033[1m \033[34m%q\033[0m", dst.db, float64(time.Since(start))/1000000, key)

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

	_, err := dst.client.LRem(ctx, key, count, value).Result()
	dst.Debug(ctx, "\033[1m\033[36mRedis(%d) LREM (%.2f ms)\033[1m \033[34m%q %d %q\033[0m", dst.db, float64(time.Since(start))/1000000, key, count, value)

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

	str, err := dst.client.BLPop(ctx, time.Duration(ttl)*time.Second, key).Result()
	dst.Debug(ctx, "\033[1m\033[36mRedis(%d) BLPOP (%.2f ms)\033[1m \033[34m%q\033[0m", dst.db, float64(time.Since(start))/1000000, key)
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

	str, err := dst.client.BLMove(ctx, source, destination, srcpos, dstpos, time.Duration(ttl)*time.Second).Result()
	dst.Debug(ctx, "\033[1m\033[36mRedis(%d) BLMOVE (%.2f ms)\033[1m \033[34m%q (%s) -> %q (%s)\033[0m", dst.db, float64(time.Since(start))/1000000, source, srcpos, destination, dstpos)
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

	err := dst.client.Expire(ctx, key, time.Duration(ttl)*time.Second).Err()
	dst.Debug(ctx, "\033[1m\033[36mRedis(%d) EXPIRE (%.2f ms)\033[1m \033[34m%q %d\033[0m", dst.db, float64(time.Since(start))/1000000, key, ttl)
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

	ttl, err := dst.client.TTL(ctx, key).Result()
	dst.Debug(ctx, "\033[1m\033[36mRedis(%d) TTL (%.2f ms)\033[1m \033[34m%q\033[0m", dst.db, float64(time.Since(start))/1000000, key)
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

	keys, err := dst.client.Keys(ctx, pattern).Result()
	dst.Debug(ctx, "\033[1m\033[36mRedis(%d) KEYS (%.2f ms)\033[1m \033[34m%q\033[0m", dst.db, float64(time.Since(start))/1000000, pattern)
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

	err := dst.client.Del(ctx, key).Err()
	dst.Debug(ctx, "\033[1m\033[36mRedis(%d) DEL (%.2f ms)\033[1m \033[31m%q\033[0m", dst.db, float64(time.Since(start))/1000000, key)
	return err
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
