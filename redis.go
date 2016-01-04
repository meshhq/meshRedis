package meshRedis

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
)

// RedisSession models a connection to an underlying NoSQL persistence store.
type RedisSession struct {
	connection redis.Conn
}

// pool is the connection pool to the Redis instance
var pool *redis.Pool

// connection is the main connection to redis
var connection *redis.Conn

//---------
// Redis Connection
//---------

// Connect establishes a connection to the Redis instance at the provided
// url. If the connection attempt is unsuccessful, an error object
// will be returned describing the failure.
//
// @param url: The URL address to which the connection will be established.
func Connect(url string) error {
	pool = createNewConnectionPool(url)
	return pingRedis(pool.Get(), time.Time{})
}

// Close kills the entire connection pool to redis
func Close() error {
	return pool.Close()
}

func createNewConnectionPool(redisURL string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			connection, err := redis.DialURL(redisURL)
			if err != nil {
				return nil, err
			}

			// Later, we want to secure redis, not now
			// 'Do' will call the auth
			// if _, err := c.Do("AUTH", password); err != nil {
			// 	c.Close()
			// 	return nil, err
			// }
			return connection, err
		},
		TestOnBorrow: pingRedis}
}

// pingRedis is used internally to ping the connection
func pingRedis(connection redis.Conn, _ time.Time) error {
	_, err := connection.Do("PING")
	return err
}

// Ping is used internally to ping the connection
func (s RedisSession) Ping() error {
	return pingRedis(s.connection, time.Time{})
}

// NewSession issues a new meshRedis ResisSession. This will be the
// interface that will be used to perform actions on redis
func NewSession() *RedisSession {
	connection := pool.Get()
	return &RedisSession{connection}
}

// Close kills the RedisSession instance
func (s RedisSession) Close() error {
	return s.connection.Close()
}

//---------
// Redis Commands
//---------

// UpdateExpirationOfKey updates the expiration value of a key in redis
// If no key is found, the `error` return value will be non-nil
func (s RedisSession) UpdateExpirationOfKey(key string, seconds int) error {
	val, err := s.connection.Do("EXPIRE", key, seconds)
	if err != nil {
		return err
	}

	if updateTime, ok := val.(int64); ok {
		time := int(updateTime)
		if time == 0 {
			errorMsg := fmt.Sprintf("That key does not exist")
			return errors.New(errorMsg)
		}
		return nil
	}

	errorMsg := fmt.Sprintf("Error updating the expiration")
	return errors.New(errorMsg)
}

// TTLForKey returns the lifetime of the value associated with the key
// If no key is found, the `error` return value will be non-nil
func (s RedisSession) TTLForKey(key string) (int, error) {
	ttl, err := s.connection.Do("TTL", key)
	if err != nil || ttl == nil {
		return 0, err
	}

	if updateTime, ok := ttl.(int64); ok {
		return int(updateTime), err
	}

	errorMsg := fmt.Sprintf("Error processing the Key")
	return 0, errors.New(errorMsg)
}

//---------
// String Commands
//---------

// SetString assigns the string to the supplied key in redis
func (s RedisSession) SetString(key string, value string) error {
	_, err := s.connection.Do("SET", key, value)
	return err
}

// SetStringWithExpiration assigns the int to the supplied key in redis
func (s RedisSession) SetStringWithExpiration(key string, seconds int, value string) error {
	_, err := s.connection.Do("SETEX", key, seconds, value)
	return err
}

// GetString retreives the value from the store.
func (s RedisSession) GetString(key string) (value string, err error) {
	val, err := s.connection.Do("GET", key)

	if err != nil || val == nil {
		return "", err
	}

	if byteVal, ok := val.([]byte); ok {
		return string(byteVal), err
	}

	errorMsg := fmt.Sprintf("The value for the key %s is not a string", key)
	return "", errors.New(errorMsg)
}

//---------
// Int Commands
//---------

// SetInt assigns the int to the supplied key in redis
func (s RedisSession) SetInt(key string, value int) error {
	_, err := s.connection.Do("SET", key, value)
	return err
}

// SetIntWithExpiration assigns the int to the supplied key in redis
func (s RedisSession) SetIntWithExpiration(key string, seconds int, value int) error {
	_, err := s.connection.Do("SETEX", key, seconds, value)
	return err
}

// GetInt retreives the value from the store.
func (s RedisSession) GetInt(key string) (value int, err error) {
	val, err := s.connection.Do("GET", key)
	if err != nil || val == nil {
		return 0, err
	}

	if byteVal, ok := val.([]byte); ok {
		strVal := string(byteVal)
		intVal, err := strconv.Atoi(strVal)
		return intVal, err
	}

	errorMsg := fmt.Sprintf("The value for the key %s is not a integer", key)
	return 0, errors.New(errorMsg)
}
