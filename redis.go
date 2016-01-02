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
func (session RedisSession) Ping() error {
	return pingRedis(session.connection, time.Time{})
}

// NewSession issues a new meshRedis ResisSession. This will be the
// interface that will be used to perform actions on redis
func NewSession() *RedisSession {
	connection := pool.Get()
	return &RedisSession{connection}
}

// Close kills the RedisSession instance
func (session RedisSession) Close() error {
	return session.connection.Close()
}

//---------
// Redis Commands
//---------

// SetString assigns the string to the supplied key in redis
func (session RedisSession) SetString(key string, value string) error {
	_, err := session.connection.Do("SET", key, value)
	return err
}

// SetStringWithExpiration assigns the int to the supplied key in redis
func (session RedisSession) SetStringWithExpiration(key string, seconds int, value string) error {
	_, err := session.connection.Do("SETEX", key, seconds, value)
	return err
}

// GetString retreives the value from the store.
func (session RedisSession) GetString(key string) (value string, err error) {
	val, err := session.connection.Do("GET", key)

	if err != nil || val == nil {
		return "", err
	}

	if byteVal, ok := val.([]byte); ok {
		return string(byteVal), err
	}

	errorMsg := fmt.Sprintf("The value for the key %s is not a string", key)
	return "", errors.New(errorMsg)
}

// SetInt assigns the int to the supplied key in redis
func (session RedisSession) SetInt(key string, value int) error {
	_, err := session.connection.Do("SET", key, value)
	return err
}

// SetIntWithExpiration assigns the int to the supplied key in redis
func (session RedisSession) SetIntWithExpiration(key string, seconds int, value int) error {
	_, err := session.connection.Do("SETEX", key, seconds, value)
	return err
}

// GetInt retreives the value from the store.
func (session RedisSession) GetInt(key string) (value int, err error) {
	val, err := session.connection.Do("GET", key)
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
