package meshRedis

import (
	"flag"
	"os"
	"testing"
	"time"
)

import . "gopkg.in/check.v1"

const localRedisURL = "redis://127.0.0.1:6379/2"

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type MeshRedisTest struct{}

var live = flag.Bool("redis", false, "Include redis tests")

var _ = Suite(&MeshRedisTest{})

// Active session for the tests
var session *RedisSession

func (m *MeshRedisTest) SetUpSuite(c *C) {
	// Set the ENV
	os.Setenv("REDIS_URL", localRedisURL)

	err := SetupRedis()
	c.Assert(err, Equals, nil)
}

func (m *MeshRedisTest) TearDownSuite(c *C) {
	err := ClosePool()
	c.Assert(err, Equals, nil)
}

func (m *MeshRedisTest) SetUpTest(c *C) {
	session = NewSession()
}

func (m *MeshRedisTest) TearDownTest(c *C) {
	err := session.CloseSession()
	c.Assert(err, Equals, nil)
}

//---------
// Connection
//---------

func (m *MeshRedisTest) TestCreateNewMeshRedisSession(c *C) {
	// Test getting a new MeshRedisSession
	err := session.Ping()
	c.Assert(err, Equals, nil)
}

//---------
// Setting / Getting Strings
//---------

func (m *MeshRedisTest) TestSettingAString(c *C) {
	// Test getting a new MeshRedisSession
	session.SetString("foo", "bar")
	val, err := session.GetString("foo")
	c.Assert(val, Equals, "bar")
	c.Assert(err, Equals, nil)
}

func (m *MeshRedisTest) TestSettingAStringWithExpiration(c *C) {
	// Test getting a new MeshRedisSession
	session.SetStringWithExpiration("foo", 1, "barBaz")
	val, err := session.GetString("foo")
	c.Assert(val, Equals, "barBaz")
	c.Assert(err, Equals, nil)

	// Wait for a second and recheck the val
	time.Sleep(1 * time.Second)
	val, err = session.GetString("barBaz")
	c.Assert(val, Equals, "")
	c.Assert(err, Equals, nil)
}

//---------
// Setting / Getting Ints
//---------

func (m *MeshRedisTest) TestSettingAInteger(c *C) {
	// Test getting a new MeshRedisSession
	session.SetInt("foo", 101)
	val, err := session.GetInt("foo")
	c.Assert(val, Equals, 101)
	c.Assert(err, Equals, nil)
}

func (m *MeshRedisTest) TestSettingAIntegerWithExpiration(c *C) {
	// Test getting a new MeshRedisSession
	session.SetIntWithExpiration("foo", 1, 102)
	val, err := session.GetInt("foo")
	c.Assert(val, Equals, 102)
	c.Assert(err, Equals, nil)

	// Wait for a second and recheck the val
	time.Sleep(2 * time.Second)
	val, err = session.GetInt("foo")
	c.Assert(val, Equals, 0)
	c.Assert(err, Equals, nil)
}

//---------
// Expire / TTL
//---------

func (m *MeshRedisTest) TestUpdatingExpirationOfAnEmptyKeyReturnsAnError(c *C) {
	key := "KeyIveNeverEverUsedBefore"

	// Test getting a new MeshRedisSession

	// First Test TTL is returning the correct default
	err := session.UpdateExpirationOfKey(key, 1)
	c.Assert(err, Not(Equals), nil)

	session.SetString(key, "something")

	// First Test TTL is returning the correct default
	err = session.UpdateExpirationOfKey(key, 1)
	c.Assert(err, Equals, nil)
}

func (m *MeshRedisTest) TestUpdatingAKeysExpiration(c *C) {
	key := "fooExpire"
	value := "expireMe"

	// Test getting a new MeshRedisSession
	session.SetString(key, value)

	// First Test TTL is returning the correct default
	ttl, err := session.PTTLForKey(key)
	c.Assert(err, Equals, nil)
	c.Assert(ttl, Equals, -1)

	// Check Value is there
	val, err := session.GetString(key)
	c.Assert(val, Equals, value)
	c.Assert(err, Equals, nil)

	// First Test TTL is returning the correct default
	err = session.UpdateExpirationOfKey(key, 1)
	c.Assert(err, Equals, nil)

	// Test Updated TTL
	ttl, err = session.PTTLForKey(key)
	c.Assert(err, Equals, nil)
	c.Assert(ttl > 990, Equals, true)

	// Check Value is there
	val, err = session.GetString(key)
	c.Assert(val, Equals, value)
	c.Assert(err, Equals, nil)

	// Sleep for timeout
	time.Sleep(2 * time.Second)
	val, err = session.GetString(key)
	c.Assert(val, Equals, "")
	c.Assert(err, Equals, nil)
}

//---------
// Flushing All Keys
//---------

func (m *MeshRedisTest) TestFlushAllKeys(c *C) {
	key := "someKey"

	// Test getting a new MeshRedisSession

	// Set / Check initial value
	session.SetString(key, "bar")
	val, err := session.GetString(key)
	c.Assert(val, Equals, "bar")
	c.Assert(err, Equals, nil)

	// Flush the DB
	err = session.FlushAllKeys()
	c.Assert(err, Equals, nil)

	// Test that string is empty
	val, err = session.GetString(key)
	c.Assert(val, Equals, "")
	c.Assert(err, Equals, nil)
}

//---------
// RPush / RPushX
//---------

// Tests if RPushX (push only if it exists), RPUSH and Delete
func (m *MeshRedisTest) TestRPushAndRPushX(c *C) {
	key := "someKey"

	// Set pushing on a list that doesn't exist. Shouldn't work
	count, err := session.RPushX(key, key)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0)

	// Confirm list count is 0
	count, err = session.GetListCount(key)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0)

	// Push and create a list
	count, err = session.RPush(key, key)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1)

	// Confirm list count is 1
	count, err = session.GetListCount(key)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1)

	// Set pushing on a list that exists. Should work this time
	count, err = session.RPushX(key, key)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 2)

	// Confirm list count is 1
	count, err = session.GetListCount(key)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 2)

	// Delete the list
	err = session.Delete(key)
	c.Assert(err, IsNil)

	// Confirm list count is 0
	count, err = session.GetListCount(key)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0)
}

//---------
// Multi / Pipelining
//---------

// Tests the atomic opp + ttl + GetListCount
func (m *MeshRedisTest) TestAtomicPushOnListWithExpiration(c *C) {
	key := "someKey"

	// Expiration is half a second
	expiration := int64(500)

	// Test getting a new MeshRedisSession

	// Set list w/ expiration
	err := session.AtomicPushOnListWithMsExpiration(key, key, expiration)
	c.Assert(err, IsNil)

	// Check if list exists and count is correct
	count, err := session.GetListCount(key)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1)

	// Check TTL is greater than 0
	ttl, err := session.PTTLForKey(key)
	c.Assert(err, IsNil)
	c.Assert(ttl, Not(Equals), 0)

	// Sleep for time
	timeWithMargin := float64(expiration) * 1.1
	sleepTime := time.Duration(timeWithMargin) * time.Millisecond
	time.Sleep(sleepTime)

	// Re Run tests w/ key expired
	// Check if list exists and count is correct
	count, err = session.GetListCount(key)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0)

	// Check TTL is greater than 0
	ttl, err = session.PTTLForKey(key)
	c.Assert(err, IsNil)
	c.Assert(ttl < 0, Equals, true)
}
