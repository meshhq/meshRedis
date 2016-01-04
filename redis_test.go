package meshRedis

import (
	"testing"
	"time"
)

import . "github.com/meshhq/meshCore/Godeps/_workspace/src/gopkg.in/check.v1"

const localRedisURL = "redis://127.0.0.1:6379/0"

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type MeshRedisTest struct{}

var _ = Suite(&MeshRedisTest{})

func (s *MeshRedisTest) SetUpTest(c *C) {
	err := Connect(localRedisURL)
	c.Assert(err, Equals, nil)
}

func (s *MeshRedisTest) TearDownTest(c *C) {
	err := Close()
	c.Assert(err, Equals, nil)
}

//---------
// Connection
//---------

func (s *MeshRedisTest) TestCreateNewMeshRedisSession(c *C) {
	// Test getting a new MeshRedisSession
	session := NewSession()
	err := session.Ping()
	c.Assert(err, Equals, nil)

	// Close the connection
	err = session.Close()
	c.Assert(err, Equals, nil)
}

//---------
// Setting / Getting Strings
//---------

func (s *MeshRedisTest) TestSettingAString(c *C) {
	// Test getting a new MeshRedisSession
	session := NewSession()
	session.SetString("foo", "bar")
	val, err := session.GetString("foo")
	c.Assert(val, Equals, "bar")
	c.Assert(err, Equals, nil)
}

func (s *MeshRedisTest) TestSettingAStringWithExpiration(c *C) {
	// Test getting a new MeshRedisSession
	session := NewSession()
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

func (s *MeshRedisTest) TestSettingAInteger(c *C) {
	// Test getting a new MeshRedisSession
	session := NewSession()
	session.SetInt("foo", 101)
	val, err := session.GetInt("foo")
	c.Assert(val, Equals, 101)
	c.Assert(err, Equals, nil)
}

func (s *MeshRedisTest) TestSettingAIntegerWithExpiration(c *C) {
	// Test getting a new MeshRedisSession
	session := NewSession()
	session.SetIntWithExpiration("foo", 1, 102)
	val, err := session.GetInt("foo")
	c.Assert(val, Equals, 102)
	c.Assert(err, Equals, nil)

	// Wait for a second and recheck the val
	time.Sleep(1 * time.Second)
	val, err = session.GetInt("foo")
	c.Assert(val, Equals, 0)
	c.Assert(err, Equals, nil)
}

//---------
// Expire / TTL
//---------

func (s *MeshRedisTest) TestUpdatingExpirationOfAnEmptyKeyReturnsAnError(c *C) {
	key := "KeyIveNeverEverUsedBefore"

	// Test getting a new MeshRedisSession
	session := NewSession()

	// First Test TTL is returning the correct default
	err := session.UpdateExpirationOfKey(key, 1)
	c.Assert(err, Not(Equals), nil)

	session.SetString(key, "something")

	// First Test TTL is returning the correct default
	err = session.UpdateExpirationOfKey(key, 1)
	c.Assert(err, Equals, nil)
}

func (s *MeshRedisTest) TestUpdatingAKeysExpiration(c *C) {
	key := "fooExpire"
	value := "expireMe"

	// Test getting a new MeshRedisSession
	session := NewSession()
	session.SetString(key, value)

	// First Test TTL is returning the correct default
	ttl, err := session.TTLForKey(key)
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
	ttl, err = session.TTLForKey(key)
	c.Assert(err, Equals, nil)
	c.Assert(ttl, Equals, 1)

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
