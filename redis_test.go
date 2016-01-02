package meshRedis

import (
	"testing"
	"time"
)

import . "github.com/meshhq/meshCore/Godeps/_workspace/src/gopkg.in/check.v1"

const redisURL = "redis://127.0.0.1:6379/0"

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type MeshRedisTest struct{}

var _ = Suite(&MeshRedisTest{})

func (s *MeshRedisTest) SetUpTest(c *C) {
	err := Connect(redisURL)
	c.Assert(err, Equals, nil)
}

func (s *MeshRedisTest) TearDownTest(c *C) {
	err := Close()
	c.Assert(err, Equals, nil)
}

func (s *MeshRedisTest) TestCreateNewMeshRedisSession(c *C) {
	// Test getting a new MeshRedisSession
	session := NewSession()
	err := session.Ping()
	c.Assert(err, Equals, nil)

	// Close the connection
	err = session.Close()
	c.Assert(err, Equals, nil)
}

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
