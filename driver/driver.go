package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/ncrypthic/inmem/engine"
	"github.com/ncrypthic/inmem/engine/log"
	"github.com/ncrypthic/inmem/engine/protocol"
)

func init() {
	sql.Register("ramsql", NewDriver())
	log.SetLevel(log.WarningLevel)
}

// Server structs holds engine for each sql.DB instance.
// This way a sql.DB cann open as much connection to engine as wanted
// without colliding with another engine (during tests for example)
// with the unique constraint of providing a unique DataSourceName
type Server struct {
	endpoint protocol.DriverEndpoint
	engine   *engine.Engine
	driver   driver.Driver
	dsn      string

	// Kill server on last connection closing
	sync.Mutex
	connCount int64
}

// Driver is the driver entrypoint,
// implementing database/sql/driver interface
type Driver struct {
	// Mutex protect the map of Server
	sync.Mutex
	// Holds all matching sql.DB instances of RamSQL engine
	servers map[string]*Server
}

// NewDriver creates a driver object
func NewDriver() *Driver {
	d := &Driver{}
	d.servers = make(map[string]*Server)
	return d
}

type connConf struct {
	Proto    string
	Addr     string
	Laddr    string
	Db       string
	Password string
	User     string
	Timeout  time.Duration
}

// Open return an active connection so RamSQL server
// If there is no connection in pool, start a new server.
// After first instantiation of the server,
func (rs *Driver) Open(dsn string) (conn driver.Conn, err error) {
	rs.Lock()
	defer rs.Unlock()

	connConf, err := parseConnectionURI(dsn)
	if err != nil {
		return nil, err
	}

	dsnServer, exist := rs.servers[dsn]
	if !exist {
		driverEndpoint, engineEndpoint, err := endpoints(connConf)
		if err != nil {
			return nil, err
		}

		engine, err := engine.New(engineEndpoint)
		if err != nil {
			return nil, err
		}

		driverConn, err := driverEndpoint.New(dsn)
		if err != nil {
			return nil, err
		}

		dsnServer := &Server{
			endpoint: driverEndpoint,
			engine:   engine,
			driver:   rs,
			dsn:      dsn,
		}
		rs.servers[dsn] = dsnServer

		return newConn(driverConn, dsnServer), nil
	}

	driverConn, err := dsnServer.endpoint.New(dsn)
	return newConn(driverConn, dsnServer), err
}

// OpenConnector must parse the name in the same format that Driver.Open
// parses the name parameter.
func (rs *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	rs.Lock()
	defer rs.Unlock()

	connConf, err := parseConnectionURI(dsn)
	if err != nil {
		return nil, err
	}

	s, exist := rs.servers[dsn]
	if !exist {
		driverEndpoint, engineEndpoint, err := endpoints(connConf)
		if err != nil {
			rs.Unlock()
			return nil, err
		}

		engine, err := engine.New(engineEndpoint)
		if err != nil {
			rs.Unlock()
			return nil, err
		}

		_, err = driverEndpoint.New(dsn)
		if err != nil {
			return nil, err
		}

		s = &Server{
			endpoint: driverEndpoint,
			engine:   engine,
			driver:   rs,
		}
		rs.servers[dsn] = s
	}
	return s, nil
}

// Connect returns a connection to the database.
// Connect may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The provided context.Context is for dialing purposes only
// (see net.DialContext) and should not be stored or used for
// other purposes. A default timeout should still be used
// when dialing as a connection pool may call Connect
// asynchronously to any query.
//
// The returned connection is only used by one goroutine at a
// time.
func (s *Server) Connect(_ context.Context) (driver.Conn, error) {
	driverConn, err := s.endpoint.New(s.dsn)
	if err != nil {
		return nil, err
	}
	return newConn(driverConn, s), nil
}

func endpoints(conf *connConf) (protocol.DriverEndpoint, protocol.EngineEndpoint, error) {
	switch conf.Proto {
	default:
		driver, engine := protocol.NewChannelEndpoints()
		return driver, engine, nil
	}
}

// The uri need to have the following syntax:
//
//   [PROTOCOL_SPECFIIC*]DBNAME/USER/PASSWD
//
// where protocol spercific part may be empty (this means connection to
// local server using default protocol). Currently possible forms:
//
//   DBNAME/USER/PASSWD
//   unix:SOCKPATH*DBNAME/USER/PASSWD
//   unix:SOCKPATH,OPTIONS*DBNAME/USER/PASSWD
//   tcp:ADDR*DBNAME/USER/PASSWD
//   tcp:ADDR,OPTIONS*DBNAME/USER/PASSWD
//   cloudsql:INSTANCE*DBNAME/USER/PASSWD
//
// OPTIONS can contain comma separated list of options in form:
//   opt1=VAL1,opt2=VAL2,boolopt3,boolopt4
// Currently implemented options:
//   laddr   - local address/port (eg. 1.2.3.4:0)
//   timeout - connect timeout in format accepted by time.ParseDuration
func parseConnectionURI(uri string) (*connConf, error) {
	c := &connConf{}

	if uri == "" {
		log.Info("Empty data source name, using 'default' engine")
		uri = "default"
	}

	pd := strings.SplitN(uri, "*", 2)
	if len(pd) == 2 {
		// Parse protocol part of URI
		p := strings.SplitN(pd[0], ":", 2)
		if len(p) != 2 {
			// Wrong protocol part of URI
			return c, nil
		}
		c.Proto = p[0]
		options := strings.Split(p[1], ",")
		c.Addr = options[0]
		for _, o := range options[1:] {
			kv := strings.SplitN(o, "=", 2)
			var k, v string
			if len(kv) == 2 {
				k, v = kv[0], kv[1]
			} else {
				k, v = o, "true"
			}
			switch k {
			case "laddr":
				c.Laddr = v
			case "timeout":
				to, err := time.ParseDuration(v)
				if err != nil {
					return nil, err
				}
				c.Timeout = to
			default:
				return nil, errors.New("Unknown option: " + k)
			}
		}
		// Remove protocol part
		pd = pd[1:]
	}
	// Parse database part of URI
	dup := strings.SplitN(pd[0], "/", 3)
	if len(dup) != 3 {
		// Wrong database part of URI
		return c, nil
	}

	c.Db = dup[0]
	c.User = dup[1]
	c.Password = dup[2]
	return c, nil
}

// Driver returns the underlying Driver of the Connector,
// mainly to maintain compatibility with the Driver method
// on sql.DB.
func (s *Server) Driver() driver.Driver {
	return s.driver
}

func (s *Server) openingConn() {
	s.Lock()
	defer s.Unlock()
	s.connCount++
}

func (s *Server) closingConn() {
	s.Lock()
	defer s.Unlock()
	s.connCount--
}
