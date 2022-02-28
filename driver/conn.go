package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"

	"github.com/ncrypthic/inmem/engine/log"
	"github.com/ncrypthic/inmem/engine/protocol"
)

// Conn implements sql/driver Conn interface
type Conn struct {
	// Mutex is locked when a Statement is created
	// then released on Statement.Exec or Statement.Query
	mutex sync.Mutex

	// Socket is the network connection to RamSQL engine
	conn protocol.DriverConn
	// socket net.Conn

	// This conn belongs to this server
	parent *Server
}

func newConn(conn protocol.DriverConn, parent *Server) driver.Conn {
	parent.openingConn()
	return &Conn{conn: conn, parent: parent}
}

// Prepare returns a prepared statement, bound to this connection.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {

	stmt := prepareStatement(c, query)

	return stmt, nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (c *Conn) Close() error {
	log.Debug("Conn.Close")
	c.conn.Close()

	if c.parent != nil {
		c.parent.closingConn()
	}

	return nil
}

// Begin starts and returns a new transaction.
func (c *Conn) Begin() (driver.Tx, error) {
	log.Info("ramsql: %s", "begin transaction")
	return NewTransaction(c), nil
}

// BeginTx starts and returns a new transaction.
// If the context is canceled by the user the sql package will
// call Tx.Rollback before discarding and closing the connection.
//
// This must check opts.Isolation to determine if there is a set
// isolation level. If the driver does not support a non-default
// level and one is set or if there is a non-default isolation level
// that is not supported, an error must be returned.
//
// This must also check opts.ReadOnly to determine if the read-only
// value is true to either set the read-only transaction property if supported
// or return an error if it is not supported.
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if sql.IsolationLevel(opts.Isolation) != sql.LevelDefault {
		return nil, errors.New("isolation level is yet not supported")
	}
	return c.Begin()
}

// PrepareContext returns a prepared statement, bound to this connection.
// context is for the preparation of the statement,
// it must not store the context within the statement itself.
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.Prepare(query)
}
