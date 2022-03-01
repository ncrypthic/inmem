package engine

import (
	"testing"

	"github.com/ncrypthic/inmem/engine/log"
	"github.com/ncrypthic/inmem/engine/parser"
	"github.com/ncrypthic/inmem/engine/protocol"
)

type TestEngineConn struct {
}

func (conn *TestEngineConn) ReadStatement() (string, error) {
	return "", nil
}

func (conn *TestEngineConn) WriteResult(lastInsertedID int64, rowsAffected int64) error {
	return nil
}

func (conn *TestEngineConn) WriteError(err error) error {
	return nil
}

func (conn *TestEngineConn) WriteRowHeader(header []string) error {
	return nil
}

func (conn *TestEngineConn) WriteRow(row []string) error {
	return nil
}

func (conn *TestEngineConn) WriteRowEnd() error {
	return nil
}

func testEngine(t *testing.T) *Engine {
	_, engineEndpoint := protocol.NewChannelEndpoints()
	e, err := New(engineEndpoint)
	if err != nil {
		t.Fatalf("Cannot create new engine: %s", err)
	}

	return e
}

func TestNewEngine(t *testing.T) {
	e := testEngine(t)
	e.Stop()
}

func TestCreateDatabase(t *testing.T) {
	log.UseTestLogger(t)
	query := `CREATE DATABASE sample`

	e := testEngine(t)
	defer e.Stop()

	i, err := parser.ParseInstruction(query)
	if err != nil {
		t.Fatalf("Cannot parse query %s : %s", query, err)
	}

	err = e.executeQuery(i[0], &TestEngineConn{})
	if err != nil {
		t.Fatalf("Cannot execute query: %s", err)
	}
	query2 := `CREATE DATABASE sample IF NOT EXISTS`

	i, err = parser.ParseInstruction(query2)
	if err != nil {
		t.Fatalf("Cannot parse query %s : %s", query2, err)
	}

	err = e.executeQuery(i[0], &TestEngineConn{})
	if err != nil {
		t.Fatalf("Cannot execute query: %s", err)
	}
}
