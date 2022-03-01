package engine

import (
	"fmt"

	"github.com/ncrypthic/inmem/engine/parser"
	"github.com/ncrypthic/inmem/engine/protocol"
)

func dropExecutor(e *Engine, dropDecl *parser.Decl, conn protocol.EngineConn) error {
	isDatabase := dropDecl.Decl[0].Token == parser.DatabaseToken
	isTable := dropDecl.Decl[0].Token == parser.TableToken
	isDropable := isDatabase || isTable
	// Should have table token
	if dropDecl.Decl == nil ||
		len(dropDecl.Decl) != 1 ||
		!isDropable ||
		len(dropDecl.Decl[0].Decl) != 1 {
		return fmt.Errorf("unexpected drop arguments")
	}
	if isDatabase {
		return conn.WriteResult(0, 1)
	}

	table := dropDecl.Decl[0].Decl[0].Lexeme

	r := e.relation(table)
	if r == nil {
		return fmt.Errorf("relation '%s' not found", table)
	}

	e.drop(table)

	return conn.WriteResult(0, 1)
}
