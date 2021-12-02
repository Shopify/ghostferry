package sqlparser

import (
	"bytes"

	"github.com/blastrain/vitess-sqlparser/tidbparser/ast"
)

func convertFromCreateTableStmt(stmt *ast.CreateTableStmt, ddl *DDL) Statement {
	columns := []*ColumnDef{}
	for _, col := range stmt.Cols {
		options := []*ColumnOption{}
		for _, option := range col.Options {
			expr := ""
			if option.Expr != nil {
				var buf bytes.Buffer
				option.Expr.Format(&buf)
				expr = buf.String()
			}
			options = append(options, &ColumnOption{
				Type:  ColumnOptionType(option.Tp),
				Value: expr,
			})
		}
		columns = append(columns, &ColumnDef{
			Name:    col.Name.Name.String(),
			Type:    col.Tp.String(),
			Elems:   col.Tp.Elems,
			Options: options,
		})
	}
	constraints := []*Constraint{}
	for _, constraint := range stmt.Constraints {
		keys := []ColIdent{}
		for _, key := range constraint.Keys {
			keys = append(keys, NewColIdent(key.Column.Name.String()))
		}
		constraints = append(constraints, &Constraint{
			Type: ConstraintType(constraint.Tp),
			Name: constraint.Name,
			Keys: keys,
		})
	}
	options := []*TableOption{}
	for _, option := range stmt.Options {
		options = append(options, &TableOption{
			Type:      TableOptionType(option.Tp),
			StrValue:  option.StrValue,
			UintValue: option.UintValue,
		})
	}
	return &CreateTable{
		DDL:         ddl,
		Columns:     columns,
		Constraints: constraints,
		Options:     options,
	}
}

func convertFromTruncateTableStmt(stmt *ast.TruncateTableStmt) Statement {
	return &TruncateTable{Table: TableName{Name: TableIdent{v: stmt.Table.Name.String()}}}
}

func convertTiDBStmtToVitessOtherAdmin(stmts []ast.StmtNode, admin *OtherAdmin) Statement {
	for _, stmt := range stmts {
		switch adminStmt := stmt.(type) {
		case *ast.TruncateTableStmt:
			return convertFromTruncateTableStmt(adminStmt)
		default:
			return admin
		}
	}
	return nil
}

func convertTiDBStmtToVitessDDL(stmts []ast.StmtNode, ddl *DDL) Statement {
	for _, stmt := range stmts {
		switch ddlStmt := stmt.(type) {
		case *ast.CreateTableStmt:
			return convertFromCreateTableStmt(ddlStmt, ddl)
		default:
			return ddl
		}
	}
	return nil
}

func convertTiDBStmtToVitessShow(stmts []ast.StmtNode, show *Show) Statement {
	for _, stmt := range stmts {
		switch showStmt := stmt.(type) {
		case *ast.ShowStmt:
			return &Show{TableName: showStmt.Table.Name.String()}
		default:
			return show
		}
	}
	return nil
}
