package query

import (
	"fmt"
	"strings"
)

const MAX_CLAUSE_DEPTH int = 16

func (stmt Statement) Compile(b *strings.Builder) (*string, error) {
	if stmt.Negated {
		b.WriteString("NOT ")
	}

	switch stmt.Category {
	case CAT_TITLE:
		b.WriteString("title ")
	case CAT_AUTHOR:
		b.WriteString("author ")
	case CAT_DATE:
		b.WriteString("date ")
	case CAT_FILETIME:
		b.WriteString("fileTime ")
	case CAT_TAGS:
		b.WriteString("tags ")
	case CAT_LINKS:
		b.WriteString("links ")
	default:
		return nil, &CompileError{
			fmt.Sprint("unknown or invalid category ", stmt.Category.String()),
		}
	}
	switch stmt.Operator {
	case OP_EQ:
		if stmt.Category.IsSet() {
			b.WriteString("IN ")
		} else {
			b.WriteString("= ")
		}
	case OP_AP:
		b.WriteString("LIKE ")
	case OP_NE:
		b.WriteString("!= ")
	case OP_LT:
		b.WriteString("< ")
	case OP_LE:
		b.WriteString("<= ")
	case OP_GE:
		b.WriteString(">= ")
	case OP_GT:
		b.WriteString("> ")
	default:
		return nil, &CompileError{
			fmt.Sprint("unknown or invalid operand ", stmt.Operator.String()),
		}
	}

	switch stmt.Value.Type() {
	case VAL_STR:
		s, ok := stmt.Value.(StringValue)
		if !ok {
			panic(CompileError{"type corruption in string value"})
		}
		b.WriteString("(?) ")
		return &s.S, nil
	case VAL_DATETIME:
		dt, ok := stmt.Value.(DatetimeValue)
		if !ok {
			panic(CompileError{"type corruption in datetime value"})
		}
		fmt.Fprint(b, dt.D.Unix(), " ")
	default:
		return nil, &CompileError{
			fmt.Sprint("unknown or invalid value type ", stmt.Value.Type()),
		}
	}
	return nil, nil
}

func (stmts Statements) Compile(b *strings.Builder, delim string) ([]string, error) {
	var args []string

	// TODO: handle meta category
	for i, stmt := range stmts {
		if i != 0 {
			b.WriteString(delim)
		}
		b.WriteByte(' ')

		arg, err := stmt.Compile(b)
		if err != nil {
			return nil, err
		} else if arg != nil {
			args = append(args, *arg)
		}
	}

	return args, nil
}

func (root Clause) Compile() (string, []string, error) {
	if d := root.Depth(); d > MAX_CLAUSE_DEPTH {
		return "", nil, &CompileError{
			fmt.Sprint("exceeded maximum clause depth of 8: ", d),
		}
	}

	b := strings.Builder{}
	args, err := root.buildCompile(&b)
	if err != nil {
		return "", nil, err
	}
	return b.String(), args, nil
}

func (c Clause) buildCompile(b *strings.Builder) ([]string, error) {
	b.WriteString("( ")

	var delim string
	switch cop := c.Operator; cop {
	case COP_AND:
		delim = "AND"
	case COP_OR:
		delim = "OR"
	default:
		return nil, &CompileError{fmt.Sprint("invalid clause operator ", cop)}
	}

	args, err := c.Statements.Compile(b, delim)
	if err != nil {
		return nil, err
	}
	for _, clause := range c.Clauses {
		b.WriteString(delim)
		b.WriteByte(' ')

		newArgs, err := clause.buildCompile(b)
		if err != nil {
			return nil, err
		} else if newArgs != nil {
			args = append(args, newArgs...)
		}
	}
	b.WriteString(") ")

	return args, nil
}
