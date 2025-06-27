package query

import (
	"fmt"
	"strings"

	"github.com/jpappel/atlas/pkg/util"
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

func (s Statements) Compile(b *strings.Builder, delim string) ([]string, error) {
	var args []string

	sCount := 0
	for cat, catStmts := range s.CategoryPartition() {
		// TODO: make sure sorted
		// TODO: loop over partitions
		if len(catStmts) == 0 {
			continue
		}
		var catStr string
		switch cat {
		case CAT_AUTHOR:
			catStr = "author "
		case CAT_DATE:
			catStr = "date "
		case CAT_FILETIME:
			catStr = "fileTime "
		case CAT_LINKS:
			catStr = "link "
		case CAT_META:
			catStr = "meta "
		case CAT_TAGS:
			catStr = "tag "
		case CAT_TITLE:
			catStr = "title "
		default:
			return nil, &CompileError{
				fmt.Sprintf("unexpected query.catType %#v", cat),
			}
		}

		for op, opStmts := range catStmts.OperatorPartition() {
			if len(opStmts) == 0 {
				continue
			}
			var opStr string
			switch op {
			case OP_AP:
				if cat.IsOrdered() {
					opStr = "BETWEEN "
				} else {
					opStr = "LIKE "
				}
			case OP_EQ:
				if cat.IsSet() {
					opStr = "IN "
				} else {
					opStr = "= "
				}
			case OP_GE:
				// NOTE: doesn't raise compiler error if operator used on invalid category
				opStr = ">= "
			case OP_GT:
				// NOTE: doesn't raise compiler error if operator used on invalid category
				opStr = "> "
			case OP_LE:
				// NOTE: doesn't raise compiler error if operator used on invalid category
				opStr = "<= "
			case OP_LT:
				// NOTE: doesn't raise compiler error if operator used on invalid category
				opStr = "< "
			case OP_NE:
				if cat.IsSet() {
					opStr = "NOT IN "
				} else {
					opStr = "!= "
				}
			case OP_PIPE:
				opStr = "?op_pipe "
			case OP_ARG:
				opStr = "?op_arg "
			default:
				return nil, &CompileError{
					fmt.Sprintf("unexpected query.opType %#v", op),
				}
			}

			if cat.IsSet() && op != OP_AP {
				b.WriteString(catStr)
				b.WriteString(opStr)
				b.WriteByte('(')
				idx := 0
				for _, stmt := range opStmts {
					arg, ok := stmt.Value.buildCompile(b)
					if ok {
						args = append(args, arg)
					}
					if idx != len(opStmts)-1 {
						b.WriteByte(',')
					}
					sCount++
					idx++
				}
				b.WriteString(") ")
			} else if cat.IsOrdered() && op == OP_AP {
				idx := 0
				for _, stmt := range opStmts {
					b.WriteString(catStr)
					d, ok := stmt.Value.(DatetimeValue)
					if !ok {
						panic("type corruption, expected DatetimeValue")
					}

					start, end := util.FuzzDatetime(d.D)

					b.WriteString(opStr)
					fmt.Fprint(b, start.Unix(), " ")
					b.WriteString("AND ")
					fmt.Fprint(b, end.Unix(), " ")
					if idx != len(opStmts)-1 {
						b.WriteString(delim)
						b.WriteByte(' ')
					}
					idx++
					sCount++
				}
			} else {
				idx := 0
				for _, stmt := range opStmts {
					b.WriteString(catStr)
					b.WriteString(opStr)
					arg, ok := stmt.Value.buildCompile(b)
					if ok {
						if op == OP_AP {
							args = append(args, "%"+arg+"%")
						} else {
							args = append(args, arg)
						}
					}
					b.WriteByte(' ')
					if idx != len(opStmts)-1 {
						b.WriteString(delim)
						b.WriteByte(' ')
					}
					idx++
					sCount++
				}
			}

			if sCount != len(s) {
				b.WriteString(delim)
				b.WriteByte(' ')
			}
		}
	}

	return args, nil
}

func (root Clause) Compile() (string, []string, error) {
	if d := root.Depth(); d > MAX_CLAUSE_DEPTH {
		return "", nil, &CompileError{
			fmt.Sprintf("exceeded maximum clause depth: %d > %d", d, MAX_CLAUSE_DEPTH),
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
	switch c.Operator {
	case COP_AND:
		delim = "AND"
	case COP_OR:
		delim = "OR"
	default:
		return nil, &CompileError{fmt.Sprint("invalid clause operator ", c.Operator)}
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
