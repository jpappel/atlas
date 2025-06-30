package query

import (
	"fmt"
	"strings"

	"github.com/jpappel/atlas/pkg/util"
)

const MAX_CLAUSE_DEPTH int = 16

type CompilationArtifact struct {
	Query string
	Args  []any
}

func (art CompilationArtifact) String() string {
	b := strings.Builder{}
	fmt.Fprintln(&b, art.Query)
	b.WriteByte('[')
	for i, arg := range art.Args {
		if i != len(art.Args)-1 {
			fmt.Fprintf(&b, "`%s`, ", arg)
		} else {
			fmt.Fprintf(&b, "`%s`", arg)
		}
	}
	b.WriteByte(']')
	return b.String()
}

func (s Statements) buildCompile(b *strings.Builder, delim string) ([]any, error) {
	var args []any

	sCount := 0
	for cat, catStmts := range s.CategoryPartition() {
		// TODO: make sure sorted
		// TODO: loop over partitions
		if len(catStmts) == 0 {
			continue
		}
		var catStr string
		switch cat {
		case CAT_PATH:
			catStr = "path "
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
			} else if cat.IsSet() && op == OP_AP {
				b.WriteString("( ")
				idx := 0
				for _, stmt := range opStmts {
					b.WriteString(catStr)
					b.WriteString(opStr)
					arg, ok := stmt.Value.buildCompile(b)
					if ok {
						args = append(args, "%"+arg+"%")
					}
					if idx != len(opStmts)-1 {
						b.WriteString(" OR ")
					}
					sCount++
					idx++
				}
				b.WriteString(" ) ")
			} else if cat.IsOrdered() && op == OP_AP {
				idx := 0
				for _, stmt := range opStmts {
					b.WriteString(catStr)
					d, ok := stmt.Value.(DatetimeValue)
					if !ok {
						panic("type corruption, expected DatetimeValue")
					}

					start, end := util.FuzzDatetime(d.D)

					if stmt.Negated {
						b.WriteString("NOT ")
					}
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
					if stmt.Negated {
						b.WriteString("NOT ")
					}
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

func (root Clause) Compile() (CompilationArtifact, error) {
	if d := root.Depth(); d > MAX_CLAUSE_DEPTH {
		return CompilationArtifact{}, &CompileError{
			fmt.Sprintf("exceeded maximum clause depth: %d > %d", d, MAX_CLAUSE_DEPTH),
		}
	}

	b := strings.Builder{}
	args, err := root.buildCompile(&b)
	if err != nil {
		return CompilationArtifact{}, err
	} else if b.Len() == 0 {
		return CompilationArtifact{}, fmt.Errorf("Empty query")
	}
	return CompilationArtifact{b.String(), args}, nil
}

func (c Clause) buildCompile(b *strings.Builder) ([]any, error) {
	isRoot := b.Len() == 0
	if !isRoot {
		b.WriteString("( ")
	}

	var delim string
	switch c.Operator {
	case COP_AND:
		delim = "AND"
	case COP_OR:
		delim = "OR"
	default:
		return nil, &CompileError{fmt.Sprint("invalid clause operator ", c.Operator)}
	}

	args, err := c.Statements.buildCompile(b, delim)
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

	if !isRoot {
		b.WriteString(") ")
	}

	return args, nil
}
