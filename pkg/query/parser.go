package query

import (
	"fmt"
	"iter"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/jpappel/atlas/pkg/util"
)

type catType int

const (
	CAT_UNKNOWN catType = iota
	CAT_TITLE
	CAT_AUTHOR
	CAT_DATE
	CAT_FILETIME
	CAT_TAGS
	CAT_LINKS
	CAT_META
)

type opType int

const (
	OP_UNKNOWN opType = iota
	OP_EQ             // equal
	OP_AP             // approximate/fuzzy
	OP_NE             // not equal
	OP_LT             // less than
	OP_LE             // less than or equal
	OP_GE             // greater than or equal
	OP_GT             // greater than
	OP_PIPE           // external pipe
	OP_ARG            // external arg
)

type clauseOperator int16

const (
	COP_UNKNOWN clauseOperator = iota
	COP_AND
	COP_OR
)

type Statement struct {
	Negated  bool
	Category catType
	Operator opType
	Value    Valuer
}

type Statements []Statement

type Clause struct {
	Statements Statements
	Clauses    []*Clause
	Operator   clauseOperator
}

type valuerType int

const (
	VAL_NOOP valuerType = iota
	VAL_STR
	VAL_DATETIME
)

// TODO: rename
type Valuer interface {
	// TODO: define
	Type() valuerType
	Compare(Valuer) int
}

var _ Valuer = StringValue{}
var _ Valuer = DatetimeValue{}

type StringValue struct {
	S string
}

func (v StringValue) Type() valuerType {
	return VAL_STR
}

func (v StringValue) Compare(other Valuer) int {
	o, ok := other.(StringValue)
	if !ok {
		return 0
	}

	if v.S < o.S {
		return -1
	} else if v.S > o.S {
		return 1
	} else {
		return 0
	}
}

type DatetimeValue struct {
	D time.Time
}

func (v DatetimeValue) Type() valuerType {
	return VAL_DATETIME
}

func (v DatetimeValue) Compare(other Valuer) int {
	o, ok := other.(DatetimeValue)
	if !ok {
		return 0
	}

	return v.D.Compare(o.D)
}

var _ Valuer = StringValue{}
var _ Valuer = DatetimeValue{}

func (t catType) String() string {
	switch t {
	case CAT_TITLE:
		return "Title"
	case CAT_AUTHOR:
		return "Author"
	case CAT_DATE:
		return "Date"
	case CAT_FILETIME:
		return "Filetime"
	case CAT_TAGS:
		return "Tags"
	case CAT_LINKS:
		return "Links"
	case CAT_META:
		return "Metadata"
	default:
		return "Invalid"
	}
}

func (t opType) String() string {
	switch t {
	case OP_EQ:
		return "Equal"
	case OP_AP:
		return "Approximate"
	case OP_NE:
		return "Not Equal"
	case OP_LT:
		return "Less Than"
	case OP_LE:
		return "Less Than or Equal"
	case OP_GE:
		return "Greater Than or Equal"
	case OP_GT:
		return "Greater Than"
	case OP_PIPE:
		return "Pipe External Command"
	case OP_ARG:
		return "Argument External Command"
	default:
		return "Invalid"
	}
}

// convert a token to a category
func tokToCat(t queryTokenType) catType {
	switch t {
	case TOK_CAT_TITLE:
		return CAT_TITLE
	case TOK_CAT_AUTHOR:
		return CAT_AUTHOR
	case TOK_CAT_DATE:
		return CAT_DATE
	case TOK_CAT_FILETIME:
		return CAT_FILETIME
	case TOK_CAT_TAGS:
		return CAT_TAGS
	case TOK_CAT_LINKS:
		return CAT_LINKS
	case TOK_CAT_META:
		return CAT_META
	default:
		return CAT_UNKNOWN
	}
}

// convert a token to a operation
func tokToOp(t queryTokenType) opType {
	switch t {
	case TOK_OP_EQ:
		return OP_EQ
	case TOK_OP_AP:
		return OP_AP
	case TOK_OP_NE:
		return OP_NE
	case TOK_OP_LT:
		return OP_LT
	case TOK_OP_LE:
		return OP_LE
	case TOK_OP_GE:
		return OP_GE
	case TOK_OP_GT:
		return OP_GT
	case TOK_OP_PIPE:
		return OP_PIPE
	case TOK_OP_ARG:
		return OP_ARG
	default:
		return OP_UNKNOWN
	}
}

func (c Clause) String() string {
	b := &strings.Builder{}
	c.buildString(b, 0)
	return b.String()
}

// Merge child clauses with their parents when applicable
func (root *Clause) Flatten() {
	stack := make([]*Clause, 0, len(root.Clauses))
	stack = append(stack, root)
	for len(stack) != 0 {
		top := len(stack) - 1
		node := stack[top]
		stack = stack[:top]

		hasMerged := false

		// merge if only child clause
		if len(node.Statements) == 0 && len(node.Clauses) == 1 {
			child := node.Clauses[0]

			node.Operator = child.Operator
			node.Statements = child.Statements
			node.Clauses = child.Clauses
		}

		// cannot be "modernized", node.Clauses is modified in loop
		for i := 0; i < len(node.Clauses); i++ {
			child := node.Clauses[i]

			// merge because of commutativity
			if node.Operator == child.Operator {
				hasMerged = true
				node.Statements = append(node.Statements, child.Statements...)
				node.Clauses = append(node.Clauses, child.Clauses...)
			} else {
				stack = append(stack, child)
			}
		}

		if hasMerged {
			numChildren := len(stack) - top
			if numChildren > 0 {
				node.Clauses = slices.Grow(node.Clauses, numChildren)
				node.Clauses = node.Clauses[:numChildren]
				copy(node.Clauses, stack[top:top+numChildren])
			} else {
				node.Clauses = nil
			}
		}
	}
}

func (c Clause) buildString(b *strings.Builder, level int) {
	writeIndent(b, level)
	b.WriteByte('(')
	if c.Operator == COP_AND {
		b.WriteString("and")
	} else if c.Operator == COP_OR {
		b.WriteString("or")
	} else {
		b.WriteString("unknown_op")
	}
	b.WriteByte('\n')

	for _, stmt := range c.Statements {
		writeIndent(b, level+1)
		b.WriteString(fmt.Sprintf("%+v", stmt))
		b.WriteByte('\n')
	}

	for _, clause := range c.Clauses {
		clause.buildString(b, level+1)
	}

	b.WriteByte('\n')
	writeIndent(b, level)
	b.WriteString(")\n")
}

// Depth of tree via recursion
func (root Clause) Depth() int {
	maxHeight := 1
	for _, child := range root.Clauses {
		maxHeight = max(maxHeight, child.Depth()+1)
	}
	return maxHeight
}

// Order of clause tree
func (root Clause) Order() int {
	count := 0
	for range root.DFS() {
		count++
	}
	return count
}

func (root *Clause) DFS() iter.Seq[*Clause] {
	return func(yield func(*Clause) bool) {
		stack := make([]*Clause, 0, len(root.Clauses))
		stack = append(stack, root)

		for len(stack) != 0 {
			node := stack[len(stack)-1]

			if !yield(node) {
				return
			}

			stack := stack[:len(stack)-1]
			stack = append(stack, node.Clauses...)
		}
	}
}

func (root *Clause) BFS() iter.Seq[*Clause] {
	return func(yield func(*Clause) bool) {
		queue := make([]*Clause, 64*len(root.Clauses))
		queue = append(queue, root)
		read := 0
		write := 1
		size := 1

		// FIXME: can potentially discard values if queue is too small
		for size != 0 {
			node := queue[read]

			if !yield(node) {
				return
			}
			read = (read + 1) % len(queue)
			size -= 1

			for _, child := range node.Clauses {
				queue[write] = child
				write = (write + 1) % len(queue)
				size += 1
			}
		}
	}
}

func Parse(tokens []Token) (*Clause, error) {

	stack := make([]*Clause, 0, 10)
	// NOTE: might be wrong for handling of intital frame
	stack = append(stack, &Clause{})

	var prevToken Token
	for i, token := range tokens {
		clause := stack[len(stack)-1]
		if i != 0 {
			prevToken = tokens[i-1]
		}

		switch token.Type {
		case TOK_CLAUSE_START:
			newClause := &Clause{}
			stack = append(stack, newClause)
		case TOK_CLAUSE_END:
			parentClause := stack[len(stack)-2]
			parentClause.Clauses = append(parentClause.Clauses, clause)
			stack = stack[:len(stack)-1]
		case TOK_CLAUSE_AND:
			if prevToken.Type != TOK_CLAUSE_START {
				return nil, &TokenError{
					got:      token,
					gotPrev:  prevToken,
					wantPrev: "TOK_CLAUSE_START",
				}
			}
			clause.Operator = COP_AND
		case TOK_CLAUSE_OR:
			if prevToken.Type != TOK_CLAUSE_START {
				return nil, &TokenError{
					got:      token,
					gotPrev:  prevToken,
					wantPrev: "TOK_CLAUSE_START",
				}
			}
			clause.Operator = COP_OR
		case TOK_OP_NEG:
			if !prevToken.Type.Any(TOK_CLAUSE_OR, TOK_CLAUSE_AND, TOK_VAL_STR, TOK_VAL_DATETIME) {
				return nil, &TokenError{
					got:      token,
					gotPrev:  prevToken,
					wantPrev: "clause operator or value",
				}
			}

			stmt := Statement{Negated: true}
			clause.Statements = append(clause.Statements, stmt)
		case TOK_CAT_TITLE, TOK_CAT_AUTHOR, TOK_CAT_DATE, TOK_CAT_FILETIME, TOK_CAT_TAGS, TOK_CAT_LINKS, TOK_CAT_META:
			if !prevToken.Type.Any(TOK_CLAUSE_OR, TOK_CLAUSE_AND, TOK_VAL_STR, TOK_VAL_DATETIME, TOK_OP_NEG, TOK_CLAUSE_END) {
				return nil, &TokenError{
					got:      token,
					gotPrev:  prevToken,
					wantPrev: "clause operator, value, TOK_OP_NEG, or TOK_CLAUSE_END",
				}
			}

			if prevToken.Type == TOK_OP_NEG {
				clause.Statements[len(clause.Statements)-1].Category = tokToCat(token.Type)
			} else {
				stmt := Statement{Category: tokToCat(token.Type)}
				clause.Statements = append(clause.Statements, stmt)
			}
		case TOK_OP_EQ, TOK_OP_AP, TOK_OP_NE, TOK_OP_LT, TOK_OP_LE, TOK_OP_GE, TOK_OP_GT, TOK_OP_PIPE, TOK_OP_ARG:
			if !prevToken.Type.isCategory() {
				return nil, &TokenError{
					got:      token,
					gotPrev:  prevToken,
					wantPrev: "category",
				}
			}

			clause.Statements[len(clause.Statements)-1].Operator = tokToOp(token.Type)
		case TOK_VAL_STR:
			if !prevToken.Type.isOperation() {
				return nil, &TokenError{
					got:      token,
					gotPrev:  prevToken,
					wantPrev: "operation",
				}
			}

			clause.Statements[len(clause.Statements)-1].Value = StringValue{token.Value}
		case TOK_VAL_DATETIME:
			if !prevToken.Type.isOperation() {
				return nil, &TokenError{
					got:      token,
					gotPrev:  prevToken,
					wantPrev: "operation",
				}
			}

			var t time.Time
			var err error
			if t, err = util.ParseDateTime(token.Value); err != nil {
				return nil, ErrDatetimeTokenParse
			}

			clause.Statements[len(clause.Statements)-1].Value = DatetimeValue{t}
		default:
			fmt.Fprintln(os.Stderr, token)
			return nil, &TokenError{
				got:     token,
				gotPrev: prevToken,
			}
		}

	}

	return stack[0].Clauses[0], nil
}
