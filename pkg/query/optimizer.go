package query

import (
	"slices"
)

type Optimizer struct{}

func StatementCmp(a Statement, b Statement) int {
	catDiff := int(a.Category - b.Category)
	opDiff := int(a.Operator - b.Operator)
	negatedDiff := 0
	if a.Negated && !b.Negated {
		negatedDiff = 1
	} else if !a.Negated && b.Negated {
		negatedDiff = -1
	}

	return catDiff*100_000 + opDiff*100 + negatedDiff*10 + a.Value.Compare(b.Value)
}

func StatementEq(a Statement, b Statement) bool {
	a.Simplify()
	b.Simplify()
	return a.Category == b.Category && a.Operator == b.Operator && a.Negated == b.Negated && a.Value.Compare(b.Value) == 0
}

// Merge child clauses with their parents when applicable
func (o Optimizer) Flatten(root *Clause) {
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

func (o Optimizer) Compact(c *Clause) {
	for clause := range c.DFS() {
		clause.Statements = slices.CompactFunc(c.Statements, StatementEq)
	}
}

// if any claus is a strict equality/inequality noop all fuzzy operations
func strictEquality(clause Clause) error {
	isStrict := slices.ContainsFunc(clause.Statements, func(stmt Statement) bool {
		if stmt.Operator == OP_EQ || stmt.Operator == OP_NE {
			return true
		}
		return false
	})

	if isStrict {
		for i := range clause.Statements {
			stmt := clause.Statements[i]
			if stmt.Operator != OP_EQ && stmt.Operator != OP_NE {
				clause.Statements[i] = Statement{}
			}
		}
	}

	return nil
}
