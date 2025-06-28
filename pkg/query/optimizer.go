package query

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/jpappel/atlas/pkg/util"
)

type Optimizer struct {
	workers  uint
	root     *Clause
	isSorted bool // current sort state of statement for all clauses
}

func StatementCmp(a Statement, b Statement) int {
	catDiff := int(a.Category - b.Category)
	opDiff := int(a.Operator - b.Operator)
	negatedDiff := 0
	if a.Negated && !b.Negated {
		negatedDiff = 1
	} else if !a.Negated && b.Negated {
		negatedDiff = -1
	}

	var valDiff int
	if a.Value != nil && b.Value != nil {
		valDiff = a.Value.Compare(b.Value)
	}

	return catDiff*100_000 + opDiff*100 + negatedDiff*10 + valDiff
}

func StatementEq(a Statement, b Statement) bool {
	a.Simplify()
	b.Simplify()
	return a.Category == b.Category && a.Operator == b.Operator && a.Negated == b.Negated && a.Value.Compare(b.Value) == 0
}

func NewOptimizer(root *Clause, workers uint) Optimizer {
	return Optimizer{
		root:    root,
		workers: workers,
	}
}

// Optimize clause according to level.
// level 0 is automatic and levels < 0 do nothing.
func (o Optimizer) Optimize(level int) {
	o.Simplify()
	if level < 0 {
		return
	} else if level == 0 {
		// TODO: determine smarter level determination strategy
		level = o.root.Depth()
	}

	oldDepth := o.root.Depth()
	for range level {
		// clause level parallel
		o.Compact()
		o.StrictEquality()
		o.Tighten()
		o.Contradictions()
		// parallel + serial
		o.Tidy()
		// purely serial
		o.Flatten()

		depth := o.root.Depth()
		if depth == oldDepth {
			break
		} else {
			oldDepth = depth
		}
	}
}

// Perform optimizations in parallel. They should **NOT** mutate the tree
func (o Optimizer) parallel(optimize func(*Clause)) {
	jobs := make(chan *Clause, o.workers)

	wg := &sync.WaitGroup{}
	wg.Add(int(o.workers))

	for range o.workers {
		go func(jobs <-chan *Clause, wg *sync.WaitGroup) {
			for clause := range jobs {
				optimize(clause)
			}
			wg.Done()
		}(jobs, wg)
	}

	for clause := range o.root.DFS() {
		jobs <- clause
	}
	close(jobs)
	wg.Wait()
}

// Perform Optimizations serially. Only use this if the tree is being modified.
// When modifying a clause set children that should not be explored to nil
func (o *Optimizer) serial(optimize func(*Clause)) {
	stack := make([]*Clause, 0, len(o.root.Clauses))
	stack = append(stack, o.root)
	for len(stack) != 0 {
		top := len(stack) - 1
		node := stack[top]
		stack = stack[:top]

		optimize(node)

		node.Clauses = slices.DeleteFunc(node.Clauses, func(child *Clause) bool {
			return child == nil
		})
		stack = append(stack, node.Clauses...)
	}
}

func (o *Optimizer) SortStatements() {
	o.parallel(func(c *Clause) {
		slices.SortFunc(c.Statements, StatementCmp)
	})
	o.isSorted = true
}

// Simplify all statements
func (o *Optimizer) Simplify() {
	o.parallel(func(c *Clause) {
		for i := range c.Statements {
			(&c.Statements[i]).Simplify()
		}
	})
}

// Merge child clauses with their parents when applicable
func (o *Optimizer) Flatten() {
	o.serial(func(node *Clause) {
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
			isSingleStmt := len(child.Clauses) == 0 && len(child.Statements) == 1
			// merge because of commutativity or leaf node with single statement
			if node.Operator == child.Operator || isSingleStmt {
				node.Statements = append(node.Statements, child.Statements...)
				node.Clauses = append(node.Clauses, child.Clauses...)
				node.Clauses[i] = nil
			}
		}
	})
}

// Remove multiples of equivalent statements within the same clause
//
// Examples
//
//	(and a="Fred Flinstone" a="Fred Flinstone") --> (and a="Fred Flinstone")
//	(or a=Shaggy -a!=Shaggy) --> (or a=Shaggy)
func (o *Optimizer) Compact() {
	o.parallel(func(c *Clause) {
		c.Statements = slices.CompactFunc(c.Statements, StatementEq)
	})
	o.isSorted = false
}

func (o *Optimizer) Tidy() {
	// ensure ordering
	if !o.isSorted {
		o.SortStatements()
	}

	marked := make(map[*Clause]bool, 0)
	markedLock := &sync.Mutex{}
	// slice away noops
	o.parallel(func(c *Clause) {
		// PERF: should be benchmarked against binary seach, likely no performance gain
		//       for typical length of Statements
		start := slices.IndexFunc(c.Statements, func(s Statement) bool {
			// NOTE: this breaks if valid categories exist between
			//       CAT_UNKNOWN + CAT_TITLE or after CAT_META
			return s.Category > CAT_UNKNOWN && s.Category <= CAT_META
		})

		// this means no valid categories in statements
		if start == -1 {
			c.Statements = nil
			markedLock.Lock()
			marked[c] = true
			markedLock.Unlock()
			return
		}

		stop := len(c.Statements)
		for i := stop; i > 0; i-- {
			// NOTE: this breaks if valid categories exist after CAT_META
			if c.Statements[i-1].Category <= CAT_META {
				stop = i
				break
			}
		}
		c.Statements = c.Statements[start:stop]
	})

	o.serial(func(c *Clause) {
		for i, child := range c.Clauses {
			if !marked[child] {
				continue
			}

			if c.Operator == COP_AND {
				c.Statements = nil
				c.Clauses = nil
				break
			} else {
				c.Clauses[i] = nil
			}
		}
	})
}

func inverseEq(s1, s2 Statement) bool {
	s1.Negated = true
	return StatementEq(s1, s2)
}

// Replace contradictions with noops
func (o *Optimizer) Contradictions() {
	if !o.isSorted {
		o.SortStatements()
	}

	o.parallel(func(c *Clause) {
		removals := make(map[int]bool, 8)
		var isContradiction func(s1, s2 Statement) bool
		for category, stmts := range c.Statements.CategoryPartition() {
			if c.Operator == COP_AND && !category.IsSet() {
				isContradiction = func(s1, s2 Statement) bool {
					return (s1.Operator == OP_EQ && s1.Operator == s2.Operator) || inverseEq(s1, s2)
				}
			} else {
				isContradiction = inverseEq
			}
			clear(removals)
			for i := range stmts {
				a := stmts[i]
				a.Negated = !a.Negated
				for j := i + 1; j < len(stmts); j++ {
					b := stmts[j]
					if isContradiction(a, b) {
						removals[i] = true
						removals[j] = true
					}
				}
			}

			for idx := range removals {
				stmts[idx] = Statement{}
			}
			if len(removals) > 0 {
				o.isSorted = false
			}
		}
	})
}

// Remove fuzzy/range based statements when possible.
// Does not remove contradictions.
//
// Examples:
//
//	(and d="May 1, 1886" d>="January 1, 1880") --> (and d="May 1, 1886")
//	(and T=notes T:"monday standup") --> (and T=notes)
//	(and T="Meeting Notes" T:notes) --> (and T="Meeting Notes")
//	(and a="Alonzo Church" a="Alan Turing" a:turing) --> (and a="Alonzo Church" a="Alan Turing")
//	(and a="Alonzo Church" a="Alan Turing" a:djikstra) --> (and a="Alonzo Church" a="Alan Turing" a:djikstra)
//	(and T=foo T=bar T:foobar) --> (and T=foo T=bar)
func (o Optimizer) StrictEquality() {
	if !o.isSorted {
		o.SortStatements()
	}
	o.parallel(func(c *Clause) {
		if c.Operator != COP_AND {
			return
		}

		stricts := make([]string, 0)
		for category, stmts := range c.Statements.CategoryPartition() {
			if category.IsSet() {
				clear(stricts)
				for i, s := range stmts {
					val := strings.ToLower(s.Value.(StringValue).S)
					switch s.Operator {
					case OP_EQ:
						stricts = append(stricts, val)
					case OP_AP:
						if slices.ContainsFunc(stricts, func(strictStr string) bool {
							return strings.Contains(strictStr, val) || strings.Contains(val, strictStr)
						}) {
							stmts[i] = Statement{}
							o.isSorted = false
						}
					}
				}
			} else {
				hasEq := false
				for i, s := range stmts {
					hasEq = hasEq || (s.Operator == OP_EQ)
					if hasEq && s.Operator != OP_EQ {
						stmts[i] = Statement{}
						o.isSorted = false
					}
				}
			}
		}
	})
}

// Shrink approximate statements and ranges
//
// Examples:
//
//	(or d>"2025-01-01 d>"2025-02-02") --> (or d>"2025-01-01")
//	(and d>"2025-01-01 d>"2025-02-02") --> (and d>"2025-02-02")
//	(or T:"Das Kapital I" T:"Das Kapital") --> (and T:"Das Kapital")
//	(and T:"Das Kapital I" T:"Das Kapital") --> (and T:"Das Kapital I")
func (o *Optimizer) Tighten() {
	if !o.isSorted {
		o.SortStatements()
	}

	o.parallel(func(c *Clause) {
		for category, stmts := range c.Statements.CategoryPartition() {
			if len(stmts) < 2 {
				continue
			}
			if c.Operator == COP_AND {
				if category.IsOrdered() {
					minLT, minLE := -1, -1
					maxGT, maxGE := -1, -1
					for i, s := range stmts {
						if s.Operator == OP_LT && minLT == -1 {
							minLT = i
						} else if s.Operator == OP_LE && minLE == -1 {
							minLE = i
						} else if s.Operator == OP_GE {
							maxGE = i
						} else if s.Operator == OP_GT {
							maxGT = i
						}
					}

					lowerIdx, upperIdx := -1, -1
					if minLT != -1 && minLE != -1 {
						ltStmt := stmts[minLT]
						leStmt := stmts[minLE]
						leDate := leStmt.Value.(DatetimeValue).D
						ltDate := ltStmt.Value.(DatetimeValue).D

						if ltDate.After(leDate) {
							upperIdx = minLE
						} else {
							upperIdx = minLT
						}
					} else if minLT != -1 {
						upperIdx = minLT
					} else if minLE != -1 {
						upperIdx = minLE
					}
					if maxGT != -1 && maxGE != -1 {
						gtStmt := stmts[maxGT]
						geStmt := stmts[maxGE]
						geDate := geStmt.Value.(DatetimeValue).D
						gtDate := gtStmt.Value.(DatetimeValue).D

						if geDate.After(gtDate) {
							lowerIdx = maxGE
						} else {
							lowerIdx = maxGT
						}
					} else if maxGT != -1 {
						lowerIdx = maxGT
					} else if maxGE != -1 {
						lowerIdx = maxGE
					}

					for i, s := range stmts {
						if !s.Operator.IsOrder() || i == lowerIdx || i == upperIdx {
							continue
						}

						stmts[i] = Statement{}
					}
				} else {
					removals := make(map[int]bool)
					for i, s1 := range util.FilterIter(stmts, func(s Statement) bool { return s.Operator == OP_AP }) {
						val1 := strings.ToLower(s1.Value.(StringValue).S)
						for j, s2 := range util.FilterIter(stmts[i+1:], func(s Statement) bool { return s.Operator == OP_AP }) {
							val2 := strings.ToLower(s2.Value.(StringValue).S)
							if strings.Contains(val2, val1) {
								removals[i] = true
							} else if strings.Contains(val1, val2) {
								removals[j] = true
							}
						}
					}
					for idx := range removals {
						stmts[idx] = Statement{}
					}
					if len(removals) > 0 {
						o.isSorted = false
					}
				}
			} else {
				if category.IsOrdered() {
					// NOTE: doesn't handle fuzzy dates
					minIdx := slices.IndexFunc(stmts, func(s Statement) bool {
						return s.Operator.IsOrder()
					})
					maxIdx := len(stmts) - 1
					for i, s := range slices.Backward(stmts) {
						if s.Operator.IsOrder() {
							maxIdx = i
							break
						}
					}
					if minIdx != -1 {
						o.isSorted = false
						start, stop := minIdx, maxIdx
						if minS := stmts[minIdx]; minS.Operator == OP_GE || minS.Operator == OP_GT {
							start++
						}
						if maxS := stmts[maxIdx]; maxS.Operator == OP_LT || maxS.Operator == OP_LE {
							stop--
						}
						for i := start; i <= stop; i++ {
							stmts[i] = Statement{}
						}
					}
				} else {
					// NOTE: this has to be all pairs for correctness,
					//       but it doesn't have to be this sloppy...... :|
					removals := make(map[int]bool)
					for i, s1 := range util.FilterIter(stmts, func(s Statement) bool { return s.Operator == OP_AP }) {
						val1 := strings.ToLower(s1.Value.(StringValue).S)
						for j, s2 := range util.FilterIter(stmts[i+1:], func(s Statement) bool { return s.Operator == OP_AP }) {
							val2 := strings.ToLower(s2.Value.(StringValue).S)
							if strings.Contains(val2, val1) {
								fmt.Fprintf(os.Stderr, "%s > %s\nRemoving %s\n", val2, val1, val2)
								// NOTE: slicing stmts offsets the all indices by 1, hence the correction
								removals[j+1] = true
							} else if strings.Contains(val1, val2) {
								fmt.Fprintf(os.Stderr, "%s > %s\nRemoving %s\n", val1, val2, val1)
								removals[i] = true
							}
						}
					}

					for idx := range removals {
						stmts[idx] = Statement{}
					}
					if len(removals) > 0 {
						o.isSorted = false
					}
				}
			}
		}
	})
}
