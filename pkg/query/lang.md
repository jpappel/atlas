# Query Language Spec

```
<expr_list> := <expr> | <expr> <expr_list>

<expr> := <statment> <bin_op> <statment>
<statment> := <statement_start> {strings} <statment_end>
<statment_start := 
<statment_end> :=

<bin_op> := "and" | "or" | "not" | "similar"
```
