# ADR-0004: Expression hierarchy — `ModifierExpression` stays a `ClauseElement` sibling, not a `ColumnElement`

**Status:** Accepted
**Date:** 2026-06-01

---

## Context

Slice 5 of `Select.join()` (#307) introduced source-table *routing*: a WHERE or ORDER BY
expression that touches only the left (driving) table rides into the phase-1
`databases.query` payload (`filter` / `sorts`); anything touching the right table is held
out of phase-1 and answered client-side after the join.

Both routing sites share one helper in `compiler.py`:

```python
def _get_expression_parent_tables(expression: ClauseElement) -> Set[Table]:
    # recursive fold: Column leaf -> {parent}; Binary/Unary/BooleanClauseList recurse;
    # OrderByClause/OrderByExpression recurse.
```

The fold returns the **set** of tables an expression touches, and each site routes on
`parent_tables == {select._table}`.

While extending the fold from WHERE to ORDER BY, an asymmetry in the expression hierarchy
surfaced (`src/normlite/sql/elements.py`):

- `ColumnElement(ClauseElement)` is the base for column expressions:
  `UnaryExpression`, `BinaryExpression`, `BooleanClauseList`, `BindParameter`. It defines
  an **operator interface** — `__and__`/`__or__` (build `BooleanClauseList`), `__invert__`
  (build `UnaryExpression`), `__bool__` (raise), plus `name`, `type_`, `primary_key`, and
  the comparator protocol.
- `ModifierExpression(ClauseElement)` is a **sibling** of `ColumnElement`, not a subclass.
  `OrderByExpression(ModifierExpression)` wraps a `(column, direction)` pair.

Because one fold now spans both families, its only common supertype is `ClauseElement`, so
the parameter annotation had to widen from `ColumnElement` to `ClauseElement`. This prompted
a proposal to "fix" the asymmetry by making `ModifierExpression` derive from `ColumnElement`
so the annotation could narrow back.

## Decision

**`ModifierExpression` remains a `ClauseElement` sibling of `ColumnElement`. We do not
re-parent it under `ColumnElement`, and we do not add a `parent` attribute to
`ColumnElement`.**

Two principles drive this:

1. **A statement modifier is not a column expression.** `ColumnElement` is an interface,
   not a tag. Re-parenting `ModifierExpression` under it would hand every ORDER BY node
   `__and__`/`__or__`/`__invert__`/`__bool__`, a comparator protocol, `type_`, and `name` —
   none of which an ORDER BY node can honor (`courses.c.title.asc() & ...` is nonsense).
   That is a Liskov/ISP violation: widening the *interface* to narrow a *type annotation*.
   The one-word annotation (`ClauseElement`) is honest about the surface; the inheritance
   would be a lie.

2. **`ColumnElement` correctly has no `parent`.** A non-leaf expression such as
   `students.c.name == "x" AND courses.c.title == "y"` does not *have* a parent table — it
   touches a **set** of them. That is exactly why the router returns `Set[Table]`. Only the
   `Column` leaf has a single well-defined `parent`. Adding `parent` to `ColumnElement` would
   force a single-table answer for multi-table nodes — reintroducing the very mis-routing the
   slice exists to prevent. The recursion that walks down to `Column` leaves is the natural
   shape of folding a tree-with-many-leaves into a set, not a workaround for a missing field.
   The `OrderByExpression -> .column` hop is likewise intrinsic: the wrapper carries the
   sort direction and cannot be collapsed without losing it.

The real (minor) smell is that **one fold straddles two node families**. The preferred
remedy — deferred to a dedicated refactor slice, not bolted onto #307 — is to keep the fold
narrow (`_get_expression_parent_tables(expr: ColumnElement)`, column expressions only) and
**unwrap `OrderByExpression.column` at the ORDER BY routing call site** before folding. The
modifier-specific knowledge ("an ORDER BY clause is a list of `(column, direction)` pairs")
then lives at the call site where it belongs, the annotation returns to `ColumnElement`, and
no class hierarchy changes.

## Alternatives Considered

**A. Re-parent `ModifierExpression` under `ColumnElement`.**
Rejected. Cures the annotation symptom by giving modifiers a column-expression interface they
cannot satisfy — a strictly worse smell (LSP/ISP violation) than a widened type hint.

**B. Add a `parent` attribute to `ColumnElement`.**
Rejected. Ambiguous/undefined for any multi-table node; would force the router to pick one
table and silently mis-route compound predicates.

**C. Call-site unwrap, keep the fold on `ColumnElement` (preferred, deferred).**
Accepted in principle, deferred in time. Keeps the fold pure and column-only, restores the
tighter annotation, and changes no hierarchy. Out of scope for #307; consistent with the
project's "prefer existing patterns, defer refactors to dedicated slices" discipline.

## Consequences

- For now, `_get_expression_parent_tables` is annotated `ClauseElement` and contains
  `OrderByClause`/`OrderByExpression` branches alongside the column-expression branches.
  This is accepted interim coupling, not the target shape.
- The expression hierarchy keeps a clean conceptual split: `ColumnElement` = things that
  participate in column-level boolean/comparison algebra; `ModifierExpression` = statement
  modifiers that *carry* columns. New modifier kinds (e.g. LIMIT/OFFSET-style nodes) extend
  `ModifierExpression`, not `ColumnElement`.
- When the call-site-unwrap refactor (Alternative C) is scheduled, it must remove the
  `OrderByClause`/`OrderByExpression` branches from the fold and re-narrow the annotation;
  the two routing sites stay symmetric ("collect tables from the columns, route on
  `== {left}`").
- Unrecognized nodes still contribute no tables; the top-level guard in `visit_select`
  raises `CompileError` only for a fully-unattributable expression. A new node type that is
  routable must extend the fold (or its call-site unwrap) or it routes silently.
