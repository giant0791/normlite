# ADR-0012: CheckConstraint — client-side enforcement and DBAPI `IntegrityError`

**Status:** Accepted
**Date:** 2026-07-01

---

## Context

normlite is adding a `CheckConstraint` construct that carries a boolean predicate over a
`Table`'s columns and an optional name:

```python
price = Column("price", Number()); discount = Column("discount", Number())
products = Table("products", metadata, price, discount,
                 CheckConstraint(price > 0,        name="check_positive_price"),
                 CheckConstraint(discount < price, name="check_discount_rules"))
```

One fact about the backend shapes everything: **Notion's `databases.create`/`update` API has
no CHECK facility.** A check cannot be serialized into the Notion database schema and Notion
will never reject a violating page on normlite's behalf. This is the same shape aggregates hit
([ADR-0011](./0011-aggregate-execution-seam.md)): a SQL concept with **no Notion backend**.

"Row-level" (`price > 0`) vs "table-level" (`discount < price`) is not a type distinction in
normlite — both are a single boolean predicate over one table's columns, evaluated per-row.
There is one construct.

## Decision

**Enforce checks client-side, in a dedicated `CheckEnforcement` seam, and raise DBAPI
`IntegrityError` on violation.** Declarative-only / no-op semantics were rejected: a
`CheckConstraint` that never rejects anything is a footgun.

- **Client-side enforcement, `Insert`-only in v1.** `CheckEnforcement` is built from the target
  table's `CheckConstraint`s (mirroring the [ADR-0011](./0011-aggregate-execution-seam.md) /
  [ADR-0008](./0008-joinexecution-seam.md) single-owner discipline) and evaluates each row
  **before** the `pages.create` call(s). It checks the **Python values pre-bind**
  (`Decimal("5")`, `-2`), keyed by column name — not the Notion JSON cells — so no cell
  decoding is needed on the insert path. A multi-row (`INSERTMANYVALUES`) insert checks every
  row and fails fast on the first violation (no partial commit). `Delete` never enforces.
  `Update` is **deferred**: its partial `.values()` would require merging new values over the
  decoded existing page image, which is substantial and orthogonal — its own later slice.
- **A new `Operator`-enum-driven evaluator.** A small tree-walker (owned beside
  `CheckEnforcement`) walks the predicate — `BinaryExpression`, `BooleanClauseList` (and/or),
  `UnaryExpression` (not) — resolving `Column` operands to `row[col.name]` and `BindParameter`
  operands to their value, dispatching on the backend-agnostic `Operator` enum. Compound
  predicates are supported in v1. The fake client's `_Filter`/`_Condition` is **not** reused:
  it lives in `notion_sdk/client.py`, speaks the Notion raw-cell shape and Notion op strings,
  and has no col-col support — reusing it from the sql layer would be a layering inversion.
- **SQL-faithful three-valued logic.** A check **rejects a row only when the predicate is
  `False`**. A `None` operand (Notion has no NULL — an empty property is Python `None`,
  [ADR-0005](./0005-outer-join-phantom-null-semantics.md)) makes the comparison **UNKNOWN →
  accepted**, exactly like SQL `CHECK` and the aggregate skip-NULL discipline. "Value must be
  present" is a NOT-NULL concern, not a CHECK.
- **DBAPI `IntegrityError(DatabaseError)`.** A violation raises a new `IntegrityError` in
  `notiondbapi/dbapi2.py` — the DBAPI-2.0-canonical error for "relational integrity affected;
  a constraint check failed" (also what SQLAlchemy surfaces). The message carries the
  constraint `name` (or the predicate repr when unnamed) and the offending column/value.
- **Column-to-column comparisons in the AST.** `discount < price` is a col-col comparison.
  `Comparator.operate` today calls `coerce_to_bindparam(other, ...)`, which (a `Column` is not
  callable) wraps the RHS `Column` as a literal `BindParameter.value` — silent garbage. Fix:
  `operate` **preserves a `ColumnElement` RHS as-is**, so `BinaryExpression.value` becomes
  "`BindParameter` (literal) **or** `ColumnElement` (column ref)". A col-col comparison is
  **not expressible as a Notion `databases.query` filter** (Notion compares a property to a
  literal, never to another property), so `visit_binary_expression` **raises loudly** if it
  ever sees a `ColumnElement` value — a col-col predicate handed to `.where()` fails fast.
- **Structural, fail-fast validation at table adoption** (not type policing). When the table
  adopts the constraint (`Table.__init__` after columns bind; `add_constraint` for the post-hoc
  path), two invariants raise `ArgumentError`: every referenced `Column` must belong (by
  identity) to the attaching table, and the argument must be a boolean-valued expression (not a
  bare `Column`/literal). No operand-type policing in v1 — any `Operator` the evaluator can
  compute is accepted (mirroring the type-agnostic `func.count(col)`).

## Considered Options

- **Declarative-only / no-op** — store the check as metadata, never enforce. Rejected: a
  constraint that never rejects is a footgun and honesty about Notion's limits is better served
  by client-side enforcement that actually means something.
- **Reject `CheckConstraint` as unsupported at construction.** Rejected: the check *can* be
  enforced client-side, exactly as aggregates are computed client-side.
- **Raise a `NormliteError` subclass instead of DBAPI `IntegrityError`.** Genuinely tempting,
  because `CheckEnforcement` runs client-side, **above the DBAPI boundary** — raising a
  DBAPI-layer error from there is a deliberate layering inversion. Rejected because the
  *meaning* is exactly DBAPI `IntegrityError`, and matching the DBAPI/SQLAlchemy surface
  (callers `except IntegrityError`/`except Error`) is worth the inversion.
- **Reuse the fake client's `_Filter` evaluator.** Rejected: wrong layer, wrong value language
  (Notion cells vs Python pre-bind values), no col-col support.

## Consequences

- A **client-side seam raises a DBAPI-layer error** — an accepted, documented layering
  inversion. Callers catch `IntegrityError`/`Error` even though no Notion call was made.
- `BinaryExpression.value` is now polymorphic (literal `BindParameter` **or** `ColumnElement`);
  every consumer must handle both, and the WHERE→filter compiler guards against the col-col case
  (touches the [ADR-0004](./0004-expression-hierarchy-modifier-vs-column.md) hierarchy).
- `Update` against a checked table is **unenforced** until the deferred merge slice lands — a
  documented hole, not a silent one.
- The reflection round-trip (persisting checks in the catalog so they survive `autoload_with`)
  is a separate decision — see [ADR-0013](./0013-checkconstraint-catalog-persistence.md).
