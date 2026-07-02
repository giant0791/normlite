# ADR-0011: Aggregate queries — the `AggregateExecution` seam and the provenance-free synthetic row

**Status:** Accepted
**Date:** 2026-06-29

---

## Context

normlite is adding SQL-style cross-row aggregates (`func.sum`, `func.avg`, `func.count`) to
`select()`, e.g.
`select(func.sum(Employee.salary).label("total_payroll"), func.avg(Employee.salary))`.

Two facts about the target backend shape the design:

1. **There is no Notion backend for a cross-row aggregate.** Notion *formula properties* are
   per-row scalar expressions (spreadsheet-style), never cross-row reductions. Notion's only
   native reductions are **rollups** (over a relation, anchored to a parent row) and **view
   calculations** (column-footer sum/avg), and view calculations are **not exposed by the
   public API**. So an aggregate over a plain database must be computed **client-side**, by
   folding over the pages returned by `databases.query`. Mapping `func` onto Notion formula
   property types is a non-goal.

2. **Aggregation is single-phase.** Unlike `Select.join()` — whose `JoinExecution` seam
   ([ADR-0008](./0008-joinexecution-seam.md)) is *stateful only because* it must survive the
   EXECUTEMANY dispatch boundary (build batch → engine fires `pages.retrieve` → assemble) —
   an aggregate is just `databases.query` drain-all followed by a reduction. There is no
   dispatch boundary to span.

v1 is deliberately minimal: **whole-set, all-aggregate, no `GROUP BY`**. The projection must
be all aggregates; the query returns **exactly one row**. That row is **synthetic** — it is
computed by normlite and is **not backed by any Notion page**, so it has no `object_id` and
no per-property provenance. This punctures the [ADR-0009](./0009-result-schema-provenance.md)
invariant that every `ResultColumn` traces back to an owning `Table`.

## Decision

**Own the reduction in a dedicated, single-phase `AggregateExecution` seam, and allow the
result row to be provenance-free.**

```
AggregateExecution(projection)
    .result_schema            # SchemaInfo whose ResultColumns carry no owning Table
    .reduce(drained_rows) -> (SchemaInfo, [synthetic_row])
```

- **Config to the constructor, data to the method** — same discipline as `JoinExecution`,
  but **one synchronous call** (`reduce`) instead of a `prepare`/`assemble` pair, because
  there is no dispatch boundary. Constructed and called in `Select._finalize_execution` once
  the result set has drained.
- **The aggregate `ResultColumn` carries no `Table`.** ADR-0009's `table` provenance field
  becomes optional. This is a *uniform* hole, not a ragged one: in the all-aggregate v1
  projection **every** column in the row is synthetic, so no row ever mixes page-derived and
  synthetic columns. (Mixed-provenance rows only arise with `GROUP BY`, which is deferred.)
- **SQL-faithful semantics.** Empties (`None`) are skipped within the set (`avg` divides by
  the non-empty count); `sum`/`avg` over zero rows return `None`, `count` returns `0`. The
  synthetic row may hold Python `None` even though Notion has no NULL
  ([ADR-0005](./0005-outer-join-phantom-null-semantics.md)) precisely because it is not a
  Notion page.
- **Aggregation forces drain-all** regardless of `stream_results`/`yield_per` — you cannot
  reduce a half-consumed stream — consistent with joins and two-phase mutations
  ([ADR-0010](./0010-streaming-result-token-pagination.md)).
- **Result-key naming: auto function-name, `.label()` override, ordinal disambiguation on
  collision.** Each aggregate column is keyed by its function name (`sum`, `avg`, `count`) unless
  the user supplies `.label(name)`, which wins. Because an aggregate is provenance-free, a name
  collision **cannot** be resolved by table-qualification the way `from_join` qualifies
  `courses.title` ([ADR-0009](./0009-result-schema-provenance.md)) — there is no owning `Table`
  to qualify with. Instead, collisions are disambiguated **positionally**, SQLAlchemy-style: a
  key that is unique in the projection stays **bare** (`sum`), but when a key appears more than
  once, **every** member of that group takes a 1-based ordinal suffix (`func.sum(x), func.sum(y)`
  → `sum_1, sum_2` — not `sum, sum_1`). This mirrors `from_join`'s "disambiguate only collisions,
  but suffix every member of the colliding group" rule; only the disambiguation *token* differs
  (ordinal suffix vs. table qualifier), because aggregates lack provenance.

## Considered Options

- **Inline free functions in `_finalize_execution`/`CursorResult`.** Rejected: this recreates
  exactly the "logic smeared across five free functions with no owner" situation ADR-0008 was
  written to eliminate.
- **Reuse/extend `JoinExecution`.** Rejected: couples merge-two-pages (two-phase, has a right
  side and a dispatch boundary) with reduce-N-to-1 (single-phase, single table). A forced
  abstraction in both directions.
- **Forbid the provenance hole (sentinel `Table` or required provenance).** Rejected: an
  aggregate genuinely has no source table; a sentinel would lie to the ADR-0009 machinery.
  Optional provenance is the honest model.

## Consequences

- `Select._projection` widens from "tuple of `Column`" to "tuple of `Column` / aggregate /
  `Label`"; `ResultColumn.table` becomes `Optional[Table]`.
- **`avg` returns a dedicated `Float` type with its own DBAPI type code.** `avg` is true
  division, so its result is a Python `float`, not the operand type (`sum`, by contrast,
  *preserves* the operand type). A `float` — an inexact statistical ratio — is deliberately
  distinct from `Numeric`/`Money`, which mean an exact `Decimal`. Making this stick end-to-end
  forced a subtlety: the result cursor reconstructs a column's `result_processor` **from its
  DBAPI type code**, not from the declared `TypeEngine` (`engine/row.py`: `type_mapper[col_type]`).
  `Float` originally shared `Numeric`'s `NUMBER_WITH_COMMAS` code, so an averaged value was
  silently re-materialised as a `Decimal` — the declared `Float` was never consulted. The fix is
  a **distinct `DBAPITypeCode.NUMBER_FLOAT`** that `Float` owns and `type_mapper` routes back to
  `Float`, so a `float` survives the round-trip. General rule this establishes: *a type whose
  runtime Python value must differ from its siblings needs its own DBAPI type code — the type
  code, not the `TypeEngine` instance, is what the result path keys on.*
- **The reduce fold resolves operands positionally, and that position is coupled to the
  compiler.** Since aggregate result keys may collide or be relabelled, `reduce` maps each
  function to its operand by the **order-preserving-deduped list of operand column names**, which
  must match the drained-row layout the compiler produces (`fetch_columns` →
  `SchemaInfo.from_table` → `_merge_names` dedup). Two aggregates over the *same* column resolve
  to one shared cell; over *different* columns, to distinct cells. This coupling is load-bearing:
  any change to how aggregate `fetch_columns` are projected must move `AggregateExecution.reduce`
  in lockstep. A cleaner future design would hand the seam the execution description rather than
  re-deriving the layout.
- `.order_by()` and `.join()`/`.outerjoin()` combined with an aggregate projection raise
  loudly (`ArgumentError`/`CompileError`) in v1 — out of scope, never a silent drop.
- **Skip-NULL keys on the *inner* cell, and the operand key ≠ the return key.** `_process_page`
  emits a present-but-null number property as `{"number": None}` (a truthy dict), not a bare
  `None`, so the reduce filter must skip on `c.get(operand_typ) is not None`, not merely
  `c is not None` — otherwise `sum`/`avg` crash on `int + None` and `count(col)` over-counts.
  Separately, `_compute_func_result` must read operand cells with the **operand column's** type
  key (`func.column.type_.get_col_spec()`) but key the result cell with the **return** type
  (`func.type_.get_col_spec()`). These coincide (`"number"`) for `sum`/`avg`, which hid the
  conflation, but diverge for `count`, whose return type is always `Integer` while its operand
  may be any type — a single shared key silently returns `0` for `count` over a non-number column.
- **`COUNT(*)` is a columnless `func.count()`; its FROM is anchored explicitly via
  `select_from`.** With no operand column there is nothing to infer the table from, so
  construction leaves `Select._table` `None` and `select(func.count()).select_from(t)` supplies
  it. `reduce` returns `len(rows)` for a columnless count (every matched row, empties included),
  and the compiler falls back to fetching `object_id` (the always-present system column, the
  default `fetch_columns`) so each page still yields one row to count. `select_from` is
  **aggregate-only** (guarded with `ArgumentError` on non-aggregate selects) to avoid silently
  unlocking an untested general explicit-FROM / join path — deferred to a dedicated slice.
- `GROUP BY` is the natural next slice. It reintroduces **mixed-provenance rows** (a grouping
  key column beside synthetic aggregate columns), the bare-column validation rule, and
  `HAVING`. When it lands it should extend `AggregateExecution` with a partitioning step
  (bucket drained rows by key, reduce per bucket) — at which point the single `reduce` call
  may grow, but the seam's ownership boundary stays put.
