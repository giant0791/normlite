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
- `.order_by()` and `.join()`/`.outerjoin()` combined with an aggregate projection raise
  loudly (`ArgumentError`/`CompileError`) in v1 — out of scope, never a silent drop.
- `GROUP BY` is the natural next slice. It reintroduces **mixed-provenance rows** (a grouping
  key column beside synthetic aggregate columns), the bare-column validation rule, and
  `HAVING`. When it lands it should extend `AggregateExecution` with a partitioning step
  (bucket drained rows by key, reduce per bucket) — at which point the single `reduce` call
  may grow, but the seam's ownership boundary stays put.
