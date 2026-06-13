# ADR-0007: Inner- vs outer-join propagation of dangling foreign keys

**Status:** Accepted
**Date:** 2026-06-13

---

## Context

The fake client validates relation *shape* but not FK *targets*
([ADR-0002](./0002-fake-client-lax-fk-validation.md)), mirroring real Notion's lazy
reference semantics: a relation property may hold a page ID that points at no existing page
(a **dangling reference**). `Select.join()` resolves the right side in phase 2 by looking up
those IDs, so it must define what happens to a left row whose relation resolves to **zero**
right rows — and that answer differs between inner and outer joins (PRD #302; slices 4–6,
#306–#308).

This is distinct from the *row-shape* decision ([ADR-0006](./0006-join-result-row-shape.md))
and from the *predicate-against-a-phantom* decision
([ADR-0005](./0005-outer-join-phantom-null-semantics.md)); this ADR fixes only the
**cardinality contract** — which left rows survive.

## Decision

**An inner join drops a left row with no matching right row; an outer join preserves it as a
single None-filled phantom.**

- **Inner join (`.join`).** A left row whose relation targets no existing right page is
  **silently dropped** — no error, no placeholder. A dangling reference and a genuinely
  empty relation are indistinguishable at the cardinality level, and both yield "no pair."
  This matches ADR-0002: invalid IDs surface as empty results, not failures.

- **Outer join (`.outerjoin`).** The same left row **survives** as one phantom row: left
  columns carry real values, every right column is `None`
  (`merge_inner_join_rows` / `_project_join_row`, `isouter=True`). The phantom is fabricated
  client-side after the merge — it never existed in the right database.

- **One pair per resolved right row.** A left row whose relation resolves to *N* existing
  right pages emits *N* rows (inner or outer); the outer-join phantom appears only when
  *N = 0*.

## Alternatives Considered

**A. Raise on a dangling reference.**
Rejected. It contradicts ADR-0002's lax-FK contract and real Notion (which accepts any
UUID-shaped string at write time), and would make test/prod behaviour diverge on
out-of-order inserts.

**B. Outer join fabricates a typed empty right row instead of None-fill.**
Rejected here and in [ADR-0005](./0005-outer-join-phantom-null-semantics.md): None-fill +
SQL NULL semantics is type-independent, whereas per-type Notion-empty values invent shapes
Notion never has and answer the wrong question.

## Consequences

- The inner-join drop is **invisible** unless a test asserts on join cardinality — dangling
  references do not announce themselves. Tests that care must assert row counts explicitly
  (ADR-0002 already flagged this).
- **SQL anti-join is inexpressible.** The classic `LEFT JOIN … WHERE right IS NULL` cannot
  be written: Notion has no `IS NULL`, `is_empty` ≠ `IS NULL`, and any right-side `WHERE`
  drops the phantom (ADR-0005). Only a bare `outerjoin()` (no right-side predicate) preserves
  unmatched left rows. This is a deliberate v1 boundary, not a bug.
- **Multi-join chaining is not yet supported.** Join execution consumes only the first join
  (`_joins[0]`); chaining `.join()/.outerjoin()` silently drops the rest. Out of scope for
  v1 — a multi-join slice (with a loud guard or a real fan-out) is needed before chaining is
  safe.
- **Self-referential joins are untested.** FK auto-detect (slice 4 / #306) does not forbid a
  table joining itself, but that path is unexercised and out of scope until a dedicated
  slice covers it.
