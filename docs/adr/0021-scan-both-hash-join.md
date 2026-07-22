# ADR-0021: Scan-both hash join — the right side is a full data-source scan, not a retrieve-by-id

**Status:** Accepted — supersedes the *Scan-vs-Retrieve* and *batched index nested-loop* decisions in
[ADR-0018](./0018-query-plan-operator-tree.md)
**Date:** 2026-07-22
**Applies:** [ADR-0020](./0020-execution-layering-sql-builds-engine-drives.md) (layering) ·
relates to [ADR-0019](./0019-sql-null-semantics-pushdown-soundness.md) (pushdown soundness)

---

## Context

[ADR-0018](./0018-query-plan-operator-tree.md) decided that a plan has **exactly one `Scan`** (the
left `data_sources.query`) and that **every right side is a `Retrieve`** — a bulk `pages.retrieve` by
`object_id` — on the premise that *"normlite joins through a `Relation` FK, so the right side is
always reached by page ID, and Notion offers no `id IN (...)` filter that could turn it into a
query."* It named the algorithm a **batched index nested-loop join** and characterised one bulk
retrieve as **"one round-trip."** #364 made the operator tree live and shipped `Retrieve` with its
intrinsic lax-FK errorhandler, an `execute_with` binding step, and an unbound-leaf guard.

Two facts force a re-examination:

1. **"One round-trip" is false at the transport.** `Cursor.executemany` (`dbapi2.py:797`) **loops**
   and calls the client **once per parameter set**; `pages.retrieve` is a single-page endpoint
   (`GET /pages/{id}`). So a "batched" retrieve of **D** distinct ids is **D HTTP round trips**, not
   one. ADR-0018's "one round-trip" holds only at the *DBAPI* layer; it hides D calls at the wire.

2. **The right side can be a query after all** — not filtered *by id* (Notion still has no
   `id IN (...)`), but a **full `data_sources.query` scan** of the right data source, **matched
   client-side** by `left.relation[i].id == right.object_id`. The merge already builds exactly the
   probe structure this needs: `right_by_oid = {object_id: right_row}` (`dml.py:1204`).

**Cost model** (H = HTTP round trips, the real currency; D = distinct referenced oids, R = right
data-source size):

| Access path | Round trips |
|---|---|
| `Retrieve` (retrieve-by-id) | **D** |
| Scan-both (`data_sources.query` full right scan) | **⌈R/100⌉** |

Scan-both wins whenever `D > ⌈R/100⌉` — i.e. whenever the left references more than ~1% of the right
table. For normlite's typically small-to-medium data sources that is the **common** case. `Retrieve`
wins only for a *selective* join against a *large* right table. This is the textbook **index
nested-loop vs hash join** trade-off; with no statistics, the planner must commit to one.

## Decision

**The right leaf of a join is a full `data_sources.query` `Scan`; the join matches client-side
against a hash of the right rows by `object_id`. A plan has no `Retrieve` — both leaves are `Scan`s.**

- **`HashJoin` becomes a symmetric drain-both hash join.** Both leaves are **independent** (the right
  scan needs nothing from the left), opened normally in `open()`, drained (the Join **blocks**), then
  merged by the existing per-left-row **array-containment probe loop** (`_merge_rows`). The name
  `HashJoin` stops lying — it is now an actual hash join (build the right by `object_id`, probe with
  the left's relation array).

- **Three complications dissolve**, all traceable to the right leaf no longer depending on the left:
  - **Lax-FK errorhandler → a hash miss.** A dangling id is simply absent from the full right scan,
    so `right_by_oid.get(dangling)` is `None` → the row is dropped (inner) or None-filled (outer).
    The `object_not_found` path and `Retrieve._lax_retrieve_errorhandler` (ADR-0002 machinery) are
    **deleted**; the "absent reference" semantics fall out for free.
  - **`execute_with` lifecycle inversion → gone.** The right `Scan` opens and executes normally.
  - **`prepare` / oid dedup → gone.** There is no `pages.retrieve` batch to build.

- **`JoinExecution` is deleted.** With `prepare` gone, its only survivor is the merge
  (`_merge_rows` / `_project_join_row` / `_right_side_passes`), which folds into `HashJoin`. This
  **absorbs #376** ("dissolve `JoinExecution` into `HashJoin`") — the class existed for the two-phase
  `prepare`/`assemble` split, and that split is what this ADR removes.

- **Behaviour-preserving.** Right-side **WHERE** and **ORDER BY** stay **client-side residuals** (the
  `Filter` and `Sort` operators); the right scan pulls the whole right table. Semantics do not move,
  so the existing join suite stays a true oracle.

## Alternatives Considered

**A. Keep `Retrieve` as the default (ADR-0018 status quo).** Rejected as the default: it costs D
round trips where the common case is cheaper as one `⌈R/100⌉` scan, and it carries three complications
(errorhandler, lifecycle inversion, prepare) that scan-both retires. **Not deleted from the design
space** — revived as a **deferred, Planner-chosen strategy** once a per-data-source **row-count
statistic** exists in the sys-catalog: D is known exactly *after the left drain* (the Join blocks), so
the planner needs only R to choose `D > ⌈R/100⌉` per query.

**B. Explode/unnest the left relation to a cardinality-1 scalar key**, making the join a textbook
scalar equi-join (`relation[0].id == right.object_id`). Deferred, bundled with **multi-join
composition** (its motivating use). The existing array-containment loop already handles cardinality
0/1/N uniformly at one site, including the outer-join phantom for an empty/all-dangling relation
(`if not matched and isouter`, `dml.py:1227`); an `Unnest` operator would re-introduce cardinality-0
as a special case for no behavioural gain today.

**C. Push right-side WHERE / ORDER BY / projection into the right `data_sources.query`.** Deferred. It
is a **strict optimization** (correctness-neutral: not pushing pulls the whole right table and filters
client-side, exactly as today) with real soundness structure — pushing the right WHERE is sound only
for **inner** joins (an **outer** join's phantom must survive, [ADR-0019](./0019-sql-null-semantics-pushdown-soundness.md)),
so it is conditional, not a one-liner. It belongs with the ADR-0019 raw-cell evaluator work (#365),
not bolted onto the access-method swap. Projection (`filter_properties` on the right) is deferred with
it, accepting that the first cut fetches whole right pages.

**D. Keep `JoinExecution` as a merge-only seam** called by a rewritten `HashJoin`. Rejected — the
class's reason for being (the two-phase dispatch split) is gone once `prepare` is deleted; a gutted
seam a rewritten operator still calls is an awkward intermediate.

## Consequences

- **`Retrieve`, `_lax_retrieve_errorhandler`, `execute_with`, the unbound-leaf guard, `prepare`, and
  `JoinExecution` are all deleted** — part of #364 intentionally unwound now that the driving seam
  exists. #364 was not wasted: it made the tree live and proved it drives end-to-end, which is the
  foundation this stands on.
- **The cost profile flips**: cheap for small/medium right tables, worse for a selective join against
  a large one. Accepted; revisited only when Alternative A's statistic lands.
- **Outer-join phantom semantics are unchanged.** Dangling ids, an empty relation, and an all-dangling
  relation all become hash misses, and the merge loop's single `isouter` site still governs None-fill.
  [[project-outer-join-empty-relation]] and the `... or []` guard are untouched.
- **Both leaves could open/drain in either order** (or concurrently) — the plan no longer encodes a
  left-before-right ordering. A future **bidirectional-relation semijoin**
  ([[project-bidirectional-semijoin-pushdown]]) would re-introduce a *left-informed* right query as a
  **separate opt-in strategy**, not a change to this one.
- **ADR-0018 is amended, not contradicted in spirit.** Its operator tree, `Planner`/`PlanningContext`,
  blocking-operator model, and engine-drives layering all stand. What changes is one premise — that
  the right side can only be reached by id — and therefore the `Retrieve`-per-right-side decision and
  the "batched index nested-loop" name.
