# ADR-0006: Join result row-shape — flat rows, projection narrowing, qualified names on collision

**Status:** Accepted
**Date:** 2026-06-13

---

## Context

`Select.join(...)` / `Select.outerjoin(...)` produce a result set that spans **two**
tables. Three shape questions had to be settled before the result-set contract could be
called stable (PRD #302, user stories 7 & 8; slice 7 / #309):

1. **Structure** — does a joined row nest the two sides (`{left: {...}, right: {...}}`) or
   flatten them into one tuple?
2. **Projection** — when the user names a subset of columns
   (`select(students.c.name, courses.c.title)`), does the result still carry every property
   of both tables, or only the named ones?
3. **Naming collisions** — when both tables expose a column of the same name
   (`students.name`, `instructors.name`), how is each surfaced through `keys()` /
   `mappings()` without one silently shadowing the other?

These decisions feed the existing `Row` metadata machinery (`SchemaInfo`, `keys()`,
`mappings()`), which until the join work assumed a single source table.

## Decision

**A joined row is a flat tuple of result columns in deterministic projection order.
Projection narrows both the result set and the phase-1 payload. Colliding names are
qualified — but only the colliding ones.**

- **Flat rows.** A merged row is `(left user cols…, right user cols…)` in projection order,
  not a nested structure. The left side precedes the right side; both follow the order in
  which their columns appear in the projection (`SchemaInfo.from_join`, `_project_join_row`
  in `sql/dml.py`). This keeps `Row` positional access and `mappings()` uniform with the
  single-table case.

- **Projection narrowing.** `select(students.c.name, courses.c.title)` projects **only** the
  named columns. The narrowing is also pushed to the phase-1 `databases.query` via
  `filter_properties`, so the wire payload carries only the projected left-side **properties**
  — with two non-negotiable carve-outs:
  - `object_id` is **not** a property and must never go in `filter_properties` (the
    in-memory client rejects it as `NotionError`); it rides in `execution_names` instead.
  - the join **onclause** column (the relation FK) is an *execution requirement* and must
    survive **inside** `filter_properties` even when the user did not project it, because
    phase-2 needs it to resolve the right side. These two pull in opposite directions and
    must not be conflated.

- **Qualified names on collision — only the colliding ones.** `SchemaInfo.from_join` groups
  the projected entities by bare name; any name that appears more than once in the
  projection is qualified as `"<parent-table>.<name>"` for **every** entity that carries it
  (so both `students.name` and `instructors.name` surface, neither shadows the other).
  Names that do not collide keep their bare form. A plain single-table `select(students)` is
  untouched — qualification fires only inside a join with an actual collision. Qualification
  uses the table's public `name`, never an internal/execution name.

## Alternatives Considered

**A. Nested rows (`row.left.name`, `row.right.title`).**
Rejected. It forks the `Row`/`mappings()` contract between single-table and join results,
breaks positional access, and pushes disambiguation onto every consumer. Flat rows with
conditional qualification keep one row model.

**B. Always qualify every column in a join.**
Rejected. It regresses the single-table-shaped contract for joins with no collision
(`select(students, courses)` would suddenly key `students.name` / `courses.title`), breaking
existing `keys()` expectations for no benefit. Qualification is a collision-resolution tool,
not a default.

**C. Project all properties regardless of the column list.**
Rejected. It defeats the point of a narrow projection (story 7), pulls every property over
the wire, and makes `filter_properties` dead weight.

## Consequences

- `SchemaInfo` is the single source of truth for the joined result shape: ordering,
  narrowing, and qualified keys all flow from it. `keys()` and `mappings()` surface the
  qualified keys on collision with no shadowing.
- The merged-column order is deterministic (projection order, left before right), so
  repeated runs and `mappings()` lookups are stable.
- **Known limitation — right-side `WHERE` on a colliding column is unsupported in v1.** The
  post-merge right-side filter looks its getters up by the bare right-table column name,
  while a colliding right column is keyed fully-qualified in the joined schema — so a
  `WHERE` on that exact column is silently ignored, or (if it is the only projected right
  column) drops every row via the all-None phantom guard. Workaround: qualify or rename.
  A real fix must look the getter up by the joined (qualified) name and needs its own slice
  with a red test. See issue #309 / #310 discussion. The all-None phantom-drop rule
  ([ADR-0005](./0005-outer-join-phantom-null-semantics.md)) is unaffected.
- Filtering on a **non-projected** right column is likewise unreconstructable from the merged
  tuple and is out of scope here (it needs evaluation against the raw phase-2 page).
- `SchemaInfo.from_join` does not yet compose `from_table`; that consolidation is deferred.
