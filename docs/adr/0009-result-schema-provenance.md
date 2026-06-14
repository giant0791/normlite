# ADR-0009: Result-schema provenance & explicit colliding projections

**Status:** Accepted
**Date:** 2026-06-14

---

## Context

[ADR-0006](./0006-join-result-row-shape.md) settled the join result row-shape and, on a
left/right column-name collision, qualified the colliding result columns
(`students.title`, `courses.title`) while leaving non-colliding names bare. It left one
boundary open, recorded as a known limitation: a **right-side `WHERE` on a colliding
column** is broken. Two things forced this ADR:

1. **The collision was barely reachable, and the fix touches a shared type.** An explicit
   duplicate-name projection — `select(students.c.title, courses.c.title)` — could not even
   be constructed: `Select._projection` was a `ColumnCollection`, which rejects duplicate
   *names* with `DuplicateColumnError`. The collision was reachable **only** through
   full-table expansion `select(students, courses)` (which bypasses the collection,
   `dml.py:611`). We want explicit colliding projections to be a first-class, supported
   query — which means the projection can no longer be name-keyed.

2. **The right-side filter, and two other sites, identified columns by name.** Once a
   colliding right column is keyed fully-qualified in the merged schema (`courses.title`)
   but the compiled Notion filter references the **bare** name (`visit_binary_expression`
   emits `column.name` = `title`), every name-based lookup in the join pipeline mis-fires
   under collision. The post-merge right-side filter (`JoinExecution._right_side_passes`)
   selected its getters with `c.name in right.uc` — false for `courses.title` — and keyed
   its synthetic page by the qualified name, so the bare-named filter could not find it.
   This produced **two** wrong outcomes (verified empirically): if the colliding column was
   the *only* projected right column the all-None phantom guard silently dropped **every**
   row; if another non-colliding right column was present, `_Filter` raised
   `ValueError: Property 'title' not found on page`. (ADR-0006's text called both "silently
   ignored"; only the first is silent — corrected there.)

The root cause across all of these is the same: the result schema knew a column's *name* but
not its *origin*, so it relied on name-matching, which is exactly what collisions break.

## Decision

**Give result columns explicit provenance and select by identity, not by name; and make a
two-table projection a plain ordered sequence so colliding columns can be projected
explicitly.**

- **Provenance on `ResultColumn` (always set).** Every `ResultColumn` carries two extra
  fields — `table: Table` (the owning table) and `bare_name: str` (the original,
  unqualified name) — populated by **both** `SchemaInfo.from_table` and
  `SchemaInfo.from_join`. They are `compare=False, repr=False`, so `ResultColumn` equality
  and the DBAPI `description` (`as_sequence()`) are unchanged. The public `name` field stays
  the merged key (qualified on collision). Provenance is an **invariant**, never `None`.

- **Selection by identity.** The right-side filter selects its columns with
  `col.table is self._join.right` instead of `col.name in right.uc`. The getter is taken at
  the merged (qualified) name via the existing index; the synthetic page is keyed by
  `col.bare_name` — which matches the bare name the compiled filter references. The
  synthetic page is right-only, so bare names are unambiguous *within it*.

- **`Select._projection` becomes `tuple[Column, ...]`.** All three construction paths
  (single table, two-table full expansion, explicit column list) normalize to a tuple, in
  projection order. Dropping `ColumnCollection` removes the name-dedup that rejected
  duplicate-name projections, so explicit `select(a.c.x, b.c.x)` is now constructable and
  qualified by `from_join`.

- **Same column twice is a hard error, in `Select.__init__`.** Removing the collection's
  dedup would also let the *same* `(table, name)` be projected twice — which `from_join`
  would qualify to two identical keys and silently collapse. A guard alongside the existing
  belongs-to-table safeguard rejects a repeated `(parent, name)` with `ArgumentError`, while
  allowing the same bare name from two different tables.

- **The compiler's left-fetch filter switches to identity.** `visit_select` built the
  phase-1 left fetch list with `col.name in select._table.c` (`compiler.py:636`); under an
  explicit collision the right column's name (`title`) is also present in the left table, so
  it leaked into the left query. Changed to `col.parent is select._table`, matching
  `_project_join_row` (`dml.py:1194`).

## Alternatives Considered

**A. Reconstruct `from_join`'s qualification rule inline at the getter-selection site.**
Compute `f"{right.name}.{name}"` when the bare name also appears in `left.uc`, look the
getter up by that, key the page bare. Rejected: it duplicates the qualification rule in two
places that must stay in lock-step, leaving the same name-matching fragility one refactor
away from breaking again. Provenance kills the whole class rather than patching the one site.

**B. Make the compiled Notion filter emit qualified property names on collision.** Rejected:
it would push join-projection awareness into `visit_binary_expression`, which has no join
context, to fix a problem the synthetic page can reconcile locally (right-only page → bare
names are unambiguous).

**C. Keep provenance optional / join-only (`Optional[Table] = None`, set only by
`from_join`).** Rejected: a sometimes-`None` field reintroduces the implicit contract the
provenance work exists to remove. `from_table` populating it (single owning table,
`bare_name = name`) is trivial, including for system columns, which carry harmless
provenance no reader consults.

## Consequences

- **Resolves the ADR-0006 right-side-`WHERE`-on-collision limitation.** Both sub-cases
  (silent over-drop; `ValueError`) are fixed; the all-None phantom-drop rule
  ([ADR-0005](./0005-outer-join-phantom-null-semantics.md)) is preserved — a genuine match's
  right slice is now non-`None`, an outer phantom is still all-`None`.
- **New supported query shape.** `select(students.c.title, courses.c.title)` is valid and
  yields qualified result keys; this is a user-visible API addition (see CONTEXT.md,
  "Projected column name (collision qualification)").
- **Out of scope (known boundary).** A two-table projection with **no** `.join()`
  (`select(students.c.name, courses.c.title)` without a join clause) is pre-existing,
  unchanged behavior — identical for colliding and non-colliding names — and is **not**
  addressed here. No guard requiring `.join()` is added.
- **Implementation lands TDD-first** with a RED test for the silent over-drop (genuine match
  survives), the `ValueError` sub-case, a discriminating predicate (a row that *should* drop
  still drops), and an outer-join phantom-drop regression under collision.
- **Compiler and cursor contracts otherwise untouched.** `as_sequence()` (the DBAPI
  `description`) and `ResultColumn` equality are unchanged; provenance is internal.
