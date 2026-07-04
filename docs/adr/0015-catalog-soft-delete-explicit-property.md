# ADR-0015: Catalog soft-delete via an explicit `is_dropped` property

**Status:** Accepted
**Date:** 2026-07-04

---

## Context

normlite models a table's lifecycle (`TableState`) from **two independent facts**:

1. **normlite's recorded intent** — whether the SQL user dropped the table. Until now this was
   encoded by **trashing the catalog row's Notion page** (`pages.update {in_trash: True}` on the
   `tables` row), read back as `SystemTablesEntry.is_dropped = page["in_trash"]`.
2. **Notion's physical reality** — whether the table's actual database is trashed. `DROP TABLE`
   trashes the *database container* (`databases.update {in_trash: True}`), read via
   `databases.retrieve(table_id)`.

`get_table_state` cross-checks the two: both dropped → `DROPPED` (a recoverable soft-delete); both
live → `ACTIVE`; **disagreement → `ORPHANED`**. The disagreement case is the whole reason two
independent flags exist — it detects a database trashed out-of-band (e.g. in the Notion UI) or a
catalog row whose intent no longer matches reality.

[ADR-0014](0014-data-source-two-id-identity.md) moved all row queries onto `data_sources.query`,
which — unlike the old `databases.query` — has **no `in_trash` parameter and unconditionally skips
trashed pages**. This silently invalidated fact #1's encoding: a soft-deleted catalog row is a
*trashed page*, so it is now **invisible to the only query surface the catalog has**.
Consequences:

- `find_sys_tables_row` returns `None` for a dropped table, so `get_table_state` can never observe
  `is_dropped = True` → `DROPPED` is undetectable (it mis-reports `ORPHANED`).
- `RESTORE` (`set_dropped(dropped=False)`) must first *find* the already-dropped row to un-drop it;
  it now finds nothing and wrongly raises "does not exist".

The soft-delete marker must survive queries. Native Notion page-trash cannot.

## Decision

**Represent the catalog-row soft-delete marker as an explicit, queryable `is_dropped` checkbox
property on the `tables` row. The catalog page is never trashed.**

- `set_dropped` / `set_dropped_by_page_id` flip the property
  (`pages.update {"properties": {"is_dropped": {"checkbox": <bool>}}}`) instead of trashing the
  page. The catalog row stays **live** and therefore visible to `data_sources.query`.
- `SystemTablesEntry.is_dropped` reads the checkbox property rather than `page["in_trash"]`.
- The **two-flag state model is preserved.** `is_dropped` (intent) stays independent of the
  database's trash state (reality); `get_table_state` keeps deriving `ACTIVE` / `DROPPED` /
  `ORPHANED` from the pair. `ORPHANED` remains detectable.
- The **physical `DROP TABLE` is unchanged** — it still trashes the *database container* via
  `databases.update {in_trash}`, which is not a `data_sources.query` surface and is read back by
  `databases.retrieve`.
- **Target `tables` row schema** (final order): `table_name` (title), `table_schema`,
  `table_catalog`, `table_id`, **`data_source_id`** (rich_text, per ADR-0014), **`is_dropped`**
  (checkbox, this ADR). `data_source_id` persistence lands in its own slice; this ADR adds only
  `is_dropped`.

## Considered Options

- **Collapse to one source of truth — derive `DROPPED` purely from the database's trash state**
  (delete the catalog-row marker). Rejected: it merges `DROPPED` and `ORPHANED`, making
  intent/reality disagreement — the entire point of `ORPHANED` — inexpressible.
- **Keep page-trash, add an "include trashed" query path.** Rejected: `data_sources.query` has no
  `in_trash` by ADR-0014's deliberate design; reintroducing one fights that decision and the real
  Notion endpoint it mirrors. Same dead end already hit by `find_sys_tables_row_by_table_id`.
- **Encode the marker as rich_text `"true"/"false"`.** Rejected: stringly-typed and needs parsing;
  a checkbox carries boolean semantics natively and is already filterable/readable in the fake.

## Consequences

- Persisted `FileBasedNotionClient` stores predating this change will not carry `is_dropped`; this
  rides the same **clean-break** guard as ADR-0014 (pre-1.0, no migrator).
- Any test asserting the *old* mechanism (`page["in_trash"] is True` after `set_dropped`) must move
  to asserting the checkbox. Two ORPHANED tests that previously passed *accidentally* (via the
  "entry is None" branch) now pass through the intended intent-vs-reality mismatch branch.
- **CREATE-after-DROP is orchestrated at the engine layer, not this primitive.** With the row now
  staying live, a naive dropped-row fall-through in `get_or_create_sys_tables_row` would create a
  *second live* row with the same name+catalog (previously the original was page-trashed and
  hidden), which the next `find_sys_tables_row` trips as a `len > 1` `InternalError`. The fix is
  **not** to restore inside `get_or_create`. Per
  [ADR-0016](0016-dropped-table-is-non-existent.md), a dropped table is non-existent to DDL:
  `Table.create` reads `get_table_state`, and on `DROPPED` **repurposes** the single leftover row
  onto a fresh database (overwriting `table_id` + `data_source_id`, clearing `is_dropped`) rather
  than restoring or duplicating. In the real flow `get_or_create` is therefore never reached with a
  dropped row; it is hardened only **defensively** — a dropped row still *exists*, so it is treated
  as existing (returned under `if_not_exists`, else `already exists`) rather than duplicated. The
  soft-delete machinery this ADR introduces is retained (a latent foundation for a possible future
  restore) but is surfaced by no DDL verb; only the diagnostic `get_table_state` still distinguishes
  `DROPPED`.
