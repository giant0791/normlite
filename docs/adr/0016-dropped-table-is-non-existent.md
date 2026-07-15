# ADR-0016: A dropped table is non-existent to DDL — SQL-destructive `DROP`, no restore

**Status:** Accepted
**Date:** 2026-07-04

---

## Context

normlite aims to behave **as SQL-like as possible**. In standard SQL there is no soft-delete:
`DROP TABLE students` destroys the table, and a later `CREATE TABLE students` succeeds with a
fresh, empty table. There is no "the table is dropped, ask permission to bring it back" state —
that concept does not exist in a SQL user's mental model.

The current lifecycle diverged from that. `Table.create` on a `DROPPED` table **raised**
`"… is dropped. Use execution_options(restore_dropped=True) to restore it."`, and
`restore_dropped=True` revived the table. This inverted the SQL default: it made the
Notion-recoverable behavior the default and forced the SQL-normal case (just make me a new table)
to opt in through an *error message*. `Table.drop` and reflection likewise treated `DROPPED` as a
distinct, special state (`"already dropped"`, `"is dropped and cannot be reflected"`).

The lifecycle machinery itself is sound: [ADR-0015](0015-catalog-soft-delete-explicit-property.md)
records a table's state from **two independent flags** — normlite's recorded intent (`is_dropped`
checkbox on the catalog row) and Notion's physical reality (the database's trash state) — from which
`get_table_state` derives `ACTIVE` / `DROPPED` / `ORPHANED`. The question this ADR answers is not
*how state is stored* but *what the DDL verbs do when they encounter `DROPPED`*.

## Decision

**To DDL operations, a `DROPPED` table is indistinguishable from a `MISSING` (never-existed) one.
`DROP` is SQL-destructive and there is no user-facing restore.**

- **`CREATE` on `DROPPED` → a fresh table.** No raise, no restore option. Because a dropped table
  still leaves a live catalog row behind, the create **repurposes** that one row onto a brand-new
  database rather than inserting a second one: it overwrites `table_id` **and** the persisted
  `data_source_id` (per [ADR-0014](0014-data-source-two-id-identity.md)) with the new database's
  ids and clears `is_dropped`. User-visibly identical to creating a `MISSING` table; internally a
  row-reuse. `checkfirst` is irrelevant here (as it is for `MISSING`) — it only distinguishes
  skip-vs-raise for an `ACTIVE` table.
- **`DROP` on `DROPPED` → `"does not exist"`** (or a no-op under `checkfirst`, i.e. `IF EXISTS`),
  exactly as dropping a `MISSING` table. Replaces the old `"already dropped in catalog"`.
- **Reflection / autoload on `DROPPED` → `NoSuchTableError` `"does not exist"`.** Replaces the old
  `"is dropped and cannot be reflected"`.
- **`ORPHANED` is unchanged** at every seam — it raises `InternalError`. It is an integrity fault
  (intent and reality disagree), not a "table is gone" case.
- **The old database is abandoned in Notion trash.** Notion has no hard-delete, so `DROP` trashes
  the database container (unchanged) and a superseding `CREATE` simply leaves the old, trashed
  database behind. It stays physically recoverable through the Notion UI by a human, but is
  unreachable by normlite once its catalog row has been repurposed.
- **Operational vs diagnostic split.** The DDL *operations* above collapse `DROPPED` into `MISSING`,
  but the diagnostic `get_table_state` **still returns a distinct `DROPPED`**. This is deliberate:
  it keeps the soft-delete machinery (the `is_dropped` marker, the lingering row) observable to
  tooling and preserves it as a latent foundation, so a future restore capability has something to
  find — even though no DDL verb surfaces the distinction today.

## Considered Options

- **Keep `restore_dropped` as an opt-in revive** (default create-fresh, `restore_dropped=True`
  revives the old database + data). Rejected: even as an opt-in, restore is a Notion-driven dialect
  idiom with no SQL analog; exposing it invites users to build on a "dropped tables come back"
  assumption that SQL semantics deny. The machinery is retained internally, so the capability can be
  reintroduced later if a concrete need appears — but it is not part of the public surface now.
- **Remove the soft-delete concept entirely** — `DROP` deletes the catalog row (page-trash), no
  `is_dropped`, no `DROPPED` state, superseding
  [ADR-0015](0015-catalog-soft-delete-explicit-property.md). Rejected *for now*: it is the cleanest
  possible model, but it walks back freshly committed work and discards the two-flag `ORPHANED`
  detection's explicit intent marker. We chose to keep the machinery latent rather than delete it.
- **Scope the change to the `CREATE` seam only**, leaving `drop`/reflect treating `DROPPED`
  specially. Rejected: it leaves normlite internally inconsistent (create says "gone," reflect says
  "dropped") for no benefit. If dropped is gone, it is gone everywhere a DDL verb can observe it.

## Consequences

- **Depends on the `data_source_id` persistence slice.** `CREATE`-after-`DROP` repurpose overwrites
  the persisted `data_source_id` column, which does not exist until that deferred slice lands. This
  ADR's slice therefore sequences *after* `data_source_id` persistence.
- **Data-safety trade-off, stated plainly.** SQL `DROP` is destructive, so a superseding `CREATE`
  loses the old table *as far as normlite is concerned*. The data physically survives in Notion's
  trash and a human can recover it through the Notion UI, but there is no normlite path back —
  once the row is repurposed, revive-by-name is gone.
- **Tests flip.** `test_create_after_drop_without_restore_raises` and
  `test_create_checkfirst_on_dropped_table_does_not_restore` change from "raises" to "creates a
  fresh table"; `test_create_after_drop_restores_when_option_enabled` is **deleted** (no option);
  `drop`-of-dropped and reflect-of-dropped tests flip to `"does not exist"`.
- **ADR-0015 is retained, not superseded**, but its CREATE-after-DROP consequence (which briefly
  read "the fall-through must become a RESTORE") is corrected here: create-after-drop is a
  *repurpose to a fresh table*, and restore is not a user-facing operation at all.
- The low-level invariant that `get_or_create_sys_tables_row` must never leave two live rows for one
  name still holds and is guarded defensively — repurpose relies on there being exactly one row to
  reuse.
