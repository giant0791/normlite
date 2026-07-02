# ADR-0014: Notion API 2025-09-03 upgrade — the database/data-source two-ID identity

**Status:** Accepted
**Date:** 2026-07-02

---

## Context

The Notion API version `2025-09-03`
([upgrade guide](https://developers.notion.com/guides/get-started/upgrade-guide-2025-09-03))
splits what used to be a single **database** into a **database** (container: `title`, `icon`,
`cover`, and a `data_sources: [{id, name}]` list) and one-or-more **data sources** (the
`properties` schema, the queryable surface, the target of relations). The operational verbs move
accordingly:

| Operation | Old home (`database_id`) | New home |
| --- | --- | --- |
| Query rows | `databases.query` | **`data_sources.query`** (`data_source_id`) |
| Read schema | `databases.retrieve` | **`data_sources.retrieve`** (`data_source_id`) |
| Write schema | `databases.update` | **`data_sources.update`** (`data_source_id`) |
| Create | `databases.create` (flat `properties`) | `databases.create` with `initial_data_source.properties`; returns both IDs |
| Drop / container | `databases.update {in_trash}` | `databases.update` (`database_id`) — unchanged |
| Page parent | `parent.type = database_id` | `parent.type = data_source_id` |
| Relation target | `relation.database_id` | `relation.data_source_id` |
| Search result object | `"object": "database"` | `"object": "data_source"` |

normlite maps one SQL `Table` to one Notion database, and today carries a **single** ID
(`object_id`/`get_oid()` = the database UUID) that serves *all* of the roles above. The upgrade
splits that one ID into two, and normlite must decide how to represent the split without inventing
SQL semantics for a capability (multi-source databases) that has no SQL analog yet.

## Decision

**Adopt the 2025-09-03 wire shape and a two-ID identity now, under the invariant that one `Table`
= one database containing exactly one data source. Multi-data-source databases are explicitly
deferred.**

- **Identity stays `database_id`.** `object_id`/`get_oid()` continues to name the **database
  UUID** — the thing a page-child hangs off and `DROP TABLE` (`databases.update {in_trash}`)
  trashes. The public `ForeignKey("students.object_id")` reference is unchanged.
- **`data_source_id` is a first-class, private `Table` attribute** (mirroring `_db_parent_id`),
  *not* a `table.c` system column. System columns model an object's top-level Notion keys via
  `api_key`; `data_source_id` is a container→child relationship (`data_sources[0].id`) with no
  such key, and it is routing plumbing, not something a SQL user selects. There is no
  `students.c.data_source_id`.
- **Routing split.** `database_id` is read only by create / drop / container-level operations;
  `data_source_id` is read by *everything operational* — `data_sources.query` (SELECT and the
  two-phase UPDATE/DELETE find-pages), `data_sources.retrieve` (reflection), page-parent on
  `pages.create`, and relation schema specs. The compiler reads `get_data_source_id()` wherever
  it used to read `get_oid()` for query/parent.
- **Both IDs are persisted.** `databases.create` returns the container plus `data_sources[0].id`;
  the create-result processing captures both, and the `tables` catalog gains a `data_source_id`
  rich-text property alongside `table_id`. Reflection is **catalog-first**: it reads
  `data_source_id` from the catalog row (normlite always enters reflection via the catalog) and
  calls `data_sources.retrieve` directly, skipping the `databases.retrieve` round-trip an external
  client would need.
- **Relations retarget to `data_source_id`.** `ForeignKey.database_id` → `ForeignKey.data_source_id`,
  resolved to the target table's data source; the relation DDL spec emits
  `{"relation": {"data_source_id": "...", "single_property": {}}}`. Relation **values** are
  untouched — a relation still stores `{"relation": [{"id": "<page_id>"}, ...]}`, because linked
  rows are still pages.
- **The fake is the executable spec.** `InMemoryNotionClient` stores `database` and `data_source`
  as **separate objects** with their own IDs and a parent link; enforces **per-child-type** parent
  validation (page → `page_id`/`data_source_id`; database → `page_id`; data_source → `database_id`);
  replaces `databases_query` with `data_sources_{retrieve,update,query}` outright (no deprecation
  shims — every caller is in-repo, pre-1.0); and returns `data_source` objects from `search`. The
  internal catalog databases (`information_schema`, `tables`) get the identical two-ID treatment —
  one code path, no special-casing.

## Considered Options

- **Flip `object_id` = `data_source_id`** (since data_source_id dominates operationally). Rejected:
  it silently rewrites FK targets, catalog lookups, and reflection at once — a high-blast-radius
  rename — for no gain over carrying a second private ID.
- **Derive `data_source_id` from `database_id` in the fake** (it's 1:1). Rejected: real Notion
  returns an *independent* `data_source_id` from `databases.create`; deriving it would make the
  fake diverge from the real API and defeat the point of speaking the new shape.
- **Pull the multi-data-source model into normlite now.** Rejected: no SQL analog exists; it would
  ripple into `Table`, reflection, FKs, and the catalog for a capability nobody has asked for.

## Consequences

- Persisted `FileBasedNotionClient` stores from before the upgrade will not load: the store shape
  changes (separate `data_source` objects, database objects lose top-level `properties`, pages
  re-parent to `data_source_id`). This is a **clean break** — pre-1.0, no external consumers — with
  a loud "store predates the 2025-09-03 upgrade; recreate it" guard rather than a migrator.
- Multi-data-source databases remain inexpressible. Adopting them later is a dedicated slice that
  must decide how multiple schemas under one container map to the SQL `Table` model.
- `NOTION_VERSION` bumps `2022-06-28` → `2025-09-03`. The fake ignores the header; it documents
  intent for the eventual real client, which this upgrade makes a drop-in.
