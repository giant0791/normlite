# ADR-0027: The catalog lists children; it does not search

**Status:** Accepted
**Date:** 2026-11-17
**Issue:** #505 (also #525)

## Context

`SystemCatalog` finds its objects by name in two places:

- **Bootstrap** gets or creates `information_schema` under the root page, the `tables` database
  under `information_schema`, and the user database page under the root page. It used
  `find_child_page`, `find_child_database` and `_add`. Only `InMemoryNotionClient` has these
  methods (the "Known limit" of [ADR-0026](0026-the-dbapi-builds-the-client-the-engine-translates-the-uri.md)).
- **Orphan detection** (`get_table_state` for a table with no catalog row) called `search` for a
  data source with the title `table_name`.

The Notion REST API has no "find child by name" endpoint. #505 adds `HttpNotionClient`, so the
catalog must use real endpoints only.

## Decision

1. `AbstractNotionClient` gets one new abstract endpoint method, `blocks_children_list`, for
   `GET /v1/blocks/{block_id}/children`. The name follows the `f"{endpoint}_{request}"` rule of
   `AbstractNotionClient.__call__`. The in-memory client derives the children from the `parent`
   pointers it stores, and returns `child_page` and `child_database` blocks.
2. The find logic lives in `SystemCatalog`. It walks all result pages and matches the block `type`
   and the exact `title`. For a database hit, it calls `databases_retrieve` to get
   `data_sources[0].id`. Two children of the same type with the same title raise `InternalError`.
3. Bootstrap creates objects with `pages_create` and `databases_create`.
4. Orphan detection lists the children of the **user database page** and looks for a
   `child_database` block with the title `table_name`.
5. `SystemCatalog` calls no `search`. `find_child_page` and `find_child_database` are deleted from
   `InMemoryNotionClient`. The catalog calls only `AbstractNotionClient` methods.

## Reasons

- **Consistency.** The children list reflects a write at once. The `search` index is eventually
  consistent: a second `create_engine` a few seconds after the first can miss the new
  `information_schema` page and create a second one.
- **Scope.** `search` covers every object the integration can see. A normal Notion database named
  `students` anywhere in the shared tree made `CREATE TABLE students` fail as `ORPHANED`. In memory,
  `search` also ignored `table_catalog`, so a table in one user database made a table with the same
  name in another user database look `ORPHANED` (#525).
- **Exact match.** `search` matches titles fuzzily. The catalog needs exact titles.

## Considered options

- **`search` with a client-side exact-title filter.** Rejected. It fixes the fuzzy match, but not
  the consistency or the scope.
- **Keep `find_child_*` as concrete helpers on `AbstractNotionClient`, built on
  `blocks_children_list`.** Rejected. Title matching and duplicate detection are catalog rules, not
  client rules. The client stays a thin mirror of the REST API.

## Consequences

- **Pagination.** A page with many children needs several `blocks_children_list` calls (100
  children per call). The catalog pages are small, but a user database with many tables costs one
  call per 100 tables for each orphan check.
- **Invisible databases.** A Notion database outside the user database page is invisible to
  normlite. Adopting such databases as tables is out of scope.
- **First-use race.** Listing is consistent, but bootstrap still has no lock. Two first-use
  bootstraps at the same time can create duplicates. This is a known limit (CONTEXT
  §"Engine construction failure").
