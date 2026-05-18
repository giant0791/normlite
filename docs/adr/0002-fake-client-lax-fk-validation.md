# ADR-0002: Fake Notion client — lax FK-target validation for relation properties

**Status:** Accepted
**Date:** 2026-05-17

---

## Context

`InMemoryNotionClient` is the in-process test double for the Notion API. With FK-bearing
tables (see [ADR-0001](./0001-update-two-phase-values-threading.md) and the
`Relation` / `ForeignKey` glossary in `CONTEXT.md`), pages can now carry relation
properties whose values are lists of page IDs pointing into another database. Three
levels of validation are plausible for the fake at `pages_create` / `pages_update` time:

1. **Lax pass-through** — store whatever bytes arrived, no checks.
2. **Shape-only** — validate the *shape* of the relation value (list of `{"id": str}`),
   but trust the page-ID strings blindly.
3. **Eager FK-target** — additionally verify that every page ID exists in the in-memory
   store and that its parent database matches the relation's declared `database_id`.

This decision shapes how dangling-FK bugs surface in tests built on top of the fake, and
sets the boundary the upcoming `Select.join()` work will be tested against.

## Decision

**The fake validates *shape* but does not validate *FK targets*.**

- `_normalize_property` gains a `relation` branch that asserts `prop["relation"]` is a
  list of `{"id": <str>}` dicts. Malformed shapes raise `NotionError`.
- Page-ID strings inside that shape are accepted without checking the store. A relation
  list may contain IDs of pages that do not exist, are in the wrong database, or are in
  the trash.
- Filter operators (`relation.contains`, `relation.does_not_contain`,
  `relation.is_empty`, `relation.is_not_empty`) match against the stored shape literally.
  A `contains("nonexistent-id")` filter matches any page that literally stores that ID;
  a join over dangling references therefore yields an empty result, not an error.

## Alternatives Considered

**A. Eager FK-target validation.**
Rejected as the default. Diverges from real Notion, which accepts any UUID-shaped string
at write time and surfaces invalid references only as missing entries when the relation
is read or joined. Eager validation would create test/prod behavioural drift: a payload
that works in tests would fail in production if the target page is created out-of-order,
and vice versa. It also adds non-trivial bookkeeping (the fake would need to enforce
ordering, handle deletes, and reason about `in_trash` state for referenced pages).

**B. Lax pass-through.**
Rejected. Without shape validation, malformed payloads from a buggy `bind_processor` or
hand-written tests slip silently into the store and corrupt downstream filter results.
The existing `_Condition` validation rigor (operator allowlists, type/shape checks for
title/rich_text) is the precedent — relation should match it.

## Consequences

- `_normalize_property` raises `NotionError` for malformed relation values; SQL-layer
  tests that rely on `Relation.bind_processor` continue to work because the processor
  always emits the canonical shape.
- Dangling-FK bugs (referencing non-existent or trashed pages) are **not** caught by the
  fake at write time. They manifest as empty join results, missing rows in `RETURNING`
  payloads, or unexpected `is_empty` matches. Tests that need to detect such bugs must
  assert on join cardinality explicitly.
- The fake's behaviour is now closer to real Notion's lazy reference semantics, reducing
  the risk that tests passing against the fake fail against the real API.
- Tightening to eager validation later would be a breaking change for any test that
  currently exploits the lax behaviour. Tests should therefore not deliberately rely on
  the fake accepting dangling references.
