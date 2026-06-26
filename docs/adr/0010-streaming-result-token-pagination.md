# ADR-0010: Streaming result over Notion token pagination

**Status:** Accepted
**Date:** 2026-06-23

---

## Context

normlite ignores Notion pagination entirely. `Cursor.execute()` (`notiondbapi/dbapi2.py`)
makes exactly one client call and appends exactly one `ResultSet`, a fully-materialized
`list[tuple]`. The fake `InMemoryNotionClient.databases_query` hard-codes
`next_cursor: None, has_more: False` and returns every matching page in one response.

This is wrong on two independent axes, which a single feature request ("expose Notion
pagination as a server-side cursor like SQLAlchemy") conflated:

1. **Correctness.** Notion's `databases.query` is paginated: `page_size` defaults to and
   caps at **100**, and the response carries `has_more` / `next_cursor`. Every internal
   consumer that calls `databases.query` — joins, two-phase `Delete`/`Update`, reflection,
   bootstrap — reads only the first response's `results` and ignores `has_more`. Against a
   real Notion workspace with >100 matches they would **silently drop rows**. This is a
   latent bug today only because the fake never truncates.

2. **Feature.** Users want SQLAlchemy-style bounded-memory iteration over large result
   sets — fetch pages on demand while iterating, rather than buffering everything.

The dependency that forces both into one slice: the tracer bullet ships against in-memory
clients only (no real Notion integration yet — see CONTEXT.md "Deferred — `has_more`
truncation" and [ADR-0007](./0007-join-dangling-fk-propagation.md)). The *only* way to
exercise either axis end-to-end is to teach the fake to paginate — and the moment it does,
every "read page 1 only" consumer becomes a >100-row test away from breaking.

A terminology trap also had to be settled: Notion has **no server-side cursor**. It holds
no cursor object, offers no `FETCH`, and gives no stability guarantee between page requests.
"Server-side cursor" is a borrowed metaphor; what we can actually deliver is lazy streaming
over **token pagination**.

## Decision

**Make the DBAPI cursor drain all pages by default; layer opt-in lazy streaming on top; model
a paginated query as one streaming `ResultSet`.**

- **Canonical term: "streaming result over token pagination."** Not "server-side cursor"
  (reserved for user-facing docs analogy, with the not-stable caveat). The result set is not
  stable across page requests.

- **Drain-all by default.** `Cursor.execute()` loops `start_cursor` ← previous `next_cursor`
  until `has_more` is false. This makes every existing caller correct against pagination for
  free and preserves today's "you get everything" semantics and the `rowcount` contract.

- **Streaming is opt-in and Select-only.** New `ExecutionOptions` keys `stream_results: bool`
  and `yield_per: int` (`yield_per` implies `stream_results`), reusing the existing
  engine→connection→statement options cascade. Honored **only** on the row-returning `Select`
  read path. Two-phase `Delete`/`Update`, join phase-1 scans, reflection and bootstrap force
  drain-all even when `stream_results=True` is inherited from the connection — phase 1 must
  know the full match set before phase 2.

- **One streaming `ResultSet`, not one per page.** Pages are orthogonal to the
  `Cursor._result_sets` list, which keeps meaning "one entry per statement / `executemany`
  batch"; `nextset()` is untouched. `ResultSet` grows a page-fetch seam: a
  `_fetch_next(start_cursor)` closure built by the `Cursor` (which owns
  `operation`/`parameters`/client and injects `start_cursor`/`page_size` into the
  `databases.query` POST **body**), plus `next_cursor`/`has_more`/`exhausted` state. The
  **first page is always eager** (establishes the description; is where the page-1
  `NotionError`→DBAPI translation happens). Later pages are eager (drain-all) or lazy
  (streaming).

- **`page_size` internal and capped.** `min(yield_per or 100, 100)`. `yield_per` (logical
  batch) is decoupled from `page_size` (transport, ≤100); `yield_per > 100` pulls multiple
  Notion pages per batch. Not a user knob in v1.

- **`rowcount` honors the pre-existing docstring contract.** Drain-all: accurate immediately.
  Streaming: **-1 until the `ResultSet` is exhausted**, then the true sum. `preserve_rowcount`
  memoizing -1 mid-stream is correct.

- **Mid-stream errors propagate.** A lazy page fetch that fails during iteration routes
  through the centralized `_translate_notion_error` and **raises** (no skip-and-continue like
  `executemany`) — a broken page makes all later pages unreachable. Already-yielded rows stay.

- **Fake simulates pagination for `databases_query` only.** Honors `page_size`/`start_cursor`,
  slices its filtered+sorted pages, emits real `next_cursor`/`has_more`. Token is an opaque
  offset-encoded string (never reaches users). Default `page_size = 100` (matches Notion; tiny
  test stores keep `has_more` false → zero behavior change for current callers). View is
  recomputed and offset-sliced each call (faithfully non-stable, not a frozen snapshot).

## Alternatives Considered

**A. Always lazy (no drain-all default).** Rejected: would leave every internal full-scan
consumer (joins, two-phase DML) reading page 1 only — shipping the correctness bug — and
would break `rowcount` for mutations. Drain-all is the honest, back-compatible foundation;
streaming is the additive feature.

**B. One `ResultSet` per page (append each page to `_result_sets`).** Tempting because
`_iter_all`/`rowcount`/`fetchone`+`nextset` already span the list, so it "just works" with
no new code. Rejected: `nextset()` and the multi-`ResultSet` list already mean "next
statement / `executemany` batch." Making pages separate result sets collides with that — a
user calling `nextset()` to skip to the next statement's output would land on page 2 of the
same query. Pagination is one logical result set delivered in chunks, not multiple result
sets.

**C. Drain inside the client (`databases_query` returns a merged full list).** Rejected: it
would force eager draining always, making lazy streaming impossible, and would push the
pagination loop below the layer (the `Cursor`) that needs to control laziness.

**D. Expose `page_size` as a user knob.** Rejected for v1: the ≤100 cap and the
`yield_per`/`page_size` decoupling are Notion-transport details. `yield_per` is the
SQLAlchemy-idiomatic logical knob; leaking `page_size` invites users to set values >100 that
silently clamp.

## Consequences

- **Fixes the latent silent-row-drop** for joins and two-phase DML against paginated results,
  as a side effect of drain-all-by-default — independently of whether anyone uses streaming.
- **`ResultSet` is no longer a frozen `list[tuple]`** — it holds page-fetch state. Its
  equality/`description` contracts and the DBAPI `description` are otherwise unchanged.
- **Paginated `execute()` is atomic in drain-all mode.** The cursor drains every page into a
  *local* `ResultSet` and only publishes it (`_reset_results()` + append) after the full token
  walk succeeds. A failure on any page — a `NotionError` translated to a DBAPI error, or a
  malformed page (`has_more=True` with `next_cursor=None`) surfaced as `DatabaseError` — leaves
  the cursor's prior result state untouched: callers never observe a torn read presented as
  complete. (Lazy streaming relaxes this by construction — see Slice 5 mid-stream error
  propagation.)
- **New user-visible API:** `execution_options(stream_results=True)` / `yield_per=N` on the
  `Select` read path; `__iter__`/`fetchone`/`fetchmany` stream, `all()`/`fetchall()`
  materialize-by-draining (documented, not blocked).
- **Adopts the centralized error translator on the lazy path**, nudging toward retiring the
  inline `raise ... from ne` in `execute()` (pre-existing tech debt; separate cleanup). The lazy
  green generalized `_translate_notion_error` from `NotionError` to any `Exception`
  (KeyError/ValueError/NotionError + unknown-type fallback), which also fixed a latent orphaned
  fallback that left unrecognised `NotionError` codes unmapped.
- **Pagination is an `execute`-only concern.** `executemany` stays the non-paginated bulk-write
  path — one client call per parameter set, no `next_cursor` walk, skip-and-continue error
  handling — and returns no streamable result set (per DBAPI 2.0, `executemany` is not for
  row-returning operations). Neither drain-all nor streaming applies to it. This is a permanent
  boundary by design, not a deferred slice.
- **Realized seam shape.** The page-fetch seam shipped as a standalone `PageIterator`
  (`notiondbapi/page_iterator.py`) owned by the `Cursor` as `_page_iter`, with
  `ResultSet.extend_from_json(page)` appending each page's rows — rather than a `_fetch_next`
  closure internal to `ResultSet` as sketched in the Decision. The decision content (one streaming
  `ResultSet`, `Cursor`-owned seam, first page eager, `page_size` injected into the body) is
  unchanged; only the object boundary moved. `PageIterator._page_size` now carries the clamped
  size; `fetchone` pulls on buffer-drain and `fetchall` drives the pull to completion.
- **Out of scope (named boundaries):** GET-style paginated endpoints (body vs `query_params`
  injection); `search`/`_get_by_title` pagination; `CursorResult.partitions(n)`; real Notion
  integration and the relation-property `has_more` truncation
  ([ADR-0007](./0007-join-dangling-fk-propagation.md)).
