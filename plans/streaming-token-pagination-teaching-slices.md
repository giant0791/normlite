# Teaching Slices — Streaming Result over Token Pagination

**Source design:** [ADR-0010](../docs/adr/0010-streaming-result-token-pagination.md) ·
CONTEXT.md → "Pagination"
**Status:** Approved (not published to tracker)
**Difficulty mix:** 2 Guided · 2 Stretch · 1 Challenge

End-to-end vertical slices, ordered by dependency. Each fits 3–10 TDD iterations and is
independently verifiable. Ships against in-memory clients only (no real Notion integration yet).

```
Slice 1 ──▶ Slice 2 ──▶ Slice 3 ──┬──▶ Slice 4
                                   └──▶ Slice 5
```
Slices 4 and 5 depend only on 3 and may proceed in parallel.

---

## Slice 1 — Fake client paginates `databases_query`

- **Learning Mode:** Guided · **Difficulty:** Guided · **Blocked by:** none
- **User stories:** substrate — drain-all becomes testable end-to-end against the fake.
- **Learning Objective:** Implement producer-side token/keyset pagination — slice a result list
  by an opaque cursor and report continuation honestly.
- **Concepts Practiced:** `page_size`/`start_cursor` handling, `has_more`/`next_cursor` shape,
  opaque offset-token encode/decode, default `page_size=100`, paginate *after* filter+sort.
- **Failure Modes to Watch:** off-by-one on the last page (`has_more` true when exactly drained);
  `next_cursor` set while `has_more` false; leaking a bare integer token; paginating before
  filter+sort.
- **Suggested First Test:** 3 matching pages, `page_size=2` → response 1: 2 results,
  `has_more=True`, non-null `next_cursor`; response 2 (with that cursor): 1 result,
  `has_more=False`, `next_cursor=None`; union equals all 3 rows in order.

## Slice 2 — `ResultSet` page-fetch seam + drain-all in `Cursor`

- **Learning Mode:** Guided · **Difficulty:** Stretch · **Blocked by:** Slice 1
- **User stories:** correctness floor — every existing caller gets all pages for free.
- **Learning Objective:** Turn a frozen `list[tuple]` into a result that owns its continuation
  state; drive the drain loop from the layer holding the client.
- **Concepts Practiced:** `_fetch_next(start_cursor)` closure built by `Cursor`; body-injection of
  `start_cursor`/`page_size` into `databases.query`; first-page-eager (description + error
  translation); `next_cursor`/`has_more`/`exhausted` state; `nextset()` stays orthogonal.
- **Failure Modes to Watch:** page-per-`ResultSet` (corrupts `nextset()`); mutating shared
  `payload` instead of a per-fetch copy; infinite loop when `has_more` true but `next_cursor`
  null; breaking `executemany` multi-result-set semantics.
- **Suggested First Test:** 5 matching pages, internal `page_size=2`, `execute(query)` then
  `fetchall()` returns all 5 in order; `rowcount == 5`. Regression: `executemany` still yields one
  result set per param batch.

## Slice 3 — Opt-in lazy fetching at the DBAPI level (`stream_results`/`yield_per`)

- **Learning Mode:** Guided · **Difficulty:** Challenge · **Blocked by:** Slice 2
- **User stories:** iterate large results with bounded memory.
- **Learning Objective:** Make fetching demand-driven and prove it by observation (client not
  called until the buffer drains); honor the deferred-`rowcount` contract.
- **Concepts Practiced:** `ExecutionOptions` keys `stream_results`/`yield_per` (`yield_per` implies
  `stream_results`); `page_size = min(yield_per or 100, 100)`; `yield_per > 100` pulls multiple
  pages per batch; `rowcount == -1` until `exhausted`.
- **Failure Modes to Watch:** eagerly draining anyway (test must count client calls, not totals);
  buffered-length as `rowcount` mid-stream; forgetting `yield_per` implies `stream_results`;
  wrong `page_size` clamp when `yield_per > 100`.
- **Suggested First Test:** Call-counting fake, `yield_per=2` over 5 rows: after `execute()` the
  client was called once; `rowcount == -1`; fetching row 3 triggers a second call; after
  exhaustion `rowcount == 5`.

## Slice 4 — Select-only gating + `CursorResult` streaming consumption

- **Learning Mode:** Solo · **Difficulty:** Stretch · **Blocked by:** Slice 3
- **User stories:** streaming never corrupts a mutation or join.
- **Learning Objective:** Gate a cross-cutting option to exactly one execution path; expose
  streaming through the high-level façade.
- **Concepts Practiced:** gate streaming to the row-returning `Select` path (mutations / join
  phase-1 / reflection force drain-all even with `stream_results=True` on the connection);
  `CursorResult.__iter__`/`fetchone`/`fetchmany` stream, `all()`/`fetchall()` materialize.
- **Failure Modes to Watch:** gating on the raw option instead of "is this a user-facing Select"
  (a `Delete` on a streaming connection silently streams phase 1); `all()` short-circuiting the
  drain; `one()`/`first()` not stopping early.
- **Suggested First Test:** On a `stream_results=True` connection, a two-phase `Delete` still
  drains all phase-1 pages and reports accurate `rowcount`; separately,
  `conn.execute(select).fetchmany(2)` returns 2 `Row`s and leaves the rest unfetched.

## Slice 5 — Mid-stream error propagation

- **Learning Mode:** Solo · **Difficulty:** Stretch · **Blocked by:** Slice 3
- **User stories:** a failed page surfaces honestly, not as silent truncation.
- **Learning Objective:** Route a failure occurring *after* `execute()` through the centralized
  translator and propagate it at the iteration boundary.
- **Concepts Practiced:** `_fetch_next` using `_translate_notion_error`; propagate (no
  skip-and-continue); already-yielded rows stay yielded.
- **Failure Modes to Watch:** copying `executemany`'s skip-and-continue (unreachable later pages);
  swallowing into an empty result; raising raw `NotionError` instead of the mapped DBAPI class.
- **Suggested First Test:** Fake injects `rate_limited` on page 2; iterating yields page-1 rows,
  then raises `OperationalError` (with `NotionError` as `__cause__`) crossing into page 2.

---

## Out of scope (named boundaries, per ADR-0010)

- GET-style paginated endpoints (`query_params` injection vs body).
- `search` / `_get_by_title` pagination (stay single-page).
- `CursorResult.partitions(n)`.
- Real Notion integration and relation-property `has_more` truncation (ADR-0007).
