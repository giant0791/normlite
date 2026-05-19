# ADR-0003: Engine.dispose() semantics

**Status:** Accepted
**Date:** 2026-05-19

---

## Context

Issue #260 adds `Engine.dispose()` so users can release backend resources held by an
`Engine` — most importantly, flush an `InMemoryNotionClient`-backed store to disk via
`FileBasedNotionClient.close() → flush()`. The user-visible motivation in the issue
body is the file-based persistence path:

```python
engine = create_engine("normlite:/path/to/database/file.db", root_page_id=...)
# ... DDL/DML ...
engine.dispose()
```

The naive reading of "dispose" leaves three load-bearing questions unanswered:

1. **Post-dispose state** — is the engine still usable, or terminal?
2. **Cascade depth** — does `dispose()` walk every component the engine holds
   (`_dbapi_connection`, `_catalog`, user-held `Connection`s, `_client`), or only the
   things that own real I/O state?
3. **Error handling** — if `_client.close()` raises (e.g. `FileBasedNotionClient.flush()`
   hits `PermissionError` or a full disk), does the engine remain "not yet disposed" so
   the user can retry, or does it commit to the terminal state regardless?

The shape of `dispose()` is locked in by the API exposed in v0.11.0. Changing any of the
three decisions later would break user code that catches the post-dispose exception,
relies on the cascade order, or expects retry-after-failure to be supported.

## Decision

**`Engine.dispose()` is terminal, has a minimal cascade, and flips the disposed flag
in `finally` whether or not `_client.close()` raises.**

```python
def dispose(self) -> None:
    if self._is_disposed:
        return
    try:
        self._client.close()
    finally:
        self._is_disposed = True
```

Concrete consequences of the three sub-decisions:

- **Terminal.** After `dispose()` has been *called* (regardless of whether it returned
  or raised), the engine is dead. `Engine.connect()` and `Engine.raw_connection()` check
  `self._is_disposed` and raise
  `InvalidRequestError("Engine has been disposed")`. A read-only `engine.disposed`
  property exposes the state. A second call to `dispose()` is a silent no-op.
- **Minimal cascade.** Only `self._client.close()` is called. `self._dbapi_connection`
  (a pure-Python object), `self._catalog` (a pure-Python object), and any user-held
  `Connection` objects handed out by `engine.connect()` are not given `close()` methods
  and are not tracked in a registry. Held-over `Connection`s fail at their next
  `execute()` because the path runs through `Engine.raw_connection()` — the single
  guard catches every realistic reuse path.
- **Finally-flips-flag on close failure.** The `try / finally` lets the I/O exception
  propagate to the caller unchanged *and* commits the engine to the terminal state.
  No retry path is supported.

## Alternatives Considered

**A. Reusable engine (SQLAlchemy-style `dispose()`).**
Rejected. SQLAlchemy's `Engine.dispose()` is "reset the pool, engine stays usable"
because there is a connection pool to reset. Normlite has no pool — one `_client`,
one `_dbapi_connection`, one `_catalog` per engine. "Reset to a fresh state" has no
meaning here. Terminal semantics is also the contract that lets a future contributor
add transactions, pooling, or background flushers without breaking users who got into
the habit of "dispose then keep going."

**B. Full cascade (`close()` on DBAPI Connection, SystemCatalog, user-held Connections).**
Rejected. The only resource with real lifecycle is the client (file handle, persisted
state). The other components are pure Python objects with no I/O of their own; adding
no-op `close()` methods to them is YAGNI. A `WeakSet` registry of handed-out
`Connection` objects adds bookkeeping that protects nothing — fail-on-next-use is just
as effective, and arguably better because the user sees the error where they made the
mistake, not silently at dispose time. Full cascade becomes the right design when there
is transactional state, a pool, or async cleanup to coordinate — none of those exist
yet.

**C. Propagate exception, leave `_is_disposed = False` (retry-possible).**
Rejected. Looks attractive ("the operation didn't succeed, so the engine isn't disposed")
but is misleading in practice:
1. `FileBasedNotionClient.flush()` opens the target file with mode `"w"` and `json.dump`s
   into it. A mid-write failure has already truncated the previous on-disk content;
   the data is gone. Retrying `dispose()` cannot recover anything.
2. Leaving `_is_disposed = False` invites the user to keep mutating the in-memory store
   after a failed flush. The store and the file are now permanently out of sync, and no
   subsequent `dispose()` can repair that.
3. It is inconsistent with how Python idiomatically handles close-time failures
   (`file.close()`, sockets, `contextlib.closing`): the resource is *closed* even if
   the final flush failed.

The `try / finally` shape gives users the actionable I/O exception *and* the unambiguous
state invariant "after dispose has been called, the engine is over."

**D. New `EngineDisposedError` exception type.**
Rejected. The existing `InvalidRequestError` in `normlite.exceptions` is documented as
"raised when a normlite method or function cannot perform as requested" — that is
exactly what "engine has been disposed" means. New typed exceptions earn their keep
when callers need to discriminate them in `except` clauses; disposed-engine errors are
almost always programming bugs, not runtime conditions you catch, so the typed-exception
payoff is low. `InternalError` was considered and rejected too — it is part of the
DBAPI exception family (database-internal error), and overloading it for engine-state
errors muddles its DBAPI semantics.

## Consequences

- `Engine` gains a private `_is_disposed: bool = False` attribute, set in `__init__`,
  flipped in `dispose()`'s `finally` block. Public `engine.disposed` property exposes it.
- `Engine.connect()` and `Engine.raw_connection()` both raise `InvalidRequestError`
  when `self._is_disposed`. No other public methods are guarded — calls that go through
  the client (`find_table_metadata`, `inspect`, etc.) fail downstream at the client
  layer, which is acceptable because the issue's user-facing path is
  `engine.connect()`-driven.
- `Engine.__enter__` / `Engine.__exit__` are added in the same slice. `__exit__` calls
  `dispose()` unconditionally and returns `None` (never swallows exceptions).
- Held-over `Connection` objects do not need any new state. Their next `execute()` reads
  `self._engine.raw_connection()`, which checks `_is_disposed` on `Engine` and raises.
- A future "engine pool" or "transactional engine" feature can introduce richer cascade
  (DBAPI `Connection.close()`, transaction rollback on dispose, etc.) without breaking
  the public contract established here: post-dispose use of `connect()` /
  `raw_connection()` remains an `InvalidRequestError`.
- Users wanting "flush without disposing" do not have an API for it. If a real use case
  appears, a separate `engine.flush()` or `dispose(close=False)` can be added later
  without breaking this contract — the addition would be non-breaking because the
  default behaviour stays terminal.
