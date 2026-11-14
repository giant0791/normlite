# ADR-0026: The DBAPI builds the client; the Engine translates the URI

**Status:** Accepted
**Date:** 2026-11-14
**Issue:** #439

## Context

Before #439, `Engine.__init__` did four jobs. It read the parsed URI. It built an
`InMemoryNotionClient` or a `FileBasedNotionClient`. It built a DBAPI `Connection` and injected the
client. Then it drove execution. Only the last job is orchestration.

#505 adds an HTTP client for real Notion integrations. If the Engine still builds clients, every new
client kind adds a branch to the Engine.

The issue sketch moved client creation into `dbapi2.connect(URI=parsed_uri)`. But the URI types
(`NotionURI`, `NotionSimulatedURI`, `NotionAuthURI`) and `_parse_uri` live in `engine/base.py`.
CONTEXT §"Layers" sets one dependency direction: ENGINE → DBAPI, and nobody calls upward. A
`connect()` that reads `uri.kind` and `uri.mode` depends on an ENGINE type.

## Decision

1. **`dbapi2.connect()` takes plain keyword arguments.** The signature is
   `connect(*, mode=None, path=None, read_only=False, token=None, version=None) -> Connection`.
   DBAPI knows no URI type.
2. **The Engine translates the URI.** `create_engine` parses the URI. The Engine turns the parsed
   URI into the keyword arguments (`_connect_args`) and calls `connect()`.
3. **The Engine is client-agnostic.** It never names or builds a concrete client class. It reaches
   the client only through the public `dbapi2.Connection.client` property, typed as
   `AbstractNotionClient`.

## Reasons

- The layer rule holds with no upward import.
- PEP 249 lets each driver choose its `connect()` parameters. SQLAlchemy uses the same split: the
  dialect turns a URL into `connect()` arguments (`create_connect_args`).
- The `normlite://` URL is the address format of ENGINE users. DBAPI does not need to own it.
- #505 changes only the body of `connect()`. The `token` and `version` keywords are already in the
  signature.

## Considered options

- **Move the URI dataclasses and `_parse_uri` into `notiondbapi/`.** Rejected. The layer rule
  holds, but DBAPI then owns the user-facing URL format.
- **`connect(dsn: str)`, and DBAPI parses the raw string.** Rejected for the same reason. It also
  moves URI validation away from `create_engine`, where users see it.
- **Keep the private reach `self._dbapi_connection._client`.** Rejected. It moves construction but
  keeps the coupling behind a private attribute.

## Consequences

- **Ensure vs. resolve the root page.** `connect()` ensures the root page of a simulated store
  exists. The Engine resolves the catalog anchor: the user's `root_page_id`, else the client's
  public `default_root_page_id` (`None` for a real integration). If both are `None`, the Engine
  raises `ArgumentError`.
- **Validation of user input stays in the Engine.** `read_only` with `:memory:` raises
  `ArgumentError` in `_connect_args`, before the Engine calls `connect()`. `connect()` raises
  `InterfaceError` for a missing or unknown `mode`, and `NotImplementedError` for `token`.
- **The Engine keeps catalog naming.** The `user_database_name` default (`"memory"`, or the file
  stem) comes from the URI in the Engine. The deprecated `Engine._database` is deleted.
- **The creator closes.** `dbapi2.Connection` gains a PEP 249 `close()`, and `Engine.dispose()`
  calls it. See the amendment in [ADR-0003](0003-engine-dispose-semantics.md).
- **Test seams stay.** `Engine._client` becomes a private read-only property that returns
  `self._dbapi_connection.client`. `dbapi2.Connection(client)` stays the dependency-injection seam,
  and `connect()` is the public factory on top of it.
- **Known limit.** `SystemCatalog` calls `find_child_page`, `find_child_database` and `_add`. These
  methods exist only on `InMemoryNotionClient`. The Engine is client-agnostic by type, but the
  catalog still needs these methods. #505 resolves this in the catalog creation workflow.
