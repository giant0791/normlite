# ADR-0026: The DBAPI builds the client; the Engine translates the URI

**Status:** Accepted (amended 2026-11-15 by #510, see "Amendment")
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
  `ArgumentError` in `NotionURI.translate_connect_args()`, before the Engine calls `connect()`.
  `connect()` raises `InterfaceError` for a missing or unknown `mode` and for file mode without a
  `path`. It raises `OperationalError` when the file client cannot open the store (a missing file
  in read-only mode, or a corrupted store). A set `token` or `version` raises
  `NotImplementedError` ("Internal integrations are not supported yet."). An external URI never
  reaches `connect()`: the Engine raises `NotImplementedError` for it first.
- **The Engine keeps catalog naming.** The `user_database_name` default (`"memory"`, or the file
  stem) comes from the URI in the Engine. The deprecated `Engine._database` is deleted.
- **The creator closes.** `dbapi2.Connection` gains a PEP 249 `close()`, and `Engine.dispose()`
  calls it. See the amendment in [ADR-0003](0003-engine-dispose-semantics.md).
- **`client` is a normlite extension.** PEP 249 does not define `Connection.client`. PEP 249
  sets a minimum, and extra attributes do not break compliance. Only the Engine reads the property.
  The property is read-only: the constructor is the only way to set the client.
- **Test seams stay.** `Engine._client` becomes a private read-only property that returns
  `self._dbapi_connection.client`. `dbapi2.Connection(client)` stays the dependency-injection seam,
  and `connect()` is the public factory on top of it.
- **Known limit.** `SystemCatalog` calls `find_child_page`, `find_child_database` and `_add`. These
  methods exist only on `InMemoryNotionClient`. The Engine is client-agnostic by type, but the
  catalog still needs these methods. #505 resolves this in the catalog creation workflow.

## Amendment (2026-11-15, #510)

The first TDD steps of #510 used a free function `_connect_args(uri)`. It returned a record of the
`connect()` keywords and the user database name. Two problems came up. The names `_ConnectArgs` and
`_ConnectKWArgs` were too close to tell apart. Each function that read the URI also had to branch on
two URI classes (`NotionSimulatedURI`, `NotionAuthURI`) with different fields.

- **One flat `NotionURI`.** It is a frozen, keyword-only dataclass in `engine/notionuri.py`. It
  holds the union of all fields: `kind` (`"simulated"`, `"internal"`, `"external"`, the only
  required field), `database`, `mode`, `path`, `token`, `version`, `client_id`, `client_secret`,
  `auth_url`. `NotionSimulatedURI` and `NotionAuthURI` are deleted (breaking). The `file` field is
  deleted, because `database` holds the file stem.
- **The URI carries the user database name.** `_parse_uri` sets `database`: `"memory"` for
  `:memory:`, the file stem for a file URI, `None` for an auth URI. A future auth URI carries it as
  a path segment (`internal/university`). The Engine uses the user `user_database_name`, else
  `uri.database`. The name is still not a `connect()` keyword.
- **The URI translates itself.** `NotionURI.translate_connect_args(*, read_only=False)` replaces
  `_connect_args`. It returns only the `connect()` keywords (`_ConnectKWArgs`, a `TypedDict`), and
  only the keys that the URI sets. It raises `ArgumentError` for `read_only=True` with a memory
  URI, and `NotImplementedError` for an external URI. `_ConnectArgs` is deleted.

**Why.** SQLAlchemy uses the same shape: one flat `URL` type with a `database` field and a
`translate_connect_args()` method. A frozen dataclass, not a `NamedTuple`, so that no code can read
the URI by position. The layer rule does not change: `NotionURI` stays in ENGINE, and DBAPI still
knows no URI type.

**Rejected.** A flat `_ConnectArgs` with `user_database_name` next to the `connect()` keywords and
`dataclasses.asdict()`. The Engine must then remove the name before it calls `connect()`, and
`asdict()` returns every field, so the keywords are no longer minimal.
