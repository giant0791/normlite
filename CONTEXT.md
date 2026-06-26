# normlite Domain Glossary

normlite is an SQLAlchemy-like frontend that maps SQL-style DML/DDL constructs onto the Notion API.
Each SQL concept maps to one or more Notion API calls; the glossary below captures those mappings
and the decisions made about them.

---

## Core Concepts

### Table
A normlite `Table` corresponds to a **Notion database**. Creating a table calls `databases.create`;
dropping it calls `databases.update` with `in_trash: True`.

### Row / Page
A single row in a `Table` corresponds to a **Notion page** inside that database.
All DML that reads or mutates rows operates on Notion pages.

### Column / Property
A `Column` in a `Table` corresponds to a **Notion database property**. Each column has a `TypeEngine`
subclass that maps to the Notion property type and owns the bind/filter value processors.

### User Column
A column explicitly declared by the user in `Table(...)`. Distinct from **system columns** (e.g.
`object_id`, `_no_id`) which are injected by normlite and carry Notion metadata.

### Projected column name (collision qualification)
A `select(...)` projection preserves the order in which columns are listed. When the projection
spans two joined tables and a **column name is shared** by both (e.g. both `students` and
`courses` declare `title`), each colliding column surfaces **fully qualified** as
`table.column` (`students.title`, `courses.title`) in `keys()` / `mappings()`; non-colliding
names stay **bare**. Selecting the **same column twice** (`select(students.c.title,
students.c.title)`) is an **error**, while the same bare name from two different tables
(`select(students.c.title, courses.c.title)`) is allowed and qualified as above.

### DML Statement
One of `Insert`, `Delete`, `Update`, `Select` — the four DML constructs. Each produces a compiled
payload and follows the two-phase or single-phase execution pipeline below.

### Compiler (NotionCompiler)
Translates a DML/DDL AST into a `compiled_dict` — a JSON-like dict with keys `operation`,
`path_params`, `payload`, and (for UPDATE) `update_payload`. Named placeholders (`:param`) are
resolved at execution time by `ExecutionContext`.

### Compiler entry points: `.compile()` vs `._compiler_dispatch()`
Every `ClauseElement` exposes two compilation entry points with **different lifecycle
semantics**. Confusing them is a real footgun.

- **`.compile(compiler)`** — public, top-level entry. **Resets `compiler._compiler_state`
  to a fresh `CompilerState()`** before dispatch, and wraps the result in a `Compiled`
  (or `DDLCompiled`) object. Use this only when compiling a *statement* from outside
  the compiler (e.g. `stmt.compile(NotionCompiler())` in tests, or from
  `Connection._execute_context`).
- **`._compiler_dispatch(compiler)`** — internal, recursive visitor dispatch.
  **Preserves `_compiler_state`** and returns the raw dict produced by the matching
  `visit_*` method. Use this from any `visit_*` method when descending into sub-nodes
  (`whereclause`, `order_by` clauses, `joins`, expressions).

**The trap**: calling `.compile()` from inside a `visit_*` method (e.g.
`[j.compile(self) for j in select._joins]`) silently clobbers the in-flight
`_compiler_state` for every sub-node and leaves `_compiler_state.stmt = None` by the
time the outer `visit_select` returns. Use `_compiler_dispatch` for sub-node descent;
reserve `compile` for the outermost call.

### Execution Pipeline
The sequence: compile → `pre_exec` (bind params) → `_setup_execution` (Notion API call(s)) →
`_execute_*` → `post_exec` → `_finalize_execution` (cursor routing).

Two execution styles exist for DML:
- **Single-phase** (`EXECUTE`, `INSERTMANYVALUES`): one or many `pages.create` calls driven by parameters.
- **Two-phase** (`EXECUTEMANY`): a `databases.query` to find matching pages, then a bulk operation
  (`pages.update`) on each result. Used by `Delete` and `Update`.

### Two-Phase Execution
The pattern used by `Delete` and `Update`:
1. `_setup_execution` calls `databases.query` (with optional filter from `.where()`) to collect
   the Notion page IDs of affected rows.
2. `_execute_many` calls `pages.update` on each collected page ID via `do_executemany`.

### ValuesBase
The DML base class for statements that carry a `VALUES` clause (`.values()`). Parent of `Insert`
and `Update`. Provides incremental, generative value accumulation and supports single-parameter
mode only (no multi-parameter bulk for `Update`).

### UpdateBase
The DML base class for all statements. Carries the target `Table` and the `.returning()` API.
Parent of `ValuesBase` and `Delete`.

### RETURNING (explicit)
Declared via `.returning(*cols)`. Triggers a post-execution fetch of the affected pages:
- `Insert`: fires `pages.retrieve` calls after `pages.create` (post-fetch pattern).
- `Delete` / `Update`: the `pages.update` response is the full Notion page — no separate retrieve
  needed. `_finalize_execution` routes to `_staged_result_cursor`.

### implicit_returning
An execution option (`implicit_returning=True`). When set without an explicit `.returning()` call,
the object IDs of affected rows are collected into
`CursorResult.returned_primary_keys_rows` after execution. Supported by `Insert`, `Delete`, and
`Update`.

### update_payload
A key emitted into `compiled_dict` exclusively by `visit_update`. Holds the VALUES template
(`{"name": ":name", "grade": ":grade"}`) for the `pages.update` properties payload. Distinct from
`payload` (the `databases.query` filter). Bound in `Update._setup_execution` using
`context.resolved_params`.

### resolved_params
An `ExecutionContext` attribute populated during `pre_exec`. Holds the `BindParameter` objects that
remain after the query filter params have been consumed by `_bind_params`. For `Update`, this
carries the column VALUES params deferred to `_setup_execution`. The `_assert_all_params_consumed`
guard is skipped for `is_update` statements.

### Relation
A normlite `Relation` is a **stateless** `TypeEngine` subclass for a column that stores links to
rows in another `Table`. Maps to a Notion **relation property** (`single_property`, unidirectional
in v1). Its sole responsibility is value translation between Python and Notion JSON:
- Python value representation: `list[str]` (a list of Notion page ID strings).
- `bind_processor` converts `list[str]` → `{"relation": [{"id": v} for v in value]}`.
- `result_processor` converts `{"relation": [{"id": "...", ...}]}` → `[d["id"] for d in value["relation"]]`.
- `get_col_spec()` returns `"relation"`.
- `get_notion_spec()` returns the partial DDL spec `{"relation": {"single_property": {}}}` —
  without `database_id`, which is not available at TypeEngine level.
`Relation` holds no mutable state. It has no `set_oid()` method and no `_oid` field.
Supported filter operators: `CONTAINS`, `DOES_NOT_CONTAIN`, `IS_EMPTY`, `IS_NOT_EMPTY`.

### ForeignKey
A constraint object passed alongside `Relation()` in `Column(...)`: e.g.
`Column("students_oid", Relation(), ForeignKey("students.object_id"))`.
Stores a string reference `"<table>.<column>"` parsed at construction into `fk.table_name` and
`fk.column_name`. Also holds `fk.database_id: str | None`, initially `None`.
Always targets the `object_id` system column (`SpecialColumns.NO_ID`) of the referenced table;
`ForeignKeyConstraint.resolve()` raises `ArgumentError` if `column_name` is anything else.
`ForeignKeyConstraint.resolve()` sets `fk.database_id = target_table.get_oid()` — this is the
Notion database UUID of the referenced table, read from its `object_id` system column.

### Relation — DDL compilation
`_compile_table_columns` assembles the full Notion property dict for each user column by calling
`col.type_.get_notion_spec()`. For `Relation` columns it then merges `database_id` from the
column's `ForeignKey` value object (`col.foreign_keys`):

```python
spec = col.type_.get_notion_spec()          # {"relation": {"single_property": {}}}
fk = next(iter(col.foreign_keys))
spec["relation"]["database_id"] = fk.database_id
# result: {"relation": {"database_id": "...", "single_property": {}}}
```

`fk.database_id` is guaranteed non-`None` by the time the compiler runs because
`MetaData.create_all()` calls `resolve()` before `table.create()`.

### Relation — Reflection
During `Table._autoload(engine)`, a Notion relation property is reflected by:
1. Extracting `database_id` from the Notion property spec.
2. Calling `engine._catalog.find_sys_tables_row_by_table_id(database_id)` (a new `SystemCatalog`
   method filtering on the `table_id` rich_text field).
3. If found: constructing `Column("<name>", Relation(), ForeignKey(f"{entry.name}.object_id"))`,
   then setting `fk.database_id = database_id` directly on the `ForeignKey` value object —
   no deferred resolution needed.
4. If not found: the related table was not created by normlite; emit a warning and skip the
   property (never silently).

### ForeignKeyConstraint
A `Constraint` auto-generated during `Table.__init__` for every column that carries a `ForeignKey`.
Mirrors how `PrimaryKeyConstraint` is auto-generated. Registered in `Table._constraints`.
`ForeignKeyConstraint.resolve(metadata)` parses `fk.table_name` / `fk.column_name`, validates
`column_name == SpecialColumns.NO_ID`, looks up the target table in `metadata.tables`, calls
`target_table.get_oid()`, and stores the result as `fk.database_id` on the `ForeignKey` value
object. Does **not** mutate the `Relation` TypeEngine instance.
`Table.foreign_keys` is a public property returning `Set[ForeignKeyConstraint]`.
Invariants enforced at `Table.__init__` time: (1) every `Relation()` column must carry exactly one
`ForeignKey` — raises `ArgumentError` if not; (2) cycles in the FK graph raise
`CircularDependencyError` from `MetaData.sorted_tables`.

### MetaData.sorted_tables
A property (mirroring SQLAlchemy) that returns all registered tables in topological dependency
order — independent tables first, dependent tables last — derived from `ForeignKeyConstraint`
relationships. Used by `MetaData.create_all()` to drive the creation order. Raises
`CircularDependencyError` (from `normlite.exceptions`, subclass of `NormliteError`) when a cycle
is detected in the FK graph — same name and semantics as SQLAlchemy's `CircularDependencyError`.

### MetaData.create_all()
Creates all tables registered in the MetaData using `sorted_tables` order. Before creating each
table, it resolves all `ForeignKeyConstraint` objects on that table by calling
`fk_constraint.resolve(metadata)`, which sets `fk.database_id` on each `ForeignKey` value object
(now available because the target table was created first and its `object_id` is known).
`Calling `Table.create()` directly on a table that has unresolved `Relation` columns (i.e.
`fk.database_id is None`) raises `ArgumentError`.

---

## Fake Client (InMemoryNotionClient)

### Fake client — Relation property support
`InMemoryNotionClient` stores and filters Notion `relation` properties so DML against
FK-bearing tables can round-trip without hitting the real Notion API.

**Storage (`pages_create` / `pages_update`).** The generic property-writing path already
accepts relation arrays (no special-casing). `_normalize_property` validates the *shape*
of a relation value: `prop["relation"]` must be a list, each item must be a dict with a
string `"id"`. Malformed shapes raise `NotionError`. Update semantics are **full
replacement** — sending `{"relation": []}` clears the link, mirroring real Notion.

**Filtering (`databases_query`).** `_Condition` is extended in place: `"relation"` is
added to `_allowed_ops` with `{contains, does_not_contain, is_empty, is_not_empty}`, and
the matching `_op_map` entries operate on the stored `list[{"id": ...}]` shape. `eval()`
gains a `relation` branch that extracts `self.property_obj["relation"]` as the operand.

**Lax FK-target validation.** The fake does **not** verify that page IDs in a relation
property point to existing pages, nor that they belong to the FK-target database. This
matches real Notion's lazy behaviour: invalid IDs surface as empty join results, not as
errors. See [[adr-0002-fake-client-lax-fk-validation]].

**Deferred — `has_more` truncation.** Real Notion truncates relation property arrays past
~25 entries on page retrieval and exposes `has_more: true` with a separate
`properties_retrieve` pagination endpoint. The fake currently returns the full list with
no truncation. `Select.join()` v1 (PRD #302) ships **against in-memory clients only**, so
this divergence is deferred **until a real Notion integration is built** — not resolved by
the join work. Joins that paginate large relations in production must not silently rely on
the fake's non-truncating behaviour. See [[adr-0007-join-dangling-fk-propagation]] for the
shipped join FK-resolution contract and PRD #302 for the integration deferral.

---

## Engine Setup

### root_page_id
The Notion-side identifier of the top-most parent under which the engine's `information_schema`
page and user databases are anchored. Every page/database in normlite needs a parent; this is the
ultimate ancestor of the tree.

The semantics differ by integration kind:
- **Simulated integrations** (`InMemoryNotionClient`, `FileBasedNotionClient`): the client mints
  its own synthetic root page (`_ROOT_PAGE_ID_`). The engine defaults
  `self._root_page_id` to that constant when the user does not supply one. **Not required** from
  `create_engine` callers.
- **Real Notion integrations** (`normlite+auth://internal`, when implemented): the root must be a
  real Notion workspace page the integration was granted access to. The client cannot invent it.
  **Required** from `create_engine` callers, and validated at engine construction.

Both memory- and file-mode simulated engines apply the same fallback symmetrically — `root_page_id`
is never *required* for simulated URIs, only optional. An earlier inconsistency where file-mode
demanded a user-supplied `root_page_id` was a latent bug; both simulated branches now share the
"default to the client's `_ROOT_PAGE_ID_`" rule.

---

## Engine Lifecycle

### Engine.dispose()
Terminal disposal of an `Engine`. Releases backend resources by calling `self._client.close()`
— a no-op for `InMemoryNotionClient` and a flush-to-disk for `FileBasedNotionClient`.
After `dispose()` has been *called* (whether it returned cleanly or raised), the engine is
**terminal**: `engine.connect()` and `engine.raw_connection()` raise
`InvalidRequestError("Engine has been disposed")`. User-held `Connection` objects are not
tracked; they fail at their next `execute()` because the path goes through
`Engine.raw_connection()`.
Idempotent: calling `dispose()` on an already-disposed engine is a silent no-op.
Cascade is minimal — only `self._client` is closed; `self._dbapi_connection` and `self._catalog`
hold no I/O state of their own and are not given a `close()` method.

**Error handling.** `self._client.close()` is wrapped in `try / finally`. If close raises
(e.g. `OSError` from a failed `FileBasedNotionClient.flush()`), the exception propagates
unchanged and `self._is_disposed` is still flipped to `True` in the `finally` block. The
engine cannot be reused or retried after a failed close — retry can't recover the partially
written file anyway, and continuing to mutate an unsynced in-memory store is the worse trap.

A read-only `engine.disposed` property exposes the state for tests and user code.

**Context-manager support.** `Engine` implements `__enter__` / `__exit__`. `__enter__` returns
`self`; `__exit__` calls `self.dispose()` unconditionally and returns `None` (never swallows
exceptions). Idempotency keeps an explicit `engine.dispose()` inside a `with` block safe.
Mirrors the CM idiom already present on `FileBasedNotionClient`.

See [[adr-0003-engine-dispose-semantics]] for the architectural rationale.

### read_only (engine kwarg)
A flat top-level kwarg on `create_engine`, plumbed through `Engine.__init__(**kwargs)` to
`FileBasedNotionClient(path, read_only=True)`. Enables test fixtures that read a saved
store from disk without mutating the file on exit.
Valid only for file-mode URIs; supplying `read_only=True` together with a `:memory:` URI raises
`ArgumentError("read_only is only valid for file-based URIs")`.

If `read_only=True` **and** `auto_load=True` (the default) are both supplied but the target file
does not exist, `FileBasedNotionClient.__init__` raises
`NotionError("Invalid request URL: <path>", status_code=400, code="invalid_request_url")`.
This treats the file path as the client's effective request URL and mirrors Notion's real
`invalid_request_url` error code. Rationale: the whole point of `read_only` + `auto_load` is to
read an existing fixture; silently producing an empty in-memory store from a mistyped path turns
the feature into a footgun.

The gating is the **intersection** of both flags, not `read_only` alone — `read_only=True` +
`auto_load=False` is a legitimate in-memory-scratchpad pattern (used internally by tests that
exercise flush/close no-op semantics) and must continue to construct successfully against a
missing path. The converse case (`read_only=False` + non-existent path) is also unchanged — it
remains the "create a new file-backed engine" path regardless of `auto_load`.

---

## Pagination

### Streaming result over token pagination
normlite's answer to large result sets. Modelled on SQLAlchemy's server-side cursor
(`stream_results` / `yield_per`) but **explicitly not** a real server-side cursor: Notion holds
no cursor object and offers no `FETCH`. The mechanism is **token pagination** — the DBAPI cursor
lazily requests the next Notion page (`start_cursor` ← previous response's `next_cursor`) only when
its in-memory buffer drains, so the caller iterates `Row`s without the whole set being resident.

Notion's pagination contract (the wire shape every paginated endpoint returns):
`object: "list"`, `results: [...]`, `has_more: bool`, `next_cursor: str | None` (present only when
`has_more` is true), with request params `start_cursor` and `page_size` (default **100**, max
**100**). "Server-side cursor" may be used in *user-facing docs* as an analogy, but never as the
canonical domain term — the result set is **not** stable across page requests.

### Drain-all (default) vs streaming (opt-in)
Two behaviors share one page-fetch seam:
- **Drain-all (default).** The DBAPI `Cursor` pulls every page (`start_cursor` ← previous
  `next_cursor`) until `has_more` is false, before the caller sees rows. This makes *every*
  existing caller — joins, two-phase `Delete`/`Update`, reflection, bootstrap — correct against
  pagination for free, and preserves today's "you get everything" semantics and `rowcount`.
- **Streaming (opt-in).** Enabled via `execution_options(stream_results=True)` or `yield_per=N`
  (`yield_per` implies `stream_results`), mirroring SQLAlchemy. Pages are pulled lazily as the
  result is iterated. **Select-only**: mutations, join phase-1 scans, reflection and bootstrap
  always force drain-all even if `stream_results=True` is set on the connection — phase 1 must
  know the full match set before phase 2 runs.

`page_size` is **internal and capped**: `min(yield_per or 100, 100)`. `yield_per` (the user's
logical batch) is decoupled from `page_size` (Notion transport, ≤100); `yield_per > 100` pulls
multiple Notion pages per logical batch. `page_size` is not a user knob in v1. It is injected into
the request body only when `yield_per` is set, on a per-request payload copy (never smearing onto
the caller's dict); drain-all (`yield_per is None`) leaves the caller's payload `page_size` alone.

**Pagination is an `execute`-only concern.** `executemany` is the non-paginated bulk-write path:
one client call per parameter set, no `next_cursor` walk, skip-and-continue error handling. It
returns no streamable result set (per DBAPI 2.0, `executemany` is not for row-returning ops), so
neither drain-all nor streaming applies — this boundary is **by design, not deferred**.

### Pagination — internal representation
A paginated query is **one streaming `ResultSet`**, not one `ResultSet` per page — pages are an
axis orthogonal to the `Cursor._result_sets` list, which keeps its existing meaning (one entry per
statement / `executemany` batch). `nextset()` still means "next statement," untouched. The
page-fetch seam is a standalone **`PageIterator`** (`notiondbapi/page_iterator.py`): the `Cursor`
builds a `page_fetcher(start_cursor)` closure (it owns `operation`/`parameters`/client and injects
`start_cursor`/`page_size` into the POST **body** of `databases.query`) and holds the iterator as
`Cursor._page_iter`; the `PageIterator` carries `next_cursor`/`has_more`/`exhausted`/`page_size`.
`ResultSet` grew `extend_from_json(page)` to append a page's rows into its buffer in place. The
**first page is always eager** (it establishes the description and is where `NotionError`→DBAPI
translation happens). Subsequent pages are eager (the drain-all `for` loop in `execute`) or lazy:
`fetchone` pulls the next page only when the in-memory buffer drains, and `fetchall` drives that
pull to completion.

### Pagination — rowcount & errors
- **`rowcount`.** Drain-all: accurate immediately. Streaming: **-1 until the `ResultSet` is
  exhausted** (`has_more` false), then the true sum. `preserve_rowcount` memoizing -1 mid-stream
  is correct, not a bug. The two-phase `Delete`/`Update` "rows matched" guarantee is meaningful
  only on the drain-all path (mutations never stream).
- **Mid-stream errors.** A page fetch that fails *during iteration* routes through the centralized
  `_translate_notion_error` and **propagates** (does not skip-and-continue like `executemany`) —
  a broken page makes all later pages unreachable. Already-yielded rows stay yielded.

### Pagination — boundaries (deferred)
- GET-style paginated endpoints (list users, block children) take `start_cursor`/`page_size` as
  **`query_params`**, not body; the v1 closure hard-codes body-injection for `databases.query` only.
- `search` and `_get_by_title` stay single-page (`has_more: False`) in the fake; only
  `databases_query` simulates pagination.
- `CursorResult.partitions(n)` (idiomatic batch consumer) deferred; `__iter__`/`fetchone`/
  `fetchmany` stream, `all()`/`fetchall()` materialize-by-draining (documented, not blocked).

See [[adr-0010-streaming-result-token-pagination]].

---

## DML Construct Decisions

### Update — partial VALUES
`update()` accepts a **subset** of user columns in `.values()`. Unspecified columns are left
unchanged on the Notion page. This differs from `Insert`, which requires all user columns.

### Update — WHERE optional
Omitting `.where()` on an `Update` updates **all rows** in the table (queries all pages, then
updates each). This mirrors the same behaviour in `Delete`.

### Update — parameters= not supported (v1)
`connection.execute(update_stmt, parameters={...})` raises `ArgumentError` in the first version.
Values must be supplied via `.values()`. SQLAlchemy supports per-row parameters; this may be added
in a future version.

### Update — CompileError on missing VALUES
`visit_update` raises `CompileError` if `_values is None` at compile time. A zero-column update
with no `parameters=` fallback is always a programming error.
