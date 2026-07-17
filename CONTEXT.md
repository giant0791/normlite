# normlite Domain Glossary

normlite is an SQLAlchemy-like frontend that maps SQL-style DML/DDL constructs onto the Notion API.
Each SQL concept maps to one or more Notion API calls; the glossary below captures those mappings
and the decisions made about them.

---

## Core Concepts

### Table
A normlite `Table` corresponds to a **Notion database** containing exactly one **Data Source**
(see below). Creating a table calls `databases.create`; dropping it calls `databases.update` with
`in_trash: True`.

### Data Source
> **Status: agreed design for the Notion API `2025-09-03` upgrade — see
> [ADR-0014](docs/adr/0014-data-source-two-id-identity.md). Not yet implemented; the entries below
> marked _(2025-09-03)_ describe the target, and the current code still uses the single-ID
> `database_id` shape.**

Under `2025-09-03` a Notion **database** (container: `title`/`icon`/`cover` + a `data_sources`
list) is split from its **data source** (the `properties` schema + the queryable surface + the
target of relations). normlite keeps the invariant **one `Table` = one database = one data
source**; multi-data-source databases are deferred.

Each `Table` therefore carries **two IDs**:
- **`object_id`/`get_oid()` = the database UUID** — unchanged identity; the thing a page-child
  hangs off and `DROP TABLE` trashes. Used only by create / drop / container operations.
- **`data_source_id` = a hidden system column** (in `_sys_columns`, captured/read by name via
  `get_data_source_id()`) — carried like any other system column but **excluded from the public
  `table.c` view** and from every query projection and page-result description (see *Hidden system
  column* under §User Column). Used by *everything operational*: `data_sources.query` (SELECT +
  two-phase UPDATE/DELETE), `data_sources.retrieve` (reflection), the `pages.create` parent, and
  relation schema specs.

Both IDs are persisted: `databases.create` returns the container plus `data_sources[0].id`, and
the `tables` catalog stores `data_source_id` alongside `table_id`. Reflection is **catalog-first**
— it reads `data_source_id` from the catalog row and calls `data_sources.retrieve` directly.

A data source's **name is its `title`** (rich text), equal to the database title under the
single-source invariant. **Orphan detection** — `get_table_state` for a table with *no* catalog
row — finds a stray table by searching **data sources** on that title (search yields `data_source`
objects, never `database`, per ADR-0014); a hit with no catalog row is `ORPHANED`.

The `tables` catalog **row schema** (final order): `table_name` (title), `table_schema`,
`table_catalog`, `table_id`, **`data_source_id`** (rich_text, per ADR-0014), **`is_dropped`**
(checkbox, per [ADR-0015](docs/adr/0015-catalog-soft-delete-explicit-property.md)).

### Catalog soft-delete (table lifecycle)

`get_table_state` derives `TableState` from **two independent flags**: normlite's recorded intent
(`is_dropped` on the catalog row) and Notion's physical reality (the table's database trash state,
read via `databases.retrieve(table_id)`). Both dropped → `DROPPED`; both live → `ACTIVE`;
**disagreement → `ORPHANED`** — the disagreement case is why the two flags stay independent.

The intent marker is an explicit **`is_dropped` checkbox on the catalog row**, *not* Notion
page-trash: `data_sources.query` unconditionally skips trashed pages (ADR-0014), so a page-trashed
catalog row would be invisible and `DROPPED`/`RESTORE` unobservable. `set_dropped` flips the
checkbox and the catalog row **stays live**. The physical `DROP TABLE` still trashes the *database
container* (`databases.update {in_trash}`) — unchanged. See
[ADR-0015](docs/adr/0015-catalog-soft-delete-explicit-property.md).

### Dropped = non-existent (SQL-destructive DROP)

To **DDL operations**, a `DROPPED` table is indistinguishable from a `MISSING` one — normlite is
SQL-like, and in SQL a dropped table is gone with no restore (see
[ADR-0016](docs/adr/0016-dropped-table-is-non-existent.md)). `CREATE` on a dropped table yields a
**fresh** table — internally by *repurposing* the single leftover catalog row onto a new database
(overwrites `table_id` + `data_source_id`, clears `is_dropped`); the old database is abandoned in
Notion trash (no hard-delete exists; a human can still recover it via the Notion UI). `DROP` on a
dropped table → `"does not exist"` (no-op under `checkfirst`); reflection → `NoSuchTableError`.
`ORPHANED` still raises `InternalError` everywhere. There is **no user-facing restore**; the
soft-delete machinery stays only as a latent foundation. The split is deliberate: DDL *operations*
collapse `DROPPED` into `MISSING`, but the diagnostic `get_table_state` **still distinguishes**
`DROPPED`, keeping the machinery observable to future tooling.

_Boundary — `get_or_create_sys_tables_row`:_ because a dropped row stays live, its dropped-row
fall-through must never insert a *second* live row (the next `find_sys_tables_row` would trip
`len > 1`). It is hardened defensively — a dropped row is treated as existing, never duplicated;
the repurpose above owns the create-after-drop path.

### Row / Page
A single row in a `Table` corresponds to a **Notion page** inside that database.
All DML that reads or mutates rows operates on Notion pages.

### Column / Property
A `Column` in a `Table` corresponds to a **Notion database property**. Each column has a `TypeEngine`
subclass that maps to the Notion property type and owns the bind/filter value processors.

### User Column
A column explicitly declared by the user in `Table(...)`. Distinct from **system columns** (e.g.
`object_id`, `_no_id`) which are injected by normlite and carry Notion metadata.

**Hidden system column**: a system column that normlite captures in `_sys_columns` (read by name,
e.g. `get_data_source_id()`) but that is **excluded from the public `table.c` view** and from every
query projection and page-result description — never user-visible. `data_source_id` and `table_name`
are the two (see [ADR-0017](docs/adr/0017-hidden-system-columns.md)).

`data_source_id`, though carried as a system column, is a hidden system column: **excluded from the
public `table.c` view** and from every page-result description (`SchemaInfo.from_table`). A Notion
*page* has no `data_source_id` key and a SQL user never selects it (ADR-0014); it is routing plumbing
captured by name via `get_data_source_id()`, never a returned column — the same rule the SELECT
projection applies (`e0647cd`), now enforced at the `table.c` source.

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

`visit_select` additionally emits `query_params` and — on the join path — `joins`,
`join_right_filter`, and `join_right_sorts`. The latter two are **Residual**s (see §Query
Planning & Execution) that have no home in a payload and ride the `compiled_dict` as ad-hoc keys;
they are re-exposed as `context.join_right_filter` / `context.join_right_sorts` and fed to
`JoinExecution.__init__`. This makes `visit_select` a **Planner** in all but name. Both keys are
**retired** by the agreed design below — they move onto the **PlanningContext**, which is the
supported channel for exactly this (compile-time decision → run time).

`compiled_dict` is **pure JSON-like data** — no AST objects (`visit_join` emits only names and a
bool). This is a real invariant: anything that must reach run time as an AST rides the
**PlanningContext** on the `Compiled`, never `compiled_dict`.

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

### Layers (DBAPI / SQL / ENGINE)
> **Status: intended architecture — partially violated today.** The execution hooks break the
> `SQL → DBAPI`-only rule; the **Query plan** is the mechanism restoring it for `SELECT`. See
> [[adr-0020-execution-layering-sql-builds-engine-drives]].

Three layers, one dependency direction — **ENGINE → SQL, ENGINE → DBAPI, SQL → DBAPI; nobody calls
upward**:

- **DBAPI** (`notiondbapi/`) — the I/O adaptation layer. Owns the PEP-249 `Cursor`, pagination
  mechanics (`PageIterator`, `start_cursor`, eager-vs-lazy page pull), and Notion transport.
- **SQL** (`sql/`) — the AST and semantics layer. Owns `ClauseElement`s, compilation, and the
  *description* of what a statement means and needs. Builds descriptions; does not perform I/O.
- **ENGINE** (`engine/`) — the orchestration layer. Owns `Connection._execute_context`, execution-
  style dispatch, and cursor routing.

**The invariant for execution**: SQL *builds* the plan (data), ENGINE *drives* it (the drive loop
lives in the engine, not the hook), and `CursorResult` is the *pull consumer* for streaming.
`Select._setup_execution` calling `context.engine.do_execute` (`dml.py:832`) is the current
violation — the SQL layer *performing* I/O rather than describing it. Knowledge crosses the boundary
as the plan; never as an engine-side `isinstance(stmt, Select)` switch (that pulls SQL knowledge
*up*). Restoration is staged: inert plan (#361) → engine drives blocking plans (#364, "Move A") →
`CursorResult` pulls the plain streaming path through the plan ("Move B", a later PRD). _Avoid_
moving pagination mechanics out of DBAPI: ENGINE owns fetch *policy* and the *drive*, DBAPI owns the
mechanics.

### Execution Pipeline
The sequence: compile → `pre_exec` (bind params) → `_setup_execution` (Notion API call(s)) →
`_execute_*` → `post_exec` → `_finalize_execution` (cursor routing).

**This term names the engine's statement-lifecycle hook sequence and nothing else.** It is a
linear sequence driven by `Connection._execute_context`. It is *not* the **Query plan** (the tree
of **Operator**s that computes a SELECT's rows — see §Query Planning & Execution); a plan is a
tree, not a pipeline, and most of its nodes block. Do not call the plan a "pipeline".

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
  without the target ID, which is not available at TypeEngine level.
  _(2025-09-03: the merged target ID becomes `data_source_id`, not `database_id` — see
  [ADR-0014](docs/adr/0014-data-source-two-id-identity.md). Relation **values** are unchanged.)_
`Relation` holds no mutable state. It has no `set_oid()` method and no `_oid` field.
Supported filter operators: `CONTAINS`, `DOES_NOT_CONTAIN`, `IS_EMPTY`, `IS_NOT_EMPTY`.

### ForeignKey
_(2025-09-03: `fk.database_id` is renamed `fk.data_source_id` and resolves to the target table's
data source, not its database UUID — see
[ADR-0014](docs/adr/0014-data-source-two-id-identity.md). The public
`ForeignKey("students.object_id")` reference is unchanged.)_

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
4. If not found: emit a warning and skip the property (never silently). "Not found" now has two
   distinct causes, both indistinguishable to reflection: the related table was never created by
   normlite, **or** it was created but has since been dropped — a dropped table's catalog row is
   trashed, and `data_sources.query` always skips trashed pages (the 2025-09-03 endpoint has no
   `in_trash` parameter; see [ADR-0014](docs/adr/0014-data-source-two-id-identity.md)). A relation
   to a dropped table is therefore unresolvable by name; its target database is trashed anyway, so
   the FK would be dangling.

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

> _(2025-09-03 upgrade — [ADR-0014](docs/adr/0014-data-source-two-id-identity.md), not yet
> implemented: the fake stores `database` and `data_source` as **separate** objects with their own
> IDs and a parent link; enforces **per-child-type** parent validation (page → `page_id`/
> `data_source_id`; database → `page_id`; data_source → `database_id`); replaces `databases_query`
> with `data_sources_{retrieve,update,query}` outright; and `search` returns `data_source` objects.
> Persisted stores from before the upgrade are a **clean break** with a loud old-format guard.)_

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

The name collides: a `page_size` **execution option** also lives in the options cascade (default
100, its cascade/merge pinned by `test_exec_opts.py`) — but it is **vestigial, inert on the query
path**: it never reaches the request body. The body's `page_size` comes from the compiler (the
Notion-max 100) and, when streaming, from the `Cursor` overriding it with `min(yield_per or 100,
100)`. So `yield_per` is the only effective page-size control — when both are set, `yield_per`
wins. Declaring streaming on `execution_options` therefore means `stream_results`/`yield_per`,
**not** `page_size`; retiring the vestigial option is a deferred cleanup (it is still test-pinned).

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

## Check Constraints

### CheckConstraint (client-side enforcement)
See [[adr-0012-checkconstraint-client-side-enforcement]] (enforcement + error) and
[[adr-0013-checkconstraint-catalog-persistence]] (persistence + reflection contract).

A `CheckConstraint` carries a boolean **predicate** over a `Table`'s columns
(`CheckConstraint(products.c.price > 0)`) and an optional `name`. Notion's
`databases.create`/`update` API has **no CHECK facility**, so — exactly as with aggregate
functions ([[adr-0011-aggregate-execution-seam]]) — the constraint **has no Notion backend**
and is enforced **client-side by normlite**: the predicate is evaluated before a row is
written, and a violating row is rejected rather than sent to Notion. A `CheckConstraint`
that never rejects anything would be a footgun, so declarative-only / no-op semantics were
explicitly ruled out.

There is **one construct**, not two: "row-level" (`price > 0`) vs "table-level"
(`discount < price`) is only informal prose describing how many columns the predicate
touches — both are a single boolean predicate over one table's columns, evaluated per-row.

### CheckConstraint — declaration & attachment
Declared **inline in the `Table(...)` constructor**, positionally, alongside `Column`s
(`Table.__init__` accepts `Constraint` args, not only `Column`s):

```python
price = Column("price", Number()); discount = Column("discount", Number())
products = Table("products", metadata, price, discount,
                 CheckConstraint(price > 0, name="check_positive_price"),
                 CheckConstraint(discount < price, name="check_discount_rules"))
```

Inside the constructor the predicate references the **bare `Column` objects** (their
`.parent` is still `None`; `Table` adopts them during construction) — NOT `products.c.price`,
which does not exist until `products` does. The post-hoc escape hatch
`products.add_constraint(CheckConstraint(products.c.price > 0))` also works (same path FK/PK
use). Both feed the same `Table._constraints` set.

### CheckConstraint — column-to-column comparisons in the AST
A table-level predicate like `discount < price` is a **column-to-column** comparison. Today
`Comparator.operate` calls `coerce_to_bindparam(other, ...)`, which (a `Column` is not
callable) wraps the RHS `Column` as a literal `BindParameter.value` — silent garbage.
Fix: `Comparator.operate` **preserves a `ColumnElement` RHS as-is** instead of coercing it;
`BinaryExpression.value` therefore becomes "either a `BindParameter` (literal) **or** a
`ColumnElement` (column ref)", and the client-side check-evaluator resolves both operands
against the row. A column-RHS comparison is **not expressible as a Notion `databases.query`
filter** (Notion compares a property to a literal, never to another property), so the
WHERE→filter compiler (`visit_binary_expression`) **raises loudly** if it ever sees a
`ColumnElement` value — a col-col predicate handed to `.where()` fails fast instead of
compiling to a broken filter.

### CheckConstraint — enforcement seam (v1: Insert-only)
Enforcement is owned by a **dedicated single-owner seam** (`CheckEnforcement`), mirroring the
[[adr-0011-aggregate-execution-seam]] config-to-constructor discipline: built from the target
table's `CheckConstraint`s, it exposes a `.check(row_values)`-style call that raises on
violation. v1 enforces **`Insert` only**; `Update` (which carries only a partial `.values()`
subset and would need to merge new values over the decoded existing page image) is a
**deferred follow-up slice**. `Delete` never enforces.

- **Row image:** evaluated against the **Python values pre-bind** (`Decimal('5')`, `-2`),
  keyed by column name — NOT the Notion JSON cells. The evaluator resolves each operand as
  `row[col.name]` and applies the operator in Python.
- **When:** right before the `pages.create` call(s), in the `Insert` setup path. A
  **multi-row** (`INSERTMANYVALUES`) insert checks every row; the whole statement fails
  fast on the first violating row (no partial commit). All of the table's `CheckConstraint`s
  are evaluated for each row.

### CheckConstraint — NULL / three-valued logic (SQL-faithful)
A check **rejects a row only when the predicate evaluates to `False`**. A `None` operand
(Notion has no NULL — an empty/omitted property is Python `None`, per
[[adr-0005-outer-join-phantom-null-semantics]]) makes the comparison **UNKNOWN → row
accepted**, exactly as SQL `CHECK` and the aggregate skip-NULL discipline
([[adr-0011-aggregate-execution-seam]]). "Value must be present" is a NOT-NULL concern, a
separate future constraint — not a CHECK.

### CheckConstraint — violation error
A violation raises DBAPI **`IntegrityError(DatabaseError)`** (new class in
`notiondbapi/dbapi2.py`), the DBAPI-2.0-canonical error for "relational integrity affected —
a constraint check failed" (also what SQLAlchemy surfaces). The message carries the
constraint `name` (or the predicate repr when unnamed) and the offending column/value.
NOTE the deliberate layering tension: `CheckEnforcement` runs **client-side, above the DBAPI
boundary**, yet raises a DBAPI-layer error — accepted for SQL/DBAPI fidelity (the *meaning*
is exactly DBAPI `IntegrityError`). Candidate ADR.

### CheckConstraint — evaluator (new, Operator-enum-driven)
A **new small tree-walking evaluator** (owned by / beside `CheckEnforcement`), NOT the fake
client's `_Filter`/`_Condition` (that lives in `notion_sdk/client.py`, speaks Notion raw-cell
shape + Notion op strings, has no col-col support — reusing it would be a layering inversion).
The evaluator walks the predicate tree — `BinaryExpression`, `BooleanClauseList` (and/or),
`UnaryExpression` (not) — resolving `Column` operands to `row[col.name]`, `BindParameter`
operands to their value, dispatching on the backend-agnostic `Operator` enum with the
three-valued logic above. **Compound predicates are supported in v1**
(`CheckConstraint((price > 0) & (discount < price))`) — the tree-walker handles them
naturally.

### CheckConstraint — construct/attachment validation (fail-fast)
Validated when the table **adopts** the constraint (`Table.__init__` after columns are bound,
and `add_constraint` for the post-hoc path) — not at `CheckConstraint(...)` build time, where
inline columns are still parent-less. Two structural invariants, both raising `ArgumentError`:
1. **Same-table** — every `Column` referenced in the predicate must belong (by identity) to
   the attaching table; cross-table refs are rejected.
2. **Boolean predicate** — the argument must be a boolean-valued expression
   (`BinaryExpression`/`BooleanClauseList`/`UnaryExpression`), not a bare `Column` or literal.

**No operand-type policing in v1** — any `Operator` the Python evaluator can compute is
accepted (no rejecting `is_empty()`, `Relation` columns, or type/op mismatches), mirroring the
type-agnostic `func.count(col)`.

### CheckConstraint — catalog persistence & reflection
Checks have no Notion-database backend, but they ARE persisted in normlite's own
`information_schema` catalog so reflection can rebuild them (they are NOT lost on reflection).
Storage is a **dedicated `check_constraints` catalog database** under `information_schema`
(sibling of `tables`, mirroring SQL `information_schema.check_constraints`), bootstrapped with
one more `_get_or_create_database` call. Rows carry `constraint_name` (title),
`table_catalog` + `table_name` (owning table), and `check_clause`. The `check_clause` stores a
**JSON AST** serialization of the predicate tree (NOT a textual `"price > 0"` clause — normlite
has no SQL-text parser, so JSON round-trips without one). `CreateTable`/`create_all` writes the
rows after `databases.create`; `DropTable` removes/marks them; reflection reads them back and
**rebuilds `CheckConstraint` objects, binding column refs to the reflected table's columns by
name**.

### CheckConstraint — `check_clause` JSON-AST format (persisted data contract)
Because these blobs are stored in Notion and must stay readable across normlite versions, the
format is a versioned data contract (`"v": 1` root field). Node kinds:
- `compare` — `{"kind":"compare","op":"<Operator enum name>","left":{col},"right":{col|lit}}`.
  `op` is the `Operator` **enum name** (`"GT"`, `"LT"`, `"EQ"`, … — backend-agnostic, stable).
  `left` is **always** a `col` (the LHS of a comparison is always a `Column`); `right` is a
  `col` (col-col) or a `lit`.
- `bool` — `{"kind":"bool","op":"and|or","clauses":[…]}` (`BooleanClauseList`).
- `not`  — `{"kind":"not","operand":{…}}` (`UnaryExpression`).
- `col`  — `{"kind":"col","name":"price"}`; rebound to the reflected table's column by name.
- `lit`  — value stored as a **string**, **re-coerced through the paired column's `TypeEngine`**
  on deserialize (`Decimal("0")` via `price.type_`) — dodges JSON float-precision loss and
  needs no per-literal type tag (every literal is paired with a known column in its `compare`).

### CheckConstraint — v1 slicing
1. construct + col-col AST change + WHERE guard + structural validation (no execution).
2. `CheckEnforcement` + evaluator + `IntegrityError`, wired into Insert (in-memory, no
   persistence) — first end-to-end usable slice.
3. JSON-AST serializer + `check_constraints` catalog database + write-on-create / drop-cleanup.
4. reflection: deserialize + rebuild on `autoload_with`.
5. (later) Update enforcement (the deferred partial-values merge).

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

---

## Aggregate Functions

### Aggregate function (`func.sum`, `func.avg`, …)
A SQL-style **cross-row aggregate**: it reduces *all matched rows* to a single scalar
(`func.sum(Employee.salary)`, `func.avg(Employee.salary)`). Despite the original framing,
an aggregate is **not** a Notion formula. Notion *formula properties* are **per-row** scalar
expressions (spreadsheet-style: `prop("salary") * 12`, computed independently per page);
they never reduce across pages. Notion's only native cross-row reductions are **rollups**
(aggregate a property across a relation, anchored to a parent row) and **view calculations**
(the column-footer sum/avg in the UI) — and view calculations are **not exposed by the public
API**. Therefore an aggregate over a plain database **has no Notion backend**: it is computed
**client-side** by normlite, by folding over the pages returned by `databases.query`. Mapping
`func` onto Notion formula property types is explicitly a non-goal of this feature.

### Aggregate query — execution shape
The `.where()` filter still rides into the `databases.query` payload (Notion-side narrowing);
the **reduction happens after the result set drains**, in normlite. An aggregate query therefore
**forces drain-all** (you cannot average a half-consumed stream), regardless of
`stream_results` — same rule that already governs joins and two-phase mutations.

### Aggregate query — v1 scope (whole-set, all-aggregate)
v1 supports **only whole-set aggregates with no `GROUP BY`**:
- The projection must be **all aggregates** — mixing a bare page-derived column with an
  aggregate (`select(Employee.dept, func.sum(...))`) is rejected, because without `GROUP BY`
  it is a SQL error and would require the partitioning machinery below.
- The query returns **exactly one synthetic row** — a `Row` with **no backing Notion page**,
  hence no `object_id` and no per-property provenance (a documented hole in the ADR-0009
  provenance model, justified because every column in the row is synthetic and uniform).
- `GROUP BY` (one synthetic row *per partition*, grouping key a real page-derived column
  beside synthetic aggregate columns, plus the bare-column rule and `HAVING`) is a deferred
  follow-up slice — a sibling to the [[adr-0008-joinexecution-seam]] prepare/assemble pattern.

### Aggregate functions — v1 set and `count` forms
v1 ships **`sum`, `avg`, `count`**. `sum`/`avg` require a `Number` column; `avg` returns a
Python `float` even over integer columns, `sum` preserves the column's numeric type. `count`
returns `int` regardless of column type and has two forms (mirroring SQLAlchemy):
`func.count()` → row count (`COUNT(*)`), `func.count(col)` → count of **non-empty** cells.

### Aggregate functions — NULL / empty semantics (SQL-faithful)
Notion has no NULL (see [[adr-0005-outer-join-phantom-null-semantics]]); an empty property
surfaces as Python `None`. Aggregates apply **SQL skip-NULL semantics** to those empties:
- **Within the set:** `sum`/`avg` ignore pages whose value is `None`; `avg` divides by the
  **non-empty** count, not the row count. `count(col)` counts only non-empty cells;
  `count()` counts all matched rows.
- **Over zero matching rows:** `sum`/`avg` return **`None`** (SQL NULL), `count` returns `0`.
  The synthetic aggregate row may legally hold Python `None` even though Notion cannot — it
  is normlite-built, not a Notion page, so the "no NULL" rule does not bind it. Callers must
  handle `None` from `sum`/`avg`.

### `func` namespace and aggregate construct
`func` is a namespace object (mirroring SQLAlchemy's `func`): `func.sum(col)`,
`func.avg(col)`, `func.count()` / `func.count(col)`. Each call builds an **aggregate
expression** — a new `ColumnElement` node carried in `Select._projection`. `_projection`
therefore widens from "tuple of `Column`" to "tuple of `Column` **or** aggregate (**or**
`Label`)". `.label(name)` wraps the aggregate to set its result key. An aggregate has **no
table provenance**, so its `ResultColumn` carries no owning `Table` — the ADR-0009 `table`
field becomes optional (a documented hole, see the v1 scope note above). The aggregate's
`type_code` comes from its **return type** (`sum` → column type, `avg` → float, `count` →
int), not from a column.

### Aggregate result keys (auto-named, label optional)
Mirroring SQLAlchemy: an unlabeled aggregate is keyed by its **function name**
(`func.sum(salary)` → `"sum"`), with `_1`/`_2` suffixes appended on collision
(`func.sum(salary), func.sum(bonus)` → `"sum", "sum_1"`). `.label("total")` overrides the
key. This reuses the same name-collision discipline already used for join projections
(see [[adr-0009-result-schema-provenance]]).

### AggregateExecution seam
The cross-row reduction is owned by a dedicated **`AggregateExecution`** seam, mirroring the
[[adr-0008-joinexecution-seam]] config-to-constructor discipline but **single-phase**: there
is no EXECUTEMANY dispatch boundary to span (aggregation is `databases.query` drain-all →
reduce), so it is **one synchronous call**, not a `prepare`/`assemble` pair.

```
AggregateExecution(projection)
    .result_schema            # SchemaInfo, provenance-free (no owning Table)
    .reduce(drained_rows) -> (SchemaInfo, [synthetic_row])
```

Constructed and called in `Select._finalize_execution` after the result set has drained.
Keeping it in a single owner (rather than free functions in `_finalize_execution`/`CursorResult`)
is the same lesson ADR-0008 drew from the join code. See
[[adr-0011-aggregate-execution-seam]].

### Aggregate query — clause interaction guards (v1)
In the whole-set/all-aggregate v1, an aggregate projection combined with:
- **`.order_by()`** → raises `ArgumentError` (ordering a single synthetic row is meaningless;
  revisit when `GROUP BY` makes ordering partitions meaningful). Fail loud, never silently drop.
- **`.join()` / `.outerjoin()`** → raises `ArgumentError`/`CompileError` (out of scope; a loud
  guard, not a silent drop — cf. [[multi-join-silent-drop-boundary]]).
- **`.where()`** → supported: the filter rides into the `databases.query` payload as usual.
- **`yield_per` / `stream_results`** → forces drain-all (ignored for streaming purposes, like
  joins and two-phase mutations); raises nothing.

### Aggregate functions — type validation (construct-time, fail-fast)
`func.sum(col)` / `func.avg(col)` inspect `col.type_` **at construction** and raise
`ArgumentError` immediately if it is not a `Number` — the error points at the offending call,
not at a later `execute()`. `func.count(col)` accepts any type; `func.count()` takes no column.
(Contrast with `visit_update`'s compile-time `CompileError`: that guards a missing clause, not
a pure construct-time type error.)

### Aggregate query — FROM anchoring (`COUNT(*)` and `select_from`)
A whole-set aggregate query's FROM table is normally **inferred from the operand column**:
`select(func.sum(t.c.a))` anchors to `t` via `a.parent`. A columnless `func.count()`
(SQL `COUNT(*)`) has **no operand**, so there is nothing to infer from — the table must be
supplied explicitly with **`select(func.count()).select_from(t)`** (mirroring SQLAlchemy).
Construction tolerates the missing anchor (`Select._table` is left `None` until `select_from`
fills it); at compile the columnless count falls back to fetching `object_id` so each matched
page still yields exactly one row for `reduce()` to count (`len(rows)`, empties included —
unlike `count(col)`, which counts non-empty cells).

`select_from()` is **aggregate-only in v1**: calling it on a plain column/table select raises
`ArgumentError`. It is not a general explicit-FROM mechanism — that (and explicit-FROM joins it
would structurally unlock) is a deliberately guarded-out future slice. Fail loud, never
silently unlock an untested path.

### Aggregate query — rowcount
A whole-set aggregate query returns **exactly one synthetic row, always** — even over zero
matched pages (`sum`/`avg` → `None`). Hence `rowcount == 1` unconditionally. This is SQL-faithful
(a `GROUP BY`-less aggregate has result cardinality 1) and deliberately differs from the
streaming `rowcount == -1` rule in [[adr-0010-streaming-result-token-pagination]] — aggregates
never stream.

---

## Query Planning & Execution

> **Status: agreed design — not yet implemented.** The entries below describe the target. Today
> `Select._setup_execution` / `_finalize_execution` branch on `if self._joins` / `if
> self._is_aggregate` and delegate to the `JoinExecution` / `AggregateExecution` seams.
> See [[adr-0018-query-plan-operator-tree]] (structure, supersedes ADR-0008) and
> [[adr-0019-sql-null-semantics-pushdown-soundness]] (semantics, supersedes ADR-0005).

### Query plan — slicing
Structure first, semantics second, so each slice has **one reason to fail**:

1. **Tree, wrapping today's I/O** ([[adr-0018-query-plan-operator-tree]]) — `Query plan` +
   `Planner` + `PlanningContext`. The plan is **inert**: built and unit-tested, not driven; `Scan`
   takes the DBAPI `Cursor` directly (**no `QueryIO` port yet** — it would have no caller). Joins
   **still ride `ExecutionStyle.EXECUTEMANY`**: `Join` keeps the `prepare`/`assemble` split
   internally, with the hooks driving I/O exactly as today. The tree is stood up *around* the
   existing choreography — ADR-0008's own delegate-then-fold discipline (#314–#317).
   Behaviour-preserving. Slice-1 `Scan` I/O is a **provisional shim, not the target contract**:
   `next()` reads a page via `fetchmany(NOTION_MAX_PAGE_SIZE)` — a row-count stand-in that
   re-splits the whole pages `PageIterator` already produced and lifts the page-size constant into
   the SQL layer — and `open()` sets `stream_results=True`, an eager-vs-lazy *policy* that belongs
   to ENGINE (and rides a non-PEP-249 `execute()` extension). The honest contract is a
   page-granular pull on the DBAPI `Cursor` (a `fetchnextpage()`-style primitive surfacing the
   `PageIterator`'s grain) reached through the **`QueryIO` port** in slice 2, where the page size
   returns to the `Cursor` and the policy moves to ENGINE. See
   [[adr-0018-query-plan-operator-tree]] Correction (3).
2. **Drop EXECUTEMANY** ([[adr-0018-query-plan-operator-tree]],
   [[adr-0020-execution-layering-sql-builds-engine-drives]]) — the plan takes ownership of its I/O
   through the **`QueryIO` port** (its first caller); the **engine** drives **blocking** plans
   (join/aggregate); joins run as `EXECUTE`; `context.py:413`, `_join_errorhandler` and the
   staged-cursor wiring go. `Join.open()` collapses `prepare`/`assemble` into one call. The plain
   streaming path stays cursor-passthrough (streaming untouched) until **Move B** (`CursorResult`
   as pull consumer, a later PRD). Behaviour-preserving, and **this** is the slice that makes
   multi-join expressible.
3. **Semantics** ([[adr-0019-sql-null-semantics-pushdown-soundness]]) — three-valued evaluator over
   raw cells + `is_null()`; ADR-0005 superseded; `all(None)` deleted. A deliberate behaviour change
   with its own tests.
4. **Multi-join** — `Join` nests; closes [[multi-join-silent-drop-boundary]].
5. **GROUP BY / HAVING** — a blocking Operator; retires ADR-0011's "sibling seam" plan.

Slices 1 and 2 are **both** behaviour-preserving but split deliberately: slice 1 stands up new
structure while the old choreography still runs, so a failure there is a *tree* bug; slice 2
removes the choreography, so a failure there is an *I/O ownership* bug. Fusing them would make a
structural bug and a dispatch bug indistinguishable in one diff — the same reasoning that keeps
semantics out of both.

**Narrative slices vs issue numbers.** The list above is the *coarse* narrative; the GitHub issues
are finer-grained, and the two numberings do **not** line up. Narrative slice 1 spans **three**
issues (#361 `Scan`, #362 `Aggregate`, #363 `Join`/`Filter` + `PlanningContext`); narrative slice 2
is the single issue #364. So "slice 2" in prose means **#364**, which the tracker calls "slice 4".
Cite issue numbers, not narrative ordinals, when picking up work.

Narrative slice 1 is **not** finished by #361. As merged, #361 shipped `Scan` alone — the
`Operator` protocol, `Planner` and `PlanningContext` its own acceptance criteria named were **not**
built (nor was the `QueryIO` port, correctly — see Correction (2)). #363 therefore carries the
`Planner` + `PlanningContext` introduction its criteria already presuppose.

Filed separately (orthogonal, no new pressure from the plan): `_compiler_state` → **compilation
stack**, resolving the §Compiler entry points trap structurally.

### Query plan
A **tree of Operators** that computes a SELECT's rows. Built by the **Planner** from the
`Select` AST; replaces the `if self._joins:` / `if self._is_aggregate:` branching in
`Select`'s hooks with plan construction. The hook **builds** the plan; the **engine** drives it
(the drive loop lives in the engine, not the hook — see
[[adr-0020-execution-layering-sql-builds-engine-drives]]).

Distinct from **Execution Pipeline** (the engine's hook sequence, §Core Concepts) — a plan is a
tree, not a linear sequence. _Avoid_: "query execution pipeline", "execution tree".

### Planner
The stage translating a `Select` AST into a **Query plan**. It runs at **execution time**, in
`Select._setup_execution`, reading the **PlanningContext** off `context.compiled`.

Planning already happens today — hand-rolled inside `NotionCompiler.visit_select`, which splits
`left_conjuncts` / `right_conjuncts` and computes the pushable leading run of ORDER BY keys. The
**Pushdown** decision stays where it is (the compiler must know the split anyway, to emit a
`payload['filter']` holding *only* the pushed conjuncts); what changes is that the leftover
**Residual** is recorded on the **PlanningContext** as an AST instead of being compiled to Notion
JSON and smuggled out on `compiled_dict`. The Planner then *consumes* that decision to build the
tree. Long-term, planning moves out of the compiler entirely and the compiler shrinks to pure
payload generation; that is a later slice, not v1.

**The residual bridge (#363).** Carrying the Residual as AST while `Filter` still wraps `_Filter`
**verbatim** leaves a gap: `_Filter` eats **Notion JSON**, not AST. The Planner closes it by
compiling the residual AST to Notion JSON at **plan-build time**, with bind values **inlined**.
Inlining is forced, not stylistic — the residual's `:key` placeholders were bound only by
`ExecutionContext._bind_params` reading `compiled_dict['join_right_filter']`, and that key is gone.
A `BindParameter` already carries its `.value`, so a residual AST is self-contained and needs no
runtime resolution.

The same fact keeps the parameter accounting honest. `_bind_params` **pops** each key it binds and
`_assert_all_params_consumed` then raises on leftovers, so the residual's binds must stop being
registered in `execution_binds` at all — which happens naturally, since a residual that is never
`_compiler_dispatch`-ed never reaches `visit_bindparam`. Compile it to JSON *and* leave its binds
registered and every join with a right-side parameterised WHERE dies with
`ArgumentError: Unused bind parameters`.

This bridge is **deliberate and throwaway**: [[adr-0019-sql-null-semantics-pushdown-soundness]]'s
three-valued evaluator reads the AST directly, deleting both the bridge and `_Filter`'s reuse (the
"layering inversion" noted under §is_null). It is not the target contract.

### PlanningContext
The **messenger carrying compile-time decisions to run time** — the `Residual` WHERE and sorts (as
**AST**), the join structure, and the projection. Authored by the compiler during `visit_select`,
harvested onto the `Compiled` object, read by the **Planner** at `_setup_execution`.

This is not a new mechanism; it **names and generalises one that already exists**.
`Compiled.__init__` already does `self._execution_binds = dict(compiler._compiler_state.execution_binds)`
(`base.py:366`) — compile-time state, harvested, carried to run time. `execution_binds` is the
pattern; `join_right_filter` on `compiled_dict` was the hack that bypassed it. Note `Compiled`
already carries `BindParameter` (AST-level) objects, so the JSON-purity rule binds `compiled_dict`
only — a `PlanningContext` holding AST is consistent with the existing design.

**Scratch vs output.** `CompilerState` today mixes two lifetimes: transient compiler *scratch*
(`in_where`, `compile_state`, `stmt`) and durable planning *outputs* (`execution_binds`,
`result_columns`, `fetch_columns`). Only outputs may survive compilation. `PlanningContext` names
that boundary and holds the outputs, so the `Compiled` harvest is **one object**, not a
field-by-field cherry-pick that must know which fields are scratch.

**Why not `ExecutionContext`.** It is constructed at `base.py:226` (step 3), *after* compile
(step 2), so it can only ever be the messenger's **destination**, never its author — that timeline
is exactly why `join_right_filter` had to ride `compiled_dict` in the first place. Growing it
would also walk back [[adr-0008-joinexecution-seam]]'s headline win (the 5 → 1 attribute
collapse).

### Operator
A node in a **Query plan**. Batch-at-a-time iterator interface, deliberately **not** classic
row-at-a-time Volcano — Notion's transport grain is a batch (`page_size` ≤ 100, `PageIterator`
already yields whole pages, `pages.retrieve` is inherently bulk), so a row-at-a-time `next()`
would re-split batches the layer below just produced.

```
Operator.open(io) -> None
Operator.next()   -> list[tuple] | None    # one batch, None when exhausted
Operator.close()  -> None
```

Node kinds: **Scan**, **Retrieve**, **Join**, **Filter**, **Aggregate** (and **GroupBy**, the
first planned addition). `JoinExecution` and `AggregateExecution` become Operators; their
config-to-constructor discipline ([[adr-0008-joinexecution-seam]],
[[adr-0011-aggregate-execution-seam]]) carries over intact.

### Scan vs Retrieve (the two access paths)
A plan has **exactly one Scan** — the left-side `data_sources.query` — no matter how many joins it
carries. Every right side is a **Retrieve**: a bulk `pages.retrieve` by `object_id`. This is not a
v1 simplification but a property of the backend: normlite joins through a `Relation` FK, so the
right side is always reached *by page ID*, and Notion offers no `id IN (...)` filter that could
turn it into a query. `Scan` is the only Operator that carries a compiled payload.

### Retrieve — a batch-bound parameterized access path
**Retrieve is the `Join`'s second child**, and it is **parameterized**: it cannot `open()` until
the Join has drained its left child and bound it with the oids. An `open()` on an unbound
`Retrieve` is a programming error and must fail loudly, never fetch nothing.

The binding is **once per open, with the whole deduplicated oid set** — *not* Graefe's per-outer-
tuple rebinding. normlite conforms to Volcano *structurally* (the inner is a bound child of the
join) but deliberately deviates on binding cardinality: `Join` is a **Blocking operator**, so it
knows every oid before `Retrieve` opens, and one bulk call is one round-trip where per-tuple
rebinding would be thousands. **Do not "restore" per-tuple rebinding** — it would be Volcano-
faithful and catastrophic.

### Join algorithm — batched index nested-loop
normlite has exactly one join algorithm, previously unnamed: drain left → collect and **dedupe**
the FK oids → **one** bulk retrieve-by-ID → hash the right rows by `object_id` → probe per left
row (`dml.py:1265`). Retrieve-by-ID *is* a primary-key index seek, batched across the entire left
side; the hash map serves **reassembly**, not matching — so this is **not** a hash join, and not a
classic nested loop (the inner is never re-opened per outer row).

The global dedup survives precisely *because* `Join` blocks. Chained multi-join composes by
nesting — `Join(Join(Scan(a), Retrieve(b)), Retrieve(c))` — giving **N sequential round-trips**
for N joins, each individually bulk-deduped.

### Blocking operator (pipeline breaker)
An **Operator** that must drain its child in `open()` before it can emit its first batch. **Join**
blocks (it needs every left row's relation IDs to build one deduplicated `pages.retrieve` batch)
and **Aggregate** blocks (you cannot average a half-consumed stream). This is the *same* rule
already stated as "joins and aggregates force drain-all" under §Pagination and
[[adr-0010-streaming-result-token-pagination]] — the plan restates it structurally rather than
changing it. **Scan** is the only non-blocking Operator; it is what keeps `stream_results` /
`yield_per` working on a plain select.

### Pushdown / Residual
Every WHERE conjunct and ORDER BY key is **either** pushed **or** residual — the split is the
Planner's central output.

- **Pushdown** — the predicate/sort/projection is translated to Notion JSON and evaluated
  **Notion-side**, inside a **Scan**'s `data_sources.query` payload (`filter`, `sorts`,
  `filter_properties`).
- **Residual** — it cannot be pushed, so it is evaluated **client-side** by normlite. A residual
  stays an **AST** (`ColumnElement`) all the way to its **Filter** Operator; it is *never*
  compiled to Notion filter JSON.

Sort pushability is **positional**: only the *leading run* of left-table ORDER BY keys is
pushable; the first non-left key makes it and everything after it residual.

### Pushdown soundness (the invariant)
**Pushing a predicate must never change its answer.** A predicate evaluated Notion-side (pushed)
and the same predicate evaluated client-side (residual) must agree — otherwise the result depends
on a Planner decision the user cannot see, which is the worst failure mode in this design.

This invariant holds **today**, and not by accident: the pushed filter is Notion-semantic, and the
residual is evaluated by `_Filter` over **raw Notion cells**, which is *also* Notion-semantic. It
is load-bearing, and it is the reason the two operators below must stay distinct.

### `is_empty()` vs `is_null()` — the overloaded-emptiness split
**Flagged ambiguity — "empty" meant two different things.** They are now separate operators and
must never be conflated:

- **`is_empty()` — Notion-semantic, PUSHABLE.** "The property holds no value" (`{"rich_text": []}`,
  `{"number": null}`). Maps to Notion's `is_empty` filter op (`type_api.py:383`), rides into the
  Scan payload.
- **`is_null()` — SQL-semantic, NEVER PUSHED.** "There is no value *here*" — the cell is literally
  `None`: an outer join's unmatched right slice, or an absent property. Notion cannot express "this
  row had no join partner", so it is **always Residual**, evaluated client-side. Being unpushable
  *by construction*, it has no pushdown parity to violate.

`is_empty ≠ is_null`, and the difference is observable: at raw level a real empty cell is a **dict**
(`{"rich_text": []}`, `{"number": None}`) while an unmatched right slice is **literally `None`** —
distinguishable, which is exactly what makes `is_null()` implementable.

**This is why decoded values are not enough.** The decode is **lossy**: `{"rich_text": []}` (Notion
`is_empty` → TRUE) and `{"rich_text": [{"text": {"content": ""}}]}` (→ FALSE) *both* decode to `""`
via `rich_text_to_plain_text` (`getters.py:93`). A client-side evaluator over decoded values could
not reproduce `is_empty`, and pushdown parity would break.

### Residual predicates are AST, evaluated over raw cells
A **Residual** stays an **AST** (`ColumnElement`) — never round-tripped through Notion's filter
language — and is evaluated over **raw Notion cells**, which is what the rows carry through the
plan (decoding to Python happens later, at the `Row`/`CursorResult` level).

`JoinExecution._right_side_passes` (`dml.py:1379`) re-wraps raw cells into a synthetic page. That
re-wrapping is **not gratuitous** — it preserves the raw fidelity `is_empty` needs. What *is* wrong
is the import: `_Filter` comes from `normlite.notion_sdk.client`, the **fake client's** internals,
and exists *only* to simulate Notion-side filtering — a real Notion integration would delete the
join's evaluator out from under it. The fix is to own a raw-cell evaluator, not to abandon raw
cells.

### Three value shapes (do not conflate)
The single most important distinction for anything evaluating a predicate client-side:

| Shape | Example | Who evaluates it |
|---|---|---|
| **Pre-bind Python values** | `Decimal('5')`, `-2` | `CheckConstraint` ([[adr-0012-checkconstraint-client-side-enforcement]]), before `pages.create` |
| **Raw Notion cells** | `{"number": 5}`, `{"rich_text": []}` | the **Filter** Operator, on query results |
| **Decoded Python values** | `Decimal('5')`, `""` | the user, via `Row` — **lossy**, see above |

**There are two client-side evaluators, and that is correct.** `CheckConstraint`'s works on
pre-bind values *before* a write; the plan's **Filter** works on raw cells *after* a read. They
share the backend-agnostic `Operator` enum and three-valued logic, not an implementation. An
earlier draft of this design claimed one evaluator could serve both — that was wrong: the shapes
and the times differ.

### Three-valued logic — UNKNOWN policy is the caller's
The evaluator returns **TRUE / FALSE / UNKNOWN**; it must **not** return `bool`, because its two
callers apply **opposite** (and both SQL-correct) policies to UNKNOWN:

- **`CheckConstraint`** rejects a row only on **FALSE** — UNKNOWN → **accept**.
- **`WHERE` / Filter** keeps a row only on **TRUE** — UNKNOWN → **drop**.

A `bool`-returning evaluator silently bakes in one caller's policy and breaks the other.
`is_null()` is *not* a comparison: it returns TRUE/FALSE, never UNKNOWN (SQL `IS NULL` semantics).

### Query plan — I/O ownership
The plan **drives its own I/O** through an injected I/O port; `Operator`s are otherwise pure
compute. A join `Select` runs as `ExecutionStyle.EXECUTE`, and the join-specific
`ExecutionStyle.EXECUTEMANY` branch (`context.py:413`) is **deleted**.

This **supersedes** [[adr-0008-joinexecution-seam]]'s "I/O stays in the hooks" — see
[[adr-0018-query-plan-operator-tree]]. ADR-0008 held that "a single synchronous call is
impossible" because the EXECUTEMANY dispatch sits between the phases; that boundary was
self-inflicted by `context.py:413` (joins borrowing the *mutation* statements' bulk channel as a
vehicle), and `Select._setup_execution` already drove phase-1 I/O itself (`dml.py:832`). The
`bulk_operation` / `bulk_parameters` channel is **untouched** and stays exactly as-is for
DELETE / UPDATE / INSERT…RETURNING — joins simply stop borrowing it, which is *not* what
ADR-0008's Alternative A rejected (that was `JoinExecution` *owning* the shared channel).

### Phantom NULL — ADR-0005 revisited
[[adr-0005-outer-join-phantom-null-semantics]] ruled that "a phantom (all-None right slice) fails
every right-side predicate", enforced **structurally**: `if all(c is None for c in right_slice):
return False` (`dml.py:1404`), *before* the predicate runs. That rule was a workaround for a
`bool`-returning, Notion-semantic evaluator with no notion of NULL — normlite had no way to say
"there is no row here", so it hard-coded the drop.

The plan **removes the constraint that forced it**. With three-valued logic plus a real `is_null()`:
- a phantom's cells are `None`, so any **comparison** on them is UNKNOWN → **dropped by WHERE** —
  the ADR-0005 outcome, now derived rather than hard-coded;
- `is_null()` on a phantom is **TRUE → kept**, which makes **anti-join expressible** — closing
  [[anti-join-inexpressible-boundary]];
- the `all(None)` structural guard is **deleted**, closing
  [[unreachable-empty-title-boundary]] (a real `title=None` row was mislabelled a phantom by it).

This is a **deliberate behaviour change**, not a refactoring side effect: the existing join suite is
no longer a bit-for-bit oracle for right-side filtering. ADR-0005 is **superseded** by
[[adr-0019-sql-null-semantics-pushdown-soundness]], which lands as a **separate slice** from the
structural work — a structural bug and a semantic bug must not be indistinguishable in one diff.

### Title NOT NULL — noted, out of scope
Notion tolerates value-less properties (an omitted `rich_text` stores `[]`, an omitted number stores
`null`), so under full SQL semantics nearly every column is nullable. The **`title` property is the
one exception** — it is non-nullable by definition. The matching SQL-faithful move is for `Insert`
to reject a `None` value for the `is_title=True` column.

**Deliberately out of scope here** — it is an `Insert`-side constraint concern (a sibling of the
deferred NOT-NULL constraint noted under §CheckConstraint — NULL / three-valued logic), not a query
planning one. Recorded so the asymmetry is not mistaken for an oversight. Partial enforcement
already exists at the fake-client level (`dml.py:1252`: "insert `title=None` is rejected by the
client").

### Multi-join (what the plan unblocks)
`Select._joins` is a flat tuple, of which only `_joins[0]` is ever executed — chaining
`.join().join()` silently drops the rest. Under the plan, joins **nest as Operators** instead.

**Chained** joins (`a JOIN b ON a.b_ref`, then `b JOIN c ON b.c_ref`) are the reason plan-owned
I/O is load-bearing: `c`'s page IDs are unknowable until `b`'s pages are retrieved, so the join
needs **N sequential round-trips**. `Connection._execute_context` fires `_execute_many` exactly
**once** (`base.py:277`), so under the current channel chained multi-join is not merely
unimplemented — it is *inexpressible*. **Star** joins (several joins off the same left table)
could be faked by concatenating batches, since both are `pages.retrieve`; chained ones cannot.
