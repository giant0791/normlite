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

### DML Statement
One of `Insert`, `Delete`, `Update`, `Select` — the four DML constructs. Each produces a compiled
payload and follows the two-phase or single-phase execution pipeline below.

### Compiler (NotionCompiler)
Translates a DML/DDL AST into a `compiled_dict` — a JSON-like dict with keys `operation`,
`path_params`, `payload`, and (for UPDATE) `update_payload`. Named placeholders (`:param`) are
resolved at execution time by `ExecutionContext`.

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
A normlite `Relation` is a `TypeEngine` subclass for a column that stores links to rows in another
`Table`. Maps to a Notion **relation property** (`single_property`, unidirectional in v1).
Python value representation: `list[str]` (a list of Notion page ID strings).
`bind_processor` wraps each string as `{"id": v}`; `result_processor` extracts the `"id"` from
each dict. At DDL time, `get_notion_spec()` requires `_oid` to have been injected first via
`Relation.set_oid(uuid)` (called by `ForeignKeyConstraint.resolve()`); raises `ArgumentError`
if still `None`. Supported filter operators: `CONTAINS`, `DOES_NOT_CONTAIN`, `IS_EMPTY`,
`IS_NOT_EMPTY`.

### ForeignKey
A constraint object passed alongside `Relation()` in `Column(...)`: e.g.
`Column("students_oid", Relation(), ForeignKey("students.object_id"))`.
Stores a string reference `"<table>.<column>"` parsed at construction into `fk.table_name` and
`fk.column_name`. Always targets the `object_id` system column (`SpecialColumns.NO_ID`) of the
referenced table; `ForeignKeyConstraint.resolve()` raises `ArgumentError` if `column_name` is
anything else. Resolved at `MetaData.create_all()` time via `ForeignKeyConstraint.resolve()`.

### Relation — Reflection
During `Table._autoload(engine)`, a Notion relation property is reflected by:
1. Extracting `database_id` from the Notion property spec.
2. Calling `engine._catalog.find_sys_tables_row_by_table_id(database_id)` (a new `SystemCatalog`
   method filtering on the `table_id` rich_text field).
3. If found: constructing `Column("<name>", Relation(), ForeignKey(f"{entry.name}.object_id"))`
   and calling `col.type_.set_oid(database_id)` immediately — no deferred resolution needed.
4. If not found: the related table was not created by normlite; emit a warning and skip the
   property (never silently).

### ForeignKeyConstraint
A `Constraint` auto-generated during `Table.__init__` for every column that carries a `ForeignKey`.
Mirrors how `PrimaryKeyConstraint` is auto-generated. Registered in `Table._constraints`.
`ForeignKeyConstraint.resolve(metadata)` parses `fk.table_name` / `fk.column_name`, validates
`column_name == SpecialColumns.NO_ID`, looks up the target table in `metadata.tables`, calls
`target_table.get_oid()`, and injects via `col.type_.set_oid(uuid)`.
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
`fk_constraint.resolve(metadata)`, which injects the target table's `object_id` (now available
because the target was created first) into the column's `Relation._database_id`. By the time the
compiler runs for any table, the schema is fully resolved: all `Relation` instances hold a concrete
`database_id`. Calling `Table.create()` directly on a table that has unresolved `Relation` columns
raises `ArgumentError`.

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
