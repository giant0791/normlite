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
no truncation. This is a known divergence to revisit when `Select.join()` is implemented;
joins that paginate large relations in production must not silently rely on the fake's
non-truncating behaviour.

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
