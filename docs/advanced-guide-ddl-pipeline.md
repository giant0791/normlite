# DDL Execution Pipeline: CreateTable, DropTable, ReflectTable

This guide is for maintainers and contributors. It explains how the three DDL constructs are
transpiled into Notion API calls and how each one specialises the common execution pipeline
([advanced-guide-execution-pipeline.md](advanced-guide-execution-pipeline.md)).

---

## Common DDL behaviour

All three constructs share the same base class, the same pipeline entry point, and the same
no-op hooks for setup.

### Class hierarchy

```
Executable
  └── ExecutableDDLStatement   (is_ddl = True)
        ├── CreateTable        (__visit_name__ = 'create_table')
        ├── DropTable          (__visit_name__ = 'drop_table')
        └── ReflectTable       (__visit_name__ = 'reflect_table')
```

`ExecutableDDLStatement` overrides `_execute_on_connection` to pass `None` as parameters —
DDL constructs never accept user-supplied bind parameters.

### Pipeline behaviour common to all three

| Phase | Behaviour |
|-------|-----------|
| **Compile** | `__visit_name__` routes to the matching `visit_*` method on `NotionCompiler`. |
| **`pre_exec`** | Binds the compiled-in parameters (e.g. `database_id`). Skips `SchemaInfo` injection — the `is_ddl` guard exits early before cursor description is stamped. |
| **`_setup_execution`** | No-op (`pass`) for all three. |
| **Dispatch** | Always `EXECUTE` — one `do_execute` call, one Notion API request. |
| **`post_exec`** | Always sets `_rowcount = -1` — DDL carries no row count. |
| **`_finalize_execution`** | The meaningful phase. Each construct overrides this to interpret the API response and update the Python object graph. |

---

## CreateTable

### What it maps to

`databases.create` — creates a new Notion database as a child of a parent page.

### Compile (`visit_create_table`)

Precondition checked at compile time: `_db_parent_id` must be set on the `Table` (the Notion
page ID that will own the new database). A `None` value raises `CompileError` immediately.

```python
compiled_dict = {
    "operation": {"endpoint": "databases", "request": "create"},
    "payload": {
        "parent": {"type": "page_id", "page_id": ":page_id"},
        "title": [{"text": {"content": ":table_name"}}],
        "properties": {
            "col_a": {"rich_text": {}},   # compiled from user Column definitions
            "col_b": {"number": {}},
            ...
        }
    }
}
```

`result_columns` is set to the four META_COL constants (`name`, `type`, `id`, `value`) — the
pipeline will return the Notion database schema, not row data.

### `_finalize_execution`

This is where the Python `Table` object comes to life:

```
1. Consume CursorResult → parse ReflectedTableInfo.from_tuples()
2. table._sys_columns["object_id"]._value = reflected_table_info.id
   → binds the Notion database_id (OID) to the Table
3. for each user column: table.c[name]._id = column_id_from_notion
   → each Column now knows its Notion property id
4. engine.create_table_metadata(...)
   → inserts a row into the information_schema.tables Notion database
5. table._sys_tables_page_id = entry.sys_tables_page_id
   → caches the catalog page_id for fast future lookup (avoids query on drop)
```

The result is consumed and closed; the `CursorResult` returned to the caller is empty.

---

## DropTable

### What it maps to

`databases.update` with `in_trash: True` — soft-deletes the Notion database.

### Compile (`visit_drop_table`)

Precondition checked at compile time: `object_id` (`get_oid()`) must be set. A `None` value
raises `CompileError`.

```python
compiled_dict = {
    "operation": {"endpoint": "databases", "request": "update"},
    "path_params": {"database_id": ":database_id"},
    "payload":     {"in_trash": ":in_trash"}       # bound to True
}
```

`in_trash` is emitted as a named bind parameter (not a literal), so it flows through the normal
`pre_exec` binding path.

### Error handling

`DropTable` is the only DDL construct that overrides `_handle_dbapi_error`. A
`ProgrammingError` from the Notion API (table not found) is translated to `NoSuchTableError`:

```python
def _handle_dbapi_error(self, exc, context):
    if isinstance(exc, ProgrammingError):
        raise NoSuchTableError(self._table.name) from exc
    raise   # all other errors propagate unchanged
```

### `_finalize_execution`

Two jobs: remove the Notion database from the catalog, with one retry on stale cache.

```
1. Consume and close CursorResult (response content is irrelevant)
2. Resolve sys_tables_page_id:
     if table._sys_tables_page_id is None:
         engine.require_table_metadata(table.name) → fetch from catalog
3. engine.drop_table_metadata_by_page_id(page_id)
     → marks the information_schema.tables row as dropped
     on ProgrammingError (stale page_id):
         → re-fetch page_id from catalog → retry once → propagate on second failure
```

The retry exists because `_sys_tables_page_id` may be stale if the catalog was mutated outside
the current `Engine` instance.

---

## ReflectTable

### What it maps to

`databases.retrieve` — reads the schema of an existing Notion database.

### Compile (`visit_reflect_table`)

The simplest `compiled_dict` of the three — no payload, only a path parameter:

```python
compiled_dict = {
    "operation":   {"endpoint": "databases", "request": "retrieve"},
    "path_params": {"database_id": ":database_id"}
}
```

Unlike `CreateTable`, the compiler does **not** validate `_db_parent_id`. The table's `object_id`
must be pre-set (typically done by the `Inspector`), and the compiler does not enforce it.

### `_finalize_execution`

Populates the `Table` object from the API response. Unlike `CreateTable`, it **constructs**
`Column` objects rather than updating existing ones:

```
1. Consume CursorResult → parse ReflectedTableInfo.from_tuples()
2. table._db_parent_id = engine._user_tables_page_id
   → records where in the Notion hierarchy this database lives
3. for each system column:
       table._sys_columns[name]._value = colmeta.value
       → sets object_id, archived, in_trash, created_time, etc.
4. for each user column:
       new_col = Column(name, type_, id_)
       new_col._set_parent(table)
       table.append_column(new_col)
       → dynamically builds the Column collection from Notion property definitions
```

`ReflectTable` does **not** write to the system catalog (`create_table_metadata` is never
called). The `Table` object is populated in memory; catalog registration is the caller's
responsibility if needed.

---

## Summary: CreateTable vs DropTable vs ReflectTable

| Aspect | CreateTable | DropTable | ReflectTable |
|--------|-------------|-----------|--------------|
| **Notion API call** | `databases.create` | `databases.update` | `databases.retrieve` |
| **`compiled_dict` keys** | `operation` + `payload` | `operation` + `path_params` + `payload` | `operation` + `path_params` |
| **Has `payload`** | Yes — parent, title, properties | Yes — `in_trash: True` | No |
| **Precondition at compile** | `_db_parent_id` not None | `object_id` not None | None enforced |
| **`_setup_execution`** | No-op | No-op | No-op |
| **`_finalize_execution` main job** | Assign OIDs to pre-declared Columns + write catalog | Drop catalog entry (with stale-cache retry) | Construct Column objects from response |
| **System catalog mutation** | Creates entry | Marks entry as dropped | None |
| **Column handling** | Assigns `_id` to existing `Column` objects | None | Constructs new `Column` objects |
| **Custom `_handle_dbapi_error`** | No | Yes — `ProgrammingError` → `NoSuchTableError` | No |
| **Returns data to caller** | Empty `CursorResult` | Empty `CursorResult` | Empty `CursorResult` |
| **Side effects on `Table`** | Sets `object_id`, column IDs, `_sys_tables_page_id` | None | Sets `_db_parent_id`, system column values, appends user Columns |

---

## SystemCatalog — the information schema behind CreateTable and DropTable

Both `CreateTable._finalize_execution` and `DropTable._finalize_execution` call `Engine` methods
that delegate to `SystemCatalog` (`engine/systemcatalog.py`).

`SystemCatalog` manages a Notion database called `information_schema.tables` that lives under
the integration's root page. It is bootstrapped once when the `Engine` is created:

```
Engine.__init__
  └── _bootstrap_catalog()
        └── SystemCatalog.bootstrap()
              ├── get_or_create page  "information_schema"
              ├── get_or_create database "tables"   ← catalog store
              └── get_or_create page  "<user_database_name>"  ← parent for user Tables
```

| Engine method | SystemCatalog action | Triggered by |
|---------------|---------------------|--------------|
| `create_table_metadata()` | `ensure_sys_tables_row()` — inserts a row into `tables` | `CreateTable._finalize_execution` |
| `require_table_metadata()` | `find_sys_tables_row()` — queries `tables` | `DropTable._finalize_execution` (stale cache path) |
| `drop_table_metadata_by_page_id()` | `set_dropped_by_page_id()` — soft-deletes the row | `DropTable._finalize_execution` |

`ReflectTable` touches none of these paths.

---

## File reference

| File | Role |
|------|------|
| `sql/ddl.py` | `CreateTable`, `DropTable`, `ReflectTable` — hooks and Python object graph updates |
| `sql/compiler.py:282–420` | `visit_create_table`, `visit_drop_table`, `visit_reflect_table` |
| `sql/reflection.py` | `ReflectedTableInfo`, `ReflectedColumnInfo` — parse Notion schema responses |
| `engine/context.py` | `pre_exec` DDL short-circuit (line 507); `post_exec` rowcount=-1 (line 533) |
| `engine/base.py` | `_execute_context` pipeline; catalog delegation methods |
| `engine/systemcatalog.py` | `SystemCatalog` — `information_schema.tables` CRUD |
