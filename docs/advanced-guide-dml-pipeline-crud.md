# DML Execution Pipeline: Insert and Update

This document describes how `Insert` and `Update` specialise the common execution pipeline
([advanced-guide-execution-pipeline.md](advanced-guide-execution-pipeline.md)) and where their
behaviour diverges.

---

## Common pipeline (all DML)

`Connection._execute_context` runs every statement through the same eight steps:

```
compile  →  distill_params  →  ExecutionContext  →  pre_exec
         →  _setup_execution  →  dispatch  →  post_exec
         →  _finalize_execution  →  CursorResult
```

`_setup_execution` and `_finalize_execution` are hooks on `Executable` — each DML class
overrides them to inject statement-specific behaviour into the shared pipeline.

---

## Insert instantiation

### Compile (`visit_insert`)

Produces a `compiled_dict` with a single Notion operation:

```python
{
  "operation": {"endpoint": "pages", "request": "create"},
  "payload":   {"parent": {"database_id": ":database_id"}, "properties": {":col": ...}}
}
```

If `.values()` was omitted at construction time, `visit_insert` injects `VALUE_PLACEHOLDER` for
every user column so they can be supplied via `parameters=` at execution time.  
Missing `.values()` is not an error for Insert — it is deferred to `pre_exec`.

### `pre_exec` — execution style selection

`_determine_execution_style` returns:

- `EXECUTE` — single-row insert (one `.values(...)` call, or `parameters=` with one dict).
- `INSERTMANYVALUES` — bulk insert (`.values([...])` or `parameters=[{...}, ...]`).

All user columns must be present in the resolved params; `_validate_insert_values` raises
`ArgumentError` otherwise (Insert requires complete rows; partial values are not supported).

### `_setup_execution` — RETURNING pre-wiring

Only fires when `.returning(*cols)` was declared. No Notion API call occurs here.
It allocates a fresh DBAPI cursor, stamps it with the projected `SchemaInfo`, and stages:

```python
ctx._staged_result_cursor = result_cursor
ctx.bulk_operation = {"endpoint": "pages", "request": "retrieve"}
```

The parameters for the `pages.retrieve` calls are not yet known — they are filled in
`_finalize_execution` once the `pages.create` responses have returned page IDs.

### Dispatch

```
EXECUTE          →  _execute_single      →  do_execute     →  pages.create   (×1)
INSERTMANYVALUES →  _execute_insert_many →  do_executemany →  pages.create   (×N)
```

### `_finalize_execution` — post-fetch pattern (RETURNING)

When `.returning()` was declared, the inserted page IDs are read from `ctx._cursor._iter_all()`
and used to fire a second round of Notion API calls:

```
pages.create (×N)  →  collect page_ids
                   →  do_executemany(pages.retrieve, [{page_id: ...}, ...])  (×N)
                   →  route _staged_result_cursor → CursorResult
```

The RETURNING data is therefore a **post-fetch**: a separate `pages.retrieve` per inserted row.

---

## Update instantiation

### Compile (`visit_update`)

Produces a `compiled_dict` with **two payload keys** — a filter payload for the query phase and a
separate VALUES template for the update phase:

```python
{
  "operation":      {"endpoint": "databases", "request": "query"},
  "path_params":    {"database_id": ":database_id"},
  "payload":        {"page_size": 100, "in_trash": False, "filter": {...}},  # where-clause
  "update_payload": {"col_name": ":col_name", ...}                           # values template
}
```

`update_payload` is unique to Update — no other DML construct emits it.  
Missing `.values()` is a hard `CompileError` at compile time (unlike Insert).

### `pre_exec` — deferred params

Because `compiled_dict` now has two roles for parameters (filter params consumed by the
`databases.query` payload, values params deferred to `_setup_execution`), `pre_exec` populates
`ctx.resolved_params` with the values `BindParameter` objects **without consuming them**.
`_assert_all_params_consumed` is skipped for `is_update` statements for this reason.

### `_setup_execution` — two-phase execution (API call inside setup)

Unlike Insert, `_setup_execution` fires the **first Notion API call** directly:

```
1. do_execute(databases.query, filter_payload)   ← collect matching pages
2. for each page:
       bind update_payload template with ctx.resolved_params
       append {path_params: {page_id: ...}, payload: {properties: ...}} to bulk_params
3. stage:
       ctx._staged_result_cursor = result_cursor (stamped with SchemaInfo)
       ctx.bulk_operation  = {"endpoint": "pages", "request": "update"}
       ctx.bulk_parameters = bulk_params
```

The dispatch step therefore calls `_execute_many`, which fires the pre-built `bulk_parameters`:

```
databases.query  →  collect page_ids  →  pages.update (×N per matching row)
```

### `_finalize_execution` — RETURNING from response

`pages.update` returns the full Notion page in its response.  
No post-fetch is needed. When `.returning()` is declared, `_finalize_execution` simply routes
`_staged_result_cursor` (which already holds the `pages.update` responses) to `CursorResult`.

---

## Insert vs Update — summary

| Aspect | Insert | Update |
|--------|--------|--------|
| **Notion write call** | `pages.create` | `pages.update` |
| **Execution style** | `EXECUTE` or `INSERTMANYVALUES` | always `EXECUTEMANY` |
| **Two-phase execution** | No | Yes — `databases.query` then `pages.update` |
| **`compiled_dict` shape** | `operation` + `payload` | `operation` + `path_params` + `payload` + `update_payload` |
| **`update_payload` key** | Absent | Present — VALUES template, bound in `_setup_execution` |
| **`pre_exec` param handling** | Fully consumes resolved params into `payload` | Populates `resolved_params` but does **not** consume them (deferred to `_setup_execution`) |
| **`_setup_execution` fires an API call** | No (pre-wires RETURNING cursor only) | Yes (`databases.query`) |
| **RETURNING data source** | Post-fetch: separate `pages.retrieve` per row | Free: `pages.update` response carries the full page |
| **Partial `.values()`** | Not allowed — all user columns required | Allowed — unspecified columns are left unchanged |
| **Missing `.values()`** | Deferred to `pre_exec` (can be supplied via `parameters=`) | Hard `CompileError` at compile time |
| **Bulk support** | Yes — `INSERTMANYVALUES` via `.values([...])` or `parameters=[...]` | No — `parameters=` raises `ArgumentError` (v1) |
