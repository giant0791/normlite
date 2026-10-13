# DML Execution Pipeline: Insert, Update and Delete

This document describes how `Insert`, `Update` and `Delete` specialise the common execution pipeline
([advanced-guide-execution-pipeline.md](advanced-guide-execution-pipeline.md)) and where their
behaviour diverges.

> **Transition — [ADR-0023](adr/0023-dml-executes-through-the-query-plan.md).**
> DELETE and UPDATE move from **Two-Phase Execution** to the **Query plan**, with a
> **ModifyTable** Operator at the root. DELETE moved in #411. UPDATE moves in #413.
> The Update section below still describes Two-Phase Execution. The Delete section describes
> the plan-driven design.

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

> **Retiring — [ADR-0023](adr/0023-dml-executes-through-the-query-plan.md).** This section is
> true until #413. Then Update runs through the Query plan like Delete (see below), and
> `ExecutionStyle.EXECUTEMANY` and `Connection._execute_many` are deleted.

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

## Delete instantiation (Query plan)

> **[ADR-0023](adr/0023-dml-executes-through-the-query-plan.md), built in #411.**

### Why Delete uses the Query plan

[ADR-0022](adr/0022-lossy-pushdown-and-the-recheck.md) re-checks every WHERE conjunct
client-side. The recheck lives in the `Filter` Operator. Two-Phase Execution has no `Filter`, so
`SELECT ... WHERE p` and `DELETE ... WHERE p` can select different rows (#397).
A Delete therefore runs through a Query plan.

### Compile (`visit_delete`)

The compiler emits:

- the `databases.query` filter payload for the `Scan`, from the shared WHERE visitor;
- the write operation `pages.update` and the write payload template `{"in_trash": True}`.

### `pre_exec` — execution style selection

`_determine_execution_style` returns `EXECUTEQUERYPLAN` for every Delete.

### `_setup_execution` — no-op

The hook sends no request and fills no `bulk_parameters`.

### Dispatch — the plan

```
ModifyTable  →  [Filter]  →  Scan
```

- **Scan** sends `databases.query`. It fetches only the properties that the WHERE reads, or only the
  title when there is no WHERE (#410). It takes the default page size, not `yield_per`.
- **Filter** re-checks the WHERE on the raw cells. It is present only when the statement has a
  WHERE.
- **ModifyTable** performs the writes. The Planner gives it the write operation and binds the
  write payload template once.
  `ModifyTable` adds only the path parameter `{"page_id": object_id}` for each row.
  It holds no `ExecutionContext`.

### `ModifyTable.open()` — all writes before the first batch

`open()` drains its child **and** sends every `pages.update`. The writes are complete when
`open()` returns. `next()` only gives back the kept responses.

- **Drain first.** The fake client paginates by offset, and it drops trashed pages
  (`notion_sdk/client.py`). A Delete that trashes one batch and then asks for the next offset skips
  rows (Halloween problem).
- **Write in `open()`.** No consumer can leave a Delete half done. An early close or a stream
  cannot stop the writes. A mutation never streams.
- **Not atomic.** A failed write raises at once through the default `errorhandler`.
  The writes before it stay. Atomicity belongs to transaction support, not to an Operator.

### Rows, RETURNING and rowcount

`ModifyTable` yields one row for each written page. It builds the rows from the `pages.update`
responses, in the RETURNING schema. It never yields the scan rows: the `Scan` does not fetch the
RETURNING columns.

- **No RETURNING:** each row is keyed by `object_id` at column 0. Implicit returning reads it there.
- **RETURNING** reads the write response. Each row holds exactly the RETURNING columns, as for
  INSERT. For Delete, it gives the last values of the page.
- **rowcount** is the number of yielded rows, so it is the number of written pages.

### `_finalize_execution` — result interpretation only

- No RETURNING: the hook soft-closes the cursor. `rowcount` stays available.
- Implicit returning: the hook reads the `object_id` column of the rows.
  It does not read `Cursor._last_inserted_row_ids`. The plan path never fills it.

`_staged_result_cursor` routing goes away for Delete.

---

## Insert vs Update — summary

The Update column describes Two-Phase Execution. It is retiring (ADR-0023, #413).

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

---

## Update (Two-Phase) vs Delete (Query plan) — summary

| Aspect | Update (until #413) | Delete (ADR-0023, #411) |
|--------|---------------------|-------------------------|
| **Execution style** | `EXECUTEMANY` | `EXECUTEQUERYPLAN` |
| **Who sends `databases.query`** | `_setup_execution` (SQL layer) | `Scan`, driven by the engine |
| **WHERE recheck** | None | `Filter` |
| **Who sends `pages.update`** | `_execute_many` | `ModifyTable.open()` |
| **Write payload** | `update_payload`, bound in `_setup_execution` | Template from the compiler, bound once by the Planner |
| **RETURNING data source** | `pages.update` responses, through `_staged_result_cursor` | `pages.update` responses, as rows of `ModifyTable` |
| **Implicit returning** | `Cursor._last_inserted_row_ids` | `object_id` column of the rows |
