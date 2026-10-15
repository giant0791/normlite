# DML Execution Pipeline: Insert, Update and Delete

This document describes how `Insert`, `Update` and `Delete` specialise the common execution pipeline
([advanced-guide-execution-pipeline.md](advanced-guide-execution-pipeline.md)) and where their
behaviour diverges.

> **[ADR-0023](adr/0023-dml-executes-through-the-query-plan.md).**
> DELETE and UPDATE execute through the **Query plan**, with a **ModifyTable** Operator at the
> root. DELETE moved in #411. UPDATE moved in #413. **Two-Phase Execution** and
> `ExecutionStyle.EXECUTEMANY` are deleted.

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

## Delete instantiation (Query plan)

> **[ADR-0023](adr/0023-dml-executes-through-the-query-plan.md), built in #411.**

### Why Delete uses the Query plan

[ADR-0022](adr/0022-lossy-pushdown-and-the-recheck.md) re-checks every WHERE conjunct
client-side. The recheck lives in the `Filter` Operator. Two-Phase Execution had no `Filter`, so
`SELECT ... WHERE p` and `DELETE ... WHERE p` could select different rows (#397).
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
- Implicit returning: the hook reads `Cursor._last_inserted_row_ids`. The `ResultSet` takes
  the `object_id` at column 0 of the rows.

Delete does not use `_staged_result_cursor`. Only Insert uses it.

---

## Update instantiation (Query plan)

> **[ADR-0023](adr/0023-dml-executes-through-the-query-plan.md), built in #413.**

Update runs the same plan as Delete: `ModifyTable → [Filter] → Scan`.
This section gives only the differences.

### Compile (`visit_update`)

The compiler emits:

- the filter payload for the `Scan`, from the shared WHERE visitor;
- `update_operation`: the write operation `pages.update`;
- `update_params`: the write payload template `{"payload": {"properties": {"col": ":col"}}}`.
  Each value is a bind placeholder.

Missing `.values()` is a `CompileError` at compile time (unlike Insert).

`visit_update` still refuses a WHERE term that it cannot push (NOT, `== None`, `!= None`).
#413 removes this refusal and adds the lax `_is_pushable` gate, as `visit_delete` has.

### `pre_exec` — the VALUES binds

`_determine_execution_style` returns `EXECUTEQUERYPLAN` for every Update.

`pre_exec` keeps the VALUES binds in `ctx.resolved_params`. The Planner reads them to bind the
write payload template once. `_assert_all_params_consumed` accepts two kinds of leftover bind:

- a VALUES bind (`COLUMN_VALUE`) of an Update: the Planner consumes it;
- a filter bind (`COLUMN_FILTER`): the `Filter` consumes it, if the WHERE is rechecked.

`parameters=` raises `ArgumentError` (v1).

### Scan columns

With a WHERE, the `Scan` fetches only the properties that the WHERE reads, as for Delete.
Without a WHERE, the title-only rule applies to Delete only. The Update `Scan` sends no
`filter_properties` and Notion returns all properties.

### Hooks

`_setup_execution` is a no-op. `_finalize_execution` does the same as for Delete.

---

## Insert vs Update — summary

| Aspect | Insert | Update |
|--------|--------|--------|
| **Notion write call** | `pages.create` | `pages.update` |
| **Execution style** | `EXECUTE` or `INSERTMANYVALUES` | always `EXECUTEQUERYPLAN` |
| **Who finds the rows** | No search — the values give the rows | `Scan` sends `databases.query`, `Filter` rechecks the WHERE |
| **Who sends the write** | Engine dispatch (`_execute_single` / `_execute_insert_many`) | `ModifyTable.open()` |
| **`compiled_dict` shape** | `operation` + `payload` | `operation` + `path_params` + `payload` + `update_operation` + `update_params` |
| **`pre_exec` param handling** | Fully consumes resolved params into `payload` | Keeps the VALUES binds in `resolved_params` for the Planner |
| **`_setup_execution`** | Pre-wires the RETURNING cursor only | No-op |
| **RETURNING data source** | Post-fetch: separate `pages.retrieve` per row | Free: rows of `ModifyTable`, from the `pages.update` responses |
| **Partial `.values()`** | Not allowed — all user columns required | Allowed — unspecified columns are left unchanged |
| **Missing `.values()`** | Deferred to `pre_exec` (can be supplied via `parameters=`) | Hard `CompileError` at compile time |
| **Bulk support** | Yes — `INSERTMANYVALUES` via `.values([...])` or `parameters=[...]` | No — `parameters=` raises `ArgumentError` (v1) |

---

## Update vs Delete — summary

Both run `ModifyTable → [Filter] → Scan` (ADR-0023). They differ only here:

| Aspect | Update (#413) | Delete (#411) |
|--------|---------------|---------------|
| **Write payload template** | `update_params`: `{"payload": {"properties": ...}}`, the `.values()` as bind placeholders | `delete_params`: `{"payload": {"in_trash": True}}`, no bind |
| **Planner binding** | Binds the template from `resolved_params` | Binding returns the template unchanged |
| **Scan with no WHERE** | Fetches all properties | Fetches the title only (#410) |
| **Unpushable WHERE term** | Refused at compile time (until the rest of #413) | Lax `_is_pushable` gate, then the `Filter` rechecks |
