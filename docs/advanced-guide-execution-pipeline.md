# Normlite Execution Pipeline -- Advanced Guide

## Introduction

This guide is intended for maintainers and contributors.

It explains how:

- `Connection`

- `ExecutionContext`

- `Executable`

- `Compiled`

- the Query plan

- DBAPI

work together.

Two terms stay separate in this guide:

- The **Execution Pipeline** is the engine's hook sequence (Steps 1 to 9 below). It is linear.
- The **Query plan** is a tree of Operators (`Scan`, `Filter`, `HashJoin`, `Aggregate`, ...).
  It computes the rows of a statement. It is not a pipeline.

------------------------------------------------------------------------

## 1. Core Architectural Components

### Engine

Responsibilities:

- Client creation

- Catalog bootstrap

- Execution options (engine-level)

- Delegating low-level execution (`do_execute()`, `do_executemany()`)

The engine is stateless regarding individual executions.

------------------------------------------------------------------------

### Connection

Responsibilities:

- Scope of execution
- Merging execution options
- Building execution context
- Orchestrating execution pipeline

Key methods:

```python
Connection.execute()
Connection._execute_context()
Connection._execute_single()
Connection._execute_insert_many()
Connection._execute_query_plan()
```

### ExecutionContext

Created in `Connection._execute_context()`.

It stores:

- Engine
- Connection
- Cursor
- Compiled object
- Distilled parameters
- Execution options
- Execution style
- Operation (JSON payload)
- Final DBAPI parameters

It acts as the state container for a single execution.

### Query plan

Built by the `Planner` (`sql/queryplan.py`) from the `ExecutionContext`.
The plan is data: the SQL layer builds it, and the engine drives it
([ADR-0020](adr/0020-execution-layering-sql-builds-engine-drives.md)).

Each Operator has `open()`, `next()` and `close()`. `next()` returns one batch of rows,
or `None` when the Operator has no more rows. A **Blocking operator** drains its child in
`open()` before it gives its first batch. `Scan` is the only Operator that does not block.


### Executable

Base class for DML and DDL statements.

It defines three core hooks:

``` python
Executable._setup_execution()
Executable._handle_dbapi_error()
Executable._finalize_execution()
```

------------------------------------------------------------------------

## 2. Full Execution Pipeline

The statement execution is initiated by calling the `Connection.execute()` method:

``` python
# stmt is a DDL/DML statement
with engine.connect() as connection:
    result = connection.execute(stmt)
```

### Step 1 — Compilation

``` python
compiled = stmt.compile(compiler)
```

- AST → JSON payload
- Collect bind parameters
- Record result metadata
- Mark DDL vs DML
- Compilation must ensure structural correctness of payload.

------------------------------------------------------------------------

###  Step 2 — Parameter Distillation

``` python
distilled_params = _distill_params(parameters)
```

Normalizes input parameters into a canonical mapping.

------------------------------------------------------------------------

### Step 3 — ExecutionContext Creation

``` python
ctx = ExecutionContext(
    engine,
    connection,     # Connection instance
    cursor,         # DBAPI cursor
    compiled,       # compiled statement
)
```

Context becomes the carrier of execution state.

------------------------------------------------------------------------

### Step 4 — `pre_exec()`

Purpose:

- Merge execution options
- Resolve bind parameters
- Choose the execution style (`_determine_execution_style()`, see Step 6)
- Prepare final DBAPI parameter dict

No backend call happens here.

------------------------------------------------------------------------

### Step 5 — `_setup_execution()`

Statement-specific preparation.

#### DML Example (Insert)

- Validate table state
- Possibly adjust returning strategy
- Finalize operation payload

#### DDL Example (DropTable)

- Ensure table OID is present
- Validate lifecycle state
- Ensure payload contains valid database_id

This stage may raise `ProgrammingError`.

#### Plan-driven statements (Select, Delete, Update)

`_setup_execution()` is a no-op. The statement sends no request here.
The engine drives the Query plan in Step 6.

> **[ADR-0023](adr/0023-dml-executes-through-the-query-plan.md), built in #411 and #413.**
> `Delete` and `Update` execute through the Query plan (`ModifyTable -> [Filter] -> Scan`).

------------------------------------------------------------------------

### Step 6 — Dispatch on the execution style

`Connection._execute_context()` dispatches on `context.execution_style`:

| Execution style    | Method                   | Statements                         | Notion requests                       |
| ------------------ | ------------------------ | ---------------------------------- | ------------------------------------- |
| `EXECUTEQUERYPLAN` | `_execute_query_plan()`  | Select, Delete, Update (ADR-0023)  | Sent by the Operators of the plan     |
| `EXECUTE`          | `_execute_single()`      | Single-row Insert, DDL             | `engine.do_execute()`, one request    |
| `INSERTMANYVALUES` | `_execute_insert_many()` | Bulk Insert                        | `engine.do_executemany()`, N requests |

#### `EXECUTEQUERYPLAN`

``` python
plan = Planner(context).plan()
plan.open(conn)
rs = ResultSet(plan.result_schema.as_sequence(), "page", plan.next() or [])
context._result_cursor._batch_source = plan
```

- The `Planner` builds the Query plan.
- The engine opens the root Operator and reads the first batch.
- The plan root becomes the batch source of the result cursor.
- With `stream_results` or `yield_per`, the engine stops here. `CursorResult` pulls the next
  batches.
- Without streaming, the engine drains the plan through the cursor and closes the plan.

A mutation never streams (ADR-0023, item 7). The engine streams only when `elem.is_select`.
The DELETE and UPDATE `Scan` take the default page size, not `yield_per`.

#### `EXECUTE`

``` python
engine.do_execute(cursor, context.operation, context.parameters)
```

Low-level execution.

If a DBAPI error occurs:

``` python
stmt._handle_dbapi_error(exc, context)
```

This hook maps backend errors to normlite exceptions and performs a semantic translation from transport layer errors to normlite errors.

------------------------------------------------------------------------

### Step 7 — `post_exec()`
Mechanical finalization:

- Store rowcount
- Store lastrowid
- Capture cursor metadata

No semantic interpretation yet.

------------------------------------------------------------------------

### Step 8 — `_finalize_execution()`

Semantic reconstruction phase.

For `Select` this hook is a no-op: the Operators already built the result rows.
For `Delete` and `Update` (ADR-0023) it only interprets the result: it soft-closes the cursor when
there is no RETURNING, and it reads implicit returning from the `object_id` column.

#### DML Example (Insert)

- Map returned rows
- Possibly update identity columns

#### DDL Example (CreateTable)

- Extract new database_id
- Update system catalog
- Attach OID to Python Table object

This stage updates in-memory object state.

------------------------------------------------------------------------

### Step 9 — CursorResult Creation

``` python
return context.setup_cursor_result()
```

Produces user-facing result abstraction.

------------------------------------------------------------------------

## Error Handling Model

Errors may originate from:

- Compilation
- `_setup_execution()`
- DBAPI execution
- `_finalize_execution()`

Mapping rules:
| Phase    | Responsibility                |
| -------- | ----------------------------- |
| Compile  | Structural payload validation |
| Setup    | Lifecycle validation          |
| DBAPI    | Transport / backend errors    |
| Finalize | Semantic reconstruction       |

------------------------------------------------------------------------

## DML Example Flow (Insert)
``` text
Insert
  ↓
compile → payload + binds
  ↓
ExecutionContext
  ↓
pre_exec()
  ↓
_setup_execution()
  ↓
DBAPI execute()
  ↓
post_exec()
  ↓
_finalize_execution()
  ↓
CursorResult
```

------------------------------------------------------------------------

## Plan-driven Example Flow (Delete, ADR-0023)

``` text
Delete
  ↓
compile → filter payload + binds + write operation pages.update + payload template {"in_trash": True}
  ↓
ExecutionContext
  ↓
pre_exec()             ← execution style EXECUTEQUERYPLAN
  ↓
_setup_execution()     ← no-op
  ↓
Planner → ModifyTable → [Filter] → Scan
  ↓
plan.open()            ← drains the Scan, sends every pages.update
  ↓
post_exec()
  ↓
_finalize_execution()  ← result interpretation only
  ↓
CursorResult
```

See [advanced-guide-dml-pipeline-crud.md](advanced-guide-dml-pipeline-crud.md) for the details.

------------------------------------------------------------------------

## DDL Example Flow (CreateTable)

``` text
CreateTable
  ↓
compile (structural payload)
  ↓
ExecutionContext
  ↓
pre_exec()
  ↓
_setup_execution()   ← lifecycle validation
  ↓
DBAPI execute()
  ↓
post_exec()
  ↓
_finalize_execution() ← attach OID + catalog update
  ↓
CursorResult
```

------------------------------------------------------------------------

## Design Principles
1. Compiler builds structurally valid JSON payloads.
2. Executable controls lifecycle validation.
3. ExecutionContext owns runtime state.
4. Engine performs transport.
5. Finalization updates Python object graph.
6. SQL builds, ENGINE drives, DBAPI performs I/O
   ([ADR-0020](adr/0020-execution-layering-sql-builds-engine-drives.md)).
   The dependency direction is ENGINE → SQL, ENGINE → DBAPI, SQL → DBAPI. Nobody calls upward.
   The SQL layer gives the engine a Query plan (data). The engine drives it.
   A hook that sends a Notion request violates this rule.

------------------------------------------------------------------------

## Hook Responsibilities Summary

| Hook                    | Purpose                            |
| ----------------------- | ---------------------------------- |
| `pre_exec()`            | Normalize binds and options        |
| `_setup_execution()`    | Prepare execution state. No-op for plan-driven statements |
| `_handle_dbapi_error()` | Translate backend errors           |
| `post_exec()`           | Capture mechanical result metadata |
| `_finalize_execution()` | Semantic reconstruction. Result interpretation only for plan-driven statements |

------------------------------------------------------------------------

## Key Architectural Insight

Normlite does not compile SQL strings.

It constructs strongly-typed backend JSON payloads.

Therefore:

- Structural correctness must be ensured before execution.
- Lifecycle validation must occur before backend calls.
- Semantic reconstruction occurs only after transport completes.
- This separation makes the execution pipeline predictable, extensible, and testable.