# Normlite Execution Pipeline -- Advanced Guide

## Introduction

This guide is intended for maintainers and contributors.

It explains how:

- `Connection`

- `ExecutionContext`

- `Executable`

- `Compiled`

- DBAPI

work together.

------------------------------------------------------------------------

## 1. Core Architectureal Components

### Engine

Responsibilities:

- Client creation

- Catalog bootstrap

- Execution options (engine-level)

- Delegating low-level execution (`do_execute()`)

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
- Choose execution style (currently SINGLE)
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

------------------------------------------------------------------------

### Step 6 — DBAPI Execution

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

------------------------------------------------------------------------

## Hook Responsibilities Summary

| Hook                    | Purpose                            |
| ----------------------- | ---------------------------------- |
| `pre_exec()`            | Normalize binds and options        |
| `_setup_execution()`    | Prepare execution state            |
| `_handle_dbapi_error()` | Translate backend errors           |
| `post_exec()`           | Capture mechanical result metadata |
| `_finalize_execution()` | Semantic reconstruction            |

------------------------------------------------------------------------

## Key Architectural Insight

Normlite does not compile SQL strings.

It constructs strongly-typed backend JSON payloads.

Therefore:

- Structural correctness must be ensured before execution.
- Lifecycle validation must occur before backend calls.
- Semantic reconstruction occurs only after transport completes.
- his separation makes the execution pipeline predictable, extensible, and testable.