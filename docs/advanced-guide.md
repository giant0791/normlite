
# Reflection Pipeline Architecture (Advanced)

This document explains how table reflection is implemented internally in normlite.

## Core Insight

**Reflection is a process, not a single statement**, but the execution pipeline only supports single-command execution.
Normlite resolves this by orchestrating reflection at the **Engine level**.

## Main Components

### Inspector
User-facing reflection API.

### Engine
Owns reflection orchestration via protected APIs such as `_reflect_table()`.

### Executable
Represents a single database command (e.g. `HasTable`, `ReflectTable`).

### ExecutionContext
Represents exactly one execution: one compiled statement, one cursor, one parameter set.

## Pseudo-DDL Primitives

- `HasTable`: checks for table existence
- `ReflectTable`: reflects column metadata

Each primitive executes exactly one command.

## Row-Based Reflection

Reflection results are returned as rows, one row per column:

```text
(name, type_engine, column_id, value)
```

This mirrors SQLAlchemy’s `information_schema.columns` model.

## ReflectedTableInfo

Intermediate structure that holds reflected column metadata and validates special columns.

## Key Invariants

1. One executable = one DB command
2. ExecutionContext is single-use
3. Engine owns orchestration
4. DBAPI cursor remains dumb
5. Reflection always returns rows

Maintainers should preserve these invariants.