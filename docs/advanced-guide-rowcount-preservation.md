# Rowcount Preservation -- Advanced Guide

## Insert Execution Flow

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
DBAPI execute()             # DBAPI sets the cursor.rowcount
  ↓
post_exec()                 # Perform rowcount preservation based on the homonimous execution option  
  ↓
_finalize_execution()       # Set up cursor result
  ↓
CursorResult
```

------------------------------------------------------------------------

## DBAPI Execution Phase
``` python
engine.do_execute(cursor, context.operation, context.parameters)
```

The `context.operation` (here `pages.create`) is executed.
`cursor.rowcount` stores the length of the returned result object from Notion (`cursor._result_set['results']`).

------------------------------------------------------------------------

## `context.post_exec()` Phase
The context checks the execution option `"preserve_rowcount"` and memoize internally the `cursor.rowcount`.

## Get Rowcount Phase
The user gets accesses the `CursorResult.rowcount` attribute:

``` python

result = connection(execute, stmt)
print(result.rowcount)
```

The `CursorResult.rowcount` accesses the memoized data by calling the `ExecutionContext.get_rowcount().

``` python
@property
def rowcount(self) -> int:
  rowcount = self.context.get_rowcount()
  return -1 if rowcount is None else rowcount
```

------------------------------------------------------------------------

## Hook Responsibilities Summary

| Hook                    | Purpose                            |
| ----------------------- | ---------------------------------- |
| `pre_exec()`            | Normalize binds and options        |
| `_setup_execution()`    | Prepare execution state            |
| `post_exec()`           | Memoize the DBAPI cursor.rowcounta |
| `_finalize_execution()` | Do nothing                         |

------------------------------------------------------------------------
