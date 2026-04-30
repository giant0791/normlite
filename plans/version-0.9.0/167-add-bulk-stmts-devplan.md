# Dev Plan - Add bulk statements

## Objectives
Extend normlite to support multi-paramters INSERT and UPDATE statements, so called _bulk inserts_.

## Use cases
### Bulk insert with `.values()`
A bulk insert is basically the ability to pass a list of dictionaries or tuples via the `.values()` clause.

``` python
stmt = (
    insert(students)
    .values(
        [
            {"name": "Galileo Galilei", "id": 1, "grade": "A"},
            {"name": "Isaac Newton", "id": 2, "grade": "B"},
            {"name": "Ada Lovelace", "id": 3, "grade": "C"},
        ]
    )
)

result = connection.execute(stmt, execution_options={"implicit_returning": True})
print(result.returned_primary_key_rows)
[(uuid1,),  (uuid2,),  (uuid3,),]
print(result.returns_rows)
False

# invariant: returned rows == number of dictionaries supplied
assert len(result.returned_primary_key_rows) == 3
```
> Note that the returned value is a list of 1-value tuples, 1 tuple for each inserted row.

> Remember that `implicit_returning` must be set to `True` otherwise the `.returned_primary_key_rows` property is `None`.

### Bulk insert via execution parameters
``` python
stmt = insert(students)
result = connection.execute(stmt, 
    [
        {"name": "Galileo Galilei", "id": 1, "grade": "A"},
        {"name": "Isaac Newton", "id": 2, "grade": "B"},
        {"name": "Ada Lovelace", "id": 3, "grade": "C"},
    ],
    execution_options={"implicit_returning": True},
)
print(result.returned_primary_key_rows)
[(uuid1,),  (uuid2,),  (uuid3,),]
print(result.returns_rows)
False

# invariant: returned rows == number of dictionaries supplied
assert len(result.returned_primary_key_rows) == 3
```
The same invariant is also applicable to this case.

### `.returning()` with bulk inserts
``` python
stmt = (
    insert(students)
    .values(
        [
            {"name": "Galileo Galilei", "id": 1, "grade": "A"},
            {"name": "Isaac Newton", "id": 2, "grade": "B"},
            {"name": "Ada Lovelace", "id": 3, "grade": "C"},
        ]
    )
    .returning(*students.c)
)

result = connection.execute(stmt)
print(result.all())
[(uuid1, ..., "Galileo Galilei", 1, "A"), (uuid2, ..., "Isaac Newton", 2, "B"), (uuid3, ..., "Ada Lovelace", 3, "C"),]
```

> The `.returning()` clause enables to explicitly specify the columns to be returned.

## Design Breakdown
### Key Insights
#### 1 - Bulk insert is a two-phase operation if `.returning` is specified, with two `executemany()` DBAPI calls.
The first phase is an `ExecutionStyle.EXECUTEMANY` pages.create operation: 
> Inserting multiple parameters requires specifying a list of payloads. This means, the "payload" argument is a list of dictionaries.

Example:
``` python
{
    "payload": [
        # first row: {"name": "Galileo Galilei", "id": 1, "grade": "A"}
        "parent": {"type": "database_id", "database_id": ...},
        "properties": {
            "name": {"title": [{"text": {"content": "Galileo Galilei"}}]},
            "id": {"number": 1},
            "grade": {"rich_text": [{"text": {"content": "A"}}]},
        },

        # second row: {"name": "Isaac Newton", "id": 2, "grade": "B"}
        "parent": {"type": "database_id", "database_id": ...},
        "properties": {
            "name": {"title": [{"text": {"content": "Isaac Newton"}}]},
            "id": {"number": 2},
            "grade": {"rich_text": [{"text": {"content": "B"}}]},
        },

        # third row: {"name": "Ada Lovelace", "id": 3, "grade": "C"}
        "parent": {"type": "database_id", "database_id": ...},
        "properties": {
            "name": {"title": [{"text": {"content": "Ada Lovelace"}}]},
            "id": {"number": 3},
            "grade": {"rich_text": [{"text": {"content": "C"}}]},
        },
    ]
}
``` 

The second phase is as for single parameters inserts, so it's again an `ExecutionStyle.EXECUTEMANY` pages.retrieve operation.

#### 2 - How values are processed in `.values()`
``` python
stmt = insert(students).values(
    name="Galileo Galilei", 
    id= 1, 
    is_active= False, 
    start_on="1600-01-01", 
    grade="A",
)

nc = engine._sql_compiler
compiled = stmt.compile(nc)

>>> stmt._values
mappingproxy(
    {
        'name': BindParameter(key='name', value='Galileo Galilei', role=<_BindRole.COLUMN_VALUE: 2>), 
        'id': BindParameter(key='id', value=1, role=<_BindRole.COLUMN_VALUE: 2>), 
        'is_active': BindParameter(key='is_active', role=<_BindRole.COLUMN_VALUE: 2>), 
        'start_on': BindParameter(key='start_on', value='1600-01-01', role=<_BindRole.COLUMN_VALUE: 2>), 
        'grade': BindParameter(key='grade', value='A', role=<_BindRole.COLUMN_VALUE: 2>)
        }
)

>>> compiled.as_dict()
{
    'operation': {'endpoint': 'pages', 'request': 'create'}, 
    'payload': {
        'parent': {'type': 'database_id', 'database_id': ':database_id'}, 
        'properties': {'name': ':name', 'id': ':id', 'is_active': ':is_active', 'start_on': ':start_on', 'grade': ':grade'
        }
    }
}

>>> compiled.params
{
    'database_id': BindParameter(key='database_id', role=<_BindRole.DBAPI_PARAM: 4>), 
    'name': BindParameter(key='name', value='Galileo Galilei', role=<_BindRole.COLUMN_VALUE: 2>), 
    'id': BindParameter(key='id', value=1, role=<_BindRole.COLUMN_VALUE: 2>), 
    'is_active': BindParameter(key='is_active', role=<_BindRole.COLUMN_VALUE: 2>), 
    'start_on': BindParameter(key='start_on', value='1600-01-01', role=<_BindRole.COLUMN_VALUE: 2>), 
    'grade': BindParameter(key='grade', value='A', role=<_BindRole.COLUMN_VALUE: 2>)
}
```
1. `.values` stores a dict associating column names to bound values through bind parameters 
2. The compiled dict is a template
3. `.params` delivers the values bound to the template bind parameters

### `sql`
Extend the `Compiled` class to handle multiple parameters for bulk inserts. 

* How do you shape the `.compiled` dictionary?
* Is `._execution_binds` affected?
* What does `compiled.params` return?

Extend `NotionCompiler.visit_insert()` to compile a multi-parameters compiled dictionary.

* Where does the compiler get the list of dictionaries? From `._values` or do you need to add a new attribute `.multi_values`?
* How are the bind parameters handled? 
* Is the current `NotionCompiler.__compile_insert_update_values()` method still usable (e.g., cycling thorugh the multiple values?)
* Does the compiler have to set a flag to signal this is a multi-parameter bulk insert?

Extend `ValuesBase.values()` to handle list of dictionaries and tuples.
* Where do you store the list of dictionaries? In `._values` or do you need to add a new attribute `._multi_values`? 
* Do you need to introduce a `._supports_multi_parameters` class attribute or is it better to have kind of `._has_multi_parameters` style attribute?
* What are the advantages of having dedicated multi-parameters attributes?

**Insights from SqlAlchemy**
* It stores the multi-paramters list of dictionary in the DML compile state: `_multi_parameters`.
* It stores a flag in the DML compile state to signal that the insert is multi-parameters one: `._has_multi_parameters`
* It has a specialized methdo in the DML compile state to process multi-paramters: `_process_multi_values(self, statement: ValuesBase)`
* `._process_multi_values()` also checks semantics (for example that there is no mix formats error)
* It stores multi-parameters in a dedicated attribute `ValuesBase._multi_values`.

> First assessment is no need to update `Insert._setup_execution()`. This method just sets up the second phase. Since the second phase is the same, there is no need to change.

No worries—that realization is exactly the turning point where everything clicks. Let’s walk through the **full pipeline end-to-end**, because once you see how payload construction and parameter resolution fit together, the role of `construct_params()` becomes obvious.

---

# 🧠 Big Picture: Two Parallel Worlds

There are **two parallel representations** that only meet at execution time:

## 1. Compiled Template (structure)

Built at **compile time**

```python
compiled.as_dict() →
{
    "operation": {"endpoint": "pages", "request": "create"},
    "payload": {
        "parent": {"database_id": ":database_id"},
        "properties": {
            "name": ":name",
            "id": ":id"
        }
    }
}
```

👉 This is a **template with placeholders**

---

## 2. Parameter Sets (values)

Built at **execution time**

```python
[
    {"name": "Galileo", "id": 1, "database_id": "..."},
    {"name": "Newton", "id": 2, "database_id": "..."},
]
```

👉 This is **data only**

---

# 🎯 The Goal

Execution must produce:

```python
[
    {
        "parent": {"database_id": "..."},
        "properties": {"name": "Galileo", "id": 1}
    },
    {
        "parent": {"database_id": "..."},
        "properties": {"name": "Newton", "id": 2}
    }
]
```

👉 This is where **template + parameters merge**

---

# 🔄 Full Execution Pipeline

Let’s go step by step.

---

# 🧩 STEP 1 — Statement construction

```python
stmt = insert(students).values(name="Galileo", id=1)
```

Internal state:

```python
stmt._single_parameters = {
    "name": "Galileo",
    "id": 1
}
```

---

# 🧩 STEP 2 — Compilation

```python
compiled = stmt.compile(nc)
```

Compiler builds:

## 2.1 Template

```python
compiled._template = {
    "payload": {
        "parent": {"database_id": ":database_id"},
        "properties": {
            "name": ":name",
            "id": ":id"
        }
    }
}
```

---

## 2.2 execution_binds

```python
compiled._compiler_state.execution_binds = {
    "database_id": BindParameter(role=DBAPI_PARAM, value="..."),
    "name": BindParameter(role=COLUMN_VALUE),
    "id": BindParameter(role=COLUMN_VALUE),
}
```

👉 This is the **bridge between template and values**

---

# 🧩 STEP 3 — `ExecutionContext.pre_exec()`

This is where **data is prepared**

---

## 3.1 Distill parameters

```python
self.distilled_params
```

Examples:

| Call                            | distilled_params |
| ------------------------------- | ---------------- |
| `execute(stmt)`                 | `[{}]`             |
| `execute(stmt, {"id": 2})`      | `[{"id": 2}]`    |
| `execute(stmt, [{...}, {...}])` | `[...]`          |

---

## 3.2 Build override sets

```python
overrides = _build_override_sets()
```

Example:

```python
stmt.values(name="default")

execute(stmt, [{"id": 1}, {"id": 2}])
```

👉 Result:

```python
overrides = [
    {"name": "default", "id": 1},
    {"name": "default", "id": 2},
]
```

---

## 3.3 Resolve parameters

```python
resolved = _resolve_parameters(overrides)
```

This calls:

```python
compiled.construct_params(row, group=i)
```

---

# 🧠 THIS is the critical moment

For each row:

```python
construct_params({
    "name": "default",
    "id": 1
})
```

produces:

```python
{
    "name": "default",
    "id": 1,
    "database_id": "...",   # ← injected here
}
```

👉 This is where:

> **DBAPI_PARAM gets merged into user data**

---

# 🧩 STEP 4 — BindParameter materialization

`_resolve_parameters()` wraps values:

```python
{
    "name": BindParameter(..., value="Galileo"),
    "id": BindParameter(..., value=1),
    "database_id": BindParameter(..., value="..."),
}
```

---

# 🧩 STEP 5 — Payload rendering

Now the magic happens:

```python
compiled._template + resolved params
```

---

## Template:

```python
"database_id": ":database_id"
```

## Replace using resolved:

```python
"database_id": resolved["database_id"]
```

---

## Final payload:

```python
{
    "parent": {"database_id": "..."},
    "properties": {
        "name": "Galileo",
        "id": 1
    }
}
```

---

# 🔁 STEP 6 — Bulk handling

For multiple rows:

```python
payload = [
    render(template, row1),
    render(template, row2),
]
```

---

# 🔥 Key Insight (this is the core)

> `construct_params()` is the **only place where template placeholders become real values**

---

# 🧠 Why DBAPI_PARAM must be in `construct_params()`

Because:

| Stage                  | Knows database_id?      |
| ---------------------- | ----------------------- |
| `.values()`            | ❌ NO                    |
| compiler template      | ❌ NO (only placeholder) |
| execution overrides    | ❌ NO                    |
| **construct_params()** | ✅ YES                   |

---

# 🎯 So the responsibility split is:

| Component                | Responsibility                          |
| ------------------------ | --------------------------------------- |
| `.values()`              | user data                               |
| compiler                 | structure + bind definitions            |
| `_build_override_sets()` | row generation                          |
| `construct_params()`     | **value resolution (including system)** |
| renderer                 | template substitution                   |

---

# 🧠 Mental Model (keep this)

```text
Template (":name", ":id", ":database_id")
        +
Resolved Params {"name": ..., "id": ..., "database_id": ...}
        ↓
Final Payload
```

---

# 🔥 Why your earlier confusion happened

You were thinking:

```text
construct_params = override logic
```

But actually:

```text
construct_params = FULL value materialization layer
```

---

# 🚀 Final takeaway

> `construct_params()` is not optional glue—it is the **central junction where user data, execution overrides, and system parameters are unified into a complete payload-ready parameter set**.

---

# 👉 If you want next step

We can now:

👉 walk through **actual code for payload rendering (`render(template, params)`)**
This is the final piece—and it will make the whole system feel complete.



> Why did we choose param_n instead of column names in NotionCompiler._compile_filter_type()?

### Answer:

Because:

* you need uniqueness
* expressions are not 1:1 with columns
* same column can appear **multiple** times

## Cursor management in `ExecutionContext`

| Statement       | Execution Style         | Cursors                 | Parameters            |
| --------------- | ----------------------- | ----------------------- | --------------------- |
| SELECT          | EXCUTE                  | _cursor                 | payload = resolved[0] |
| DELETE          | EXECUTEMANY             | _staged_result_cursor   | payload = resolved[0]
| INSERT single   | EXCEUTEMANY             | _staged_result_cursor   |
| INSERT multi    | INSERTMANYVALUES        | _cursor for insert      |
|                 |                         | _staged_result_cursor   |
|                 |                         | for returning           |


## How `ExecutionContext.parameters` materializes the DBAPI parameters 

### `.pre_exec()` resolves bindings either as a list of dictionaries or as a dictionary
The _resolve_parameters() returns either a dictionary of bind parameters or a list of dictionary of bind parameters


### `.parameters` returns either a list of dictionaries or a dictionary
The attribute `.parameters` now construct the DBAPI parameters based on the execution style

``` python
@property
def parameters(self) -> Union[dict, list[dict]]:
    if self.execution_style == ExecutionStyle.INSERTMANYVALUES:
        if not isinstance(self.payload, list):
            RuntimeError("INSERTMANYVALUES requires DBAPI 'payload' parameter to be a list")

        return [
            {
                "payload": payload
                for payload in self.payload
            }
        ]

    # implementation as is
    if not instance(self.payload, dict):
        RuntimeError("EXECUTE and EXCUTEMANY require DBAPI 'payload' parameter to be a dictionary")

    ...
``` 


### Bind resolution and payload rendering

``` python
# bind parameters for execution with the resolved values
if 'payload' in self.compiled_dict:
    template = self.compiled_dict["payload"]

    if self.execution_style == ExecutionStyle.INSERTMANYVALUES:
        self.payload = [
            self._bind_params(template, dict(param_set))
            for param_set in resolved_parameters
        ]
    else:
        self.payload = self._bind_params(template, resolved_params[0])
```


## Putting all the pieces together

> Why we need a separate `Connection._execute_insert_many()` method?

The bulk insert is a two step operation:
1. it uses the context `._cursor` to execute the operation `{"endpoing": "pages", "request": "create"}` on a multi-payload to insert all the rows. 
    Thus, it requires a `Cursor.executemany()` call
2. if returning, then it uses the `._staged_result_cursor` to store the result of the query to retrieve the newly inserted rows

The bulk insert requires an `Cursor.executemany()` and it differs from the normal insert, which requires just a single `Cursor.execute()`.