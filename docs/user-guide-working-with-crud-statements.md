#  Working with normlite CRUD statements
## Intro
In this guide, you will learn what CRUD constructs `normlite` provides.

You will apply CRUD statements on the following table:
``` python
metadata = MetaData()
students = Table(
    'students',
    metadata,
    Column('id', Integer()),
    Column('name', String(is_title=True)),
    Column('grade', String()),
    Column('is_active', Boolean()),
    Column('started_on', Date()),
)
```

## The `insert()` constructor
To add new rows into a table, use the `insert()` constructor:
``` python
stmt = insert(students)
```
The `insert()` constructor procures a `Insert` object and it connects it to the table `students`.
Now, you can add rows in several ways.

```{seealso}
- {py:class}`normlite.sql.dml.Insert` for a comprehensive class documentation
- {py:func}`normlite.sql.dml.insert` for the constructor 
```
### Add rows with the `values()` clause.
The `Insert` class provides the API `values()` to specify rows.
The most simple form is to provide keyword arguments list specifying the value for each columns, as follows:
``` python
stmt = stmt.values(
    id=123456, 
    name='Galileo Galilei', 
    grade='A', 
    is_active=False, 
    started_on=date(1581, 9, 1)
)
```

You can also pass in a tuple with all values:
```python
stmt = stmt.values((123456, 'Galileo Galilei', 'A', False, date(1581, 9, 1)))
```

Or you can pass a mapping, like this:
```python
stmt = stmt.values({
    "id": 123456, 
    "name": "Galileo Galilei", 
    "grade": "A", 
    "is_active": False, 
    "started_on": date(1581, 9, 1)
})
```

You can also insert multiple rows using `.values()`.
Just provide a list of row values (e.g. a list of mappings).
``` python
stmt = stmt.values([
    {
        "id": 123456, 
        "name": "Galileo Galilei", 
        "grade": "A", 
        "is_active": False, 
        "started_on": date(1581, 9, 1)
    },
    {
        "id": 123457, 
        "name": "Isaac Newton", 
        "grade": "B", 
        "is_active": False, 
        "started_on": date(1705, 9, 1)
    },
    {
        "id": 123458, 
        "name": "Ada Lovelace", 
        "grade": "C", 
        "is_active": False, 
        "started_on": date(1867, 9, 1)
    },
])
``` 

### Add rows with the `parameters` keyword argument in `.execute()`
This is likely the most common way to add rows.
```python
result = connection.execute(
    stmt,
    parameters={
        "id": 123456, 
        "name": "Galileo Galilei", 
        "grade": "A", 
        "is_active": False, 
        "started_on": date(1581, 9, 1)
    }
)
```

The `parameters` argument accepts both sigle parameters (e.g., a mapping) or multiple parameters (e.g., a sequence of mappings):
```python 
result = connection.execute(
    stmt,
    parameters=[
    {
        "id": 123456, 
        "name": "Galileo Galilei", 
        "grade": "A", 
        "is_active": False, 
        "started_on": date(1581, 9, 1)
    },
    {
        "id": 123457, 
        "name": "Isaac Newton", 
        "grade": "B", 
        "is_active": False, 
        "started_on": date(1705, 9, 1)
    },
    {
        "id": 123458, 
        "name": "Ada Lovelace", 
        "grade": "C", 
        "is_active": False, 
        "started_on": date(1867, 9, 1)
    },
])
```

### Return behavior for INSERT statements.
You can specify which columns the `Insert` object should return in the result rows by using the `.returning()` method.
The following example lets you return all columns, including the system columns, in the result rows.
```python
stmt = insert(students).returning(*students.c)
result = connection.execute(
    stmt,
    parameters=[
        {"id": 123458, "name": "John", "grade": "A", "is_active": True, "started_on": "2026-01-01"},
        {"id": 123459, "name": "Mark", "grade": "A", "is_active": True, "started_on": "2026-01-01"},
        {"id": 123460, "name": "Paula", "grade": "B", "is_active": True, "started_on": "2026-01-01"},
    ]
)
```
When you use the `.returning()` method, `.returned_primary_keys_rows` returns `None` and the `implicit_returning` execution option is ignored.

## The `delete()` constructor
To delete rows from a table, use the `delete()` constructor:
``` python
stmt = (
    delete(students)
    .where(students.c.started_on.after("2020-02-01") & students.c.started_on.before("2022-01-01"))
)
```
The `delete()` constructor procures a `Delete` object and it connects it to the table `students`.
Now, you can add rows in several ways.

```{seealso}
- {py:class}`normlite.sql.dml.Delete` for a comprehensive class documentation
- {py:func}`normlite.sql.dml.delete` for the constructor 
```

## Summary of returning behavior for INSERT/DELETE
| Feature                            | Implicit Returning | Explicit `.returning()` |
| ---------------------------------- | ------------------ | ----------------------- |
| Requires API post-fetch            | No                 | Yes                     |
| Returns full rows                  | No                 | Yes                     |
| Returns primary keys               | Yes                | No                      |
| Uses `returned_primary_keys_rows` | Yes                | No                      |
| Performance                        | Fast               | Slower (extra API call) |

> If performance is important, set the execution option `implicit_returning` to `True` and store the `_returned_primary_keys_rows` for later use.

```{note}
{py:attr}`normlite.engine.cursor.CursorResult.returned_primary_keys_rows` provides a sequence of one-value tuples containing the object ids of the deleted rows.
```
