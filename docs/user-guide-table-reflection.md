# Table Reflection with the Inspector API

Normlite provides **table reflection** to let you inspect existing Notion databases and integrate them seamlessly into your application.
Reflection allows you to discover table existence, column definitions, and to populate `Table` objects with their real schema — **without writing low-level API code**.

## What is Table Reflection?

Table reflection is the process of **reading a database schema from Notion** and translating it into normlite objects such as:

- `Table`
- `Column`
- `TypeEngine`

Reflection is **read-only** and safe.

## The Inspector API

```python
from normlite import inspect

inspector = inspect(engine)
```

### has_table(table_name)

```python
if inspector.has_table("students"):
    print("Table exists")
```

### get_columns(table_name)

```python
columns = inspector.get_columns("students")
for col in columns:
    print(col.name, col.type)
```

Returns a sequence of `ReflectedColumnInfo`.

### reflect_table(table)

```python
from normlite import Table, MetaData

metadata = MetaData()
students = Table("students", metadata)
inspector.reflect_table(students)
```

After reflection, the table is ready for queries.

## The Schema API

`Table` and `MetaData` provide a reflection API as well.

### Table autoload_with

```python
from  normlite import Table, MetaData

metadata = MetaData()
students = Table("students", metadata, autoload_with=engine)
```

### Reflect all tables registered with MetaData
```python
from  normlite import Table, MetaData

metadata = MetaData()
students = Table("students", metadata)
classes = Table("classes", metadata)
exams = Table("exams", metadata)
metadata.reflect(engine)
```

This API is very powerful because it enables to reflect all tables at once.
