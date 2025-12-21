# User Guide

## Working With Notion Databases in normlite

> A quick, friendly introduction to how normlite organizes your Notion data under the hood.

When you use `normlite` with Notion, you don’t have to think about internal storage details. But it is helpful to understand the basic idea of how your tables, schema, and data are arranged so things “just work” the way you expect.

This section gives you a simple mental model — no deep internals, no heavy terminology. Think of it as a map to help you get started quickly and confidently.

## Your Database Lives Inside a Notion Integration

Every database you create with `normlite` is backed by a Notion internal integration.
This integration holds everything related to that database:
- your tables
- the data inside them
- metadata needed by `normlite` to understand the structure

Once you create a Notion integration and share a top-level page with it, you’re ready to go. normlite takes care of the rest.

## What Is the INFORMATION_SCHEMA?

Inside your shared Notion page, normlite automatically creates a small internal space called **INFORMATION_SCHEMA**.

Think of it as a tiny “control room” where `normlite` keeps track of things like:
- which tables exist
- what columns they have
- the types of those columns

You never need to edit anything inside INFORMATION_SCHEMA.
normlite manages it for you, just like a database engine would.

If you’re curious: INFORMATION_SCHEMA is simply a special Notion page containing a few internal tables. But you’re free to ignore it entirely — it’s safe, hidden away, and fully automated.

## Your Data Lives in a Separate “Root Page”

Next to INFORMATION_SCHEMA, `normlite` also sets up a **Database Root Page**.

This is the home for all your tables — the place where your actual data lives.

Whenever you run something like:
```python
engine.execute(text("CREATE TABLE tasks (title TEXT PRIMARY KEY)"))
```
normlite creates a Notion database under your root page.
You can even view or edit that database in the Notion app, just like any normal Notion page.

## Connecting Is Easy

When everything is set up, connecting is as simple as:
```python
engine = create_engine("normlite+auth:///internal?token=YOUR_NOTION_TOKEN&version=NOTION_VERSION")
```
There is **no need to specify a database name**.
Your integration token already defines which database you’re working with.

## Behind the Scenes (Only What You Need to Know)

Here’s the entire system in one sentence:

> **You give `normlite` a token → `normlite` manages an INFORMATION_SCHEMA and a root page → you work with tables normally.**

You don’t need to know the shape of the metadata, the naming conventions, or the layout. Those details matter for advanced users or contributors, but not for getting started.

## What Happens If I Change My Schema Later?

`normlite` is designed to evolve with you.

When you:
- add a table
- drop a table
- rename a column (future feature)

`normlite` updates the INFORMATION_SCHEMA automatically.
You never edit it manually.
You simply write SQL, and normlite keeps everything in sync.

Future `normlite versions` will introduce structured migrations — but even then, upgrading will not require extra work from you.

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
