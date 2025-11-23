# `normlite`: SQL Meets Notion

**Query. Create. Update. Drop.**  
All with the power of **SQL**, directly on your **Notion** workspaces.

`normlite` is a Python library that brings SQL capabilities to the [Notion API](https://developers.notion.com/). It allows developers to treat Notion pages and databases like real tables — using familiar SQL constructs like `SELECT`, `INSERT`, `UPDATE`, `DELETE`, and even DDL operations like `CREATE TABLE` and `DROP TABLE`.

This is *Notion for developers*, the way it should have been.

---

## What normlite Can Do

- Connect to Notion using an internal integration
- Run SQL queries to interact with Notion databases
- Automatically map Notion property types (`rich_text`, `number`, `title`, etc.) to Python types (`str`, `int`, `float`, etc.)
- Provide a familiar `SQLAlchemy`-inspired interface

Here’s a sneak peek of what using `normlite` feels like:

```python
from normlite import create_engine, text

NOTION_TOKEN = 'your-secret-token'
NOTION_VERSION = '2022-06-28'

# Create a Notion engine using internal integration
engine = create_engine(
    f'normlite+auth://internal?token={NOTION_TOKEN}&version={NOTION_VERSION}'
)

conn = engine.connect()

# Create a Notion database
conn.execute(text("CREATE TABLE students (id int, name title_varchar(255), grade varchar(1))"))

# Insert some students
conn.execute(text(
    "INSERT INTO students (id, name, grade) VALUES (:id, :name, :grade)"
), [
    {"id": 1, "name": "Isaac Newton", "grade": "B"},
    {"id": 2, "name": "Galileo Galilei", "grade": "A"},
])

# Query the students
rows = conn.execute("SELECT id, name, grade FROM students").fetchall()
for row in rows:
    print(row)
# Row(1, 'Isaac Newton', 'B',)
# Row(2, 'Galileo Galilei', 'A',)
```

# Why I Started This Project

I love Notion. I love SQL.
But I found working with the Notion API... verbose.

So I asked myself:

> “What if interacting with Notion was as simple as writing SQL?”

And just like that, `normlite` was born — a personal learning journey to:

1. Deepen my Python and software architecture skills

2. Learn what it takes to ship a real open source project

3. Share something useful with the developer community

If you feel the same friction when working with Notion, this project is for you.

# Vision

My long-term goal is to create a full-featured ORM framework for Notion —
a tool that lets developers query, model, and manipulate Notion data the same way they do with relational databases using tools like SQLAlchemy or Django ORM.

# Roadmap
## v1.0.0 — Alpha

* SQL-style access to Notion resources

* DDL & DML support (create, insert, update, delete, drop)

* Support for simulated and test integrations (in-memory and file-based)

* Schema reflection from existing Notion databases

## v1.0.0 — Internal Integrations

* Full support for internal integrations ([Notion API version 2025-09-03](https://developers.notion.com/docs/upgrade-guide-2025-09-03))

## v2.0.0 — Transaction Management

* Flask proxy to enable transactions for `normlite` apps

* Support for isolation level "READ COMMITTED"

## v3.0.0 — ORM Power

* Class-based mapping between Python and Notion pages

# Help Me Shape This

This project is at a very early stage, and your feedback would be invaluable.
Are we solving the right problem? Does this API make sense? What's missing?
Ways you can help:

* ⭐ Star this repo to show interest

* 🧠 Open an issue with your ideas or pain points

* 🧪 Try the code, fork it, and share what worked or didn’t

* 💬 Leave a comment, even just to say hi or why this is interesting to you

I'm building normlite in public, and I'd love for you to be part of that journey.

# A Note on Time & Commitment

This is a passion project, and I’m juggling it with many other responsibilities.

So here’s the deal:

* I’ll do my best to make steady progress

* I’ll welcome all kinds of input and ideas

* But I may go quiet at times — don’t take it personally 💛

# License
``normlite`` was created by Gianmarco Antonini. It is licensed under the terms of the GNU Affero General Public License v3.0 (AGPL-3.0-or-later).
See the [LICENSE](../LICENSE) file for details.

# Contributing

Contributions are very welcome, but my current focus is getting v1.0.0 stable before merging external PRs.

In the meantime, you’re encouraged to:

* Fork the repo

* Explore and experiment

* Raise issues, ideas, or improvement proposals

# Thanks for Reading

Let me know what you think — the good, the bad, the confusing.
I’m eager to know if `normlite` can become a tool you want to use.

Let’s build something great together.

— *Gianmarco Antonini*