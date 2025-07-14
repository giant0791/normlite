# normlite
Get access to Notion databases with the full power of SQL.
```python
>>> from normlite.engine import create_engine
>>> engine = create_engine('my-notion-database', api_key='my-api-key')
>>> cursor = engine.connect()
>>> cursor.execute('create table students (id int, name title_varchar(255), grade varchar(1))')
>>> cursor.execute("insert into students (id, name, grade) values (1, 'Isaac Newton', 'B')")
>>> cursor.execute("insert into students (id, name, grade) values (2, 'Galileo Galilei', 'A')")
>>> result = cursor.execute('select id, name, grade from students')
>>> rows = result.fetchall()
>>> for row in rows:
>>>   print(row)
>>> Row('id': 1, 'name': 'Isaac Newton', 'grade': 'B')
>>> Row('id': 2, 'name': 'Galileo Galilei', 'grade': 'A')
```
# Contributing
Coming soon!

## Build the documentation
You can build the documentation using the following shell command:
```bash
$ uv run python -m sphinx docs docs/html -b html -W
```
The documentation is built under the `docs/html` directory.

