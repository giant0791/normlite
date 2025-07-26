# normlite
Get access to Notion databases with the full power of SQL.
```python
>>> from normlite import create_engine, text
>>> NOTION_TOKEN = ...
>>> NOTION_VERSION = '2022-06-28'
>>> # create a proxy object to an internal Notion integration
>>> engine = create_engine(
>>>    f'normlite+auth://internal?token={NOTION_TOKEN}&version={NOTION_VERSION}'
>>> )
>>>
>>> #  get the connection to the integration
>>> conn = engine.connect()
>>>
>>> # create a table
>>> conn.execute(text("create table students (id int, name title_varchar(255), grade varchar(1))"))
>>>
>>> # insert rows 
>>> conn.execute(
>>>     text("insert into students (id, name, grade) values (:id, :name, :grade)"),
>>>     [{"id": 1, "name": "Isaac Newton", "grade": "B"}]
>>> )
>>> conn.execute(
>>>     text("insert into students (id, name, grade) values (:id, :name, :grade)"),
>>>     [{"id": 2, "name": "Galileo Galilei", "grade": "B"}]
>>> )
>>>
>>> # fetch the inserted rows
>>> result = conn.execute('select id, name, grade from students')
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

