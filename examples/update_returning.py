# normlite/examples/insert_returning.py
# Copyright (C) 2025 Gianmarco Antonini
#
# This module is part of normlite and is released under the GNU Affero General Public License.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.from datetime import date

"""
This example shows how to create a new table and insert rows.
It illustrates the returning behavior of the INSERT statement.
It uses an in-memory client. 
"""
from datetime import date

from normlite import (
    create_engine, 
    insert,
    update, 
    select,
    Table, 
    Column,
    String, 
    Integer, 
    Boolean, 
    Date, 
    MetaData,
    ExecutionOptions,
    CursorResult
)

# create bind to in-memory database
engine = create_engine('normlite:///:memory:')
engine.execution_options(
    preserve_rowcount=True, 
    preserve_rowid=False,
    isolation_level="AUTOCOMMIT",
    implicit_returning=False,
    page_size=100,
)

# declare a table
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

# create table and add some rows
with engine.connect() as connection:
    students.create(bind=engine)

    stmt = insert(students)
    result = connection.execute(
        stmt,
        parameters=[
            {"id": 123458, "name": "Alice", "grade": "A", "is_active": True, "started_on": "2020-01-01"},
            {"id": 123459, "name": "Bob", "grade": "A", "is_active": True, "started_on": "2021-01-01"},
            {"id": 123460, "name": "Tucker", "grade": "B", "is_active": True, "started_on": "2022-01-01"},
        ],
    )

    stmt = (
        update(students)
        .values(is_active=False)
        .returning(*students.c)
    )

    result = connection.execute(stmt)

    for row in result.all():
        print(row)

    stmt = (
        update(students)
        .values(is_active=True)
        .where(students.c.name == "Alice")
    )

    result = connection.execute(
        stmt,
        execution_options={"implicit_returning": True}
    )

    for oid in result.returned_primary_keys_rows:
        print(oid)

    stmt = select(students)

    result = connection.execute(stmt)

    for row in result.all():
        print(row)
    
