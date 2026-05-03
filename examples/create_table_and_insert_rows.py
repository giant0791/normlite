# normlite/examples/create_table_insert_rows.py
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
It illustrates how to use the CREATE TABLE and INSERT constructs.
It uses an in-memory client. 
"""
from datetime import date

from normlite import (
    create_engine, 
    insert, 
    select,
    Table, 
    Column,
    String, 
    Integer, 
    Boolean, 
    Date, 
    MetaData,
)

# create bind to in-memory database
engine = create_engine('normlite:///:memory:')

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
    
    stmt = (
        insert(students)
        .values(
            id=123456, 
            name='Galileo Galilei', 
            grade='A', 
            is_active=False, 
            started_on=date(1581, 9, 1)
        )
    )

    connection.execute(stmt)
    
    stmt = students.insert()
    connection.execute(
        stmt, 
        {
            'id': 123457, 
            'name': 'Isaac Newton', 
            'grade': 'B', 
            'is_active': False, 
            'started_on': date(1661, 9, 1)
        }
    )

    stmt = (
        select(students)
        .where(students.c.is_active.is_not(True))
    )
    result = connection.execute(stmt)
    rows = result.all()

    # print values for user defined columns only
    for row in rows:
        print(row)