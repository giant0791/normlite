# normlite/examples/bulk_insert.py
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
This example shows how to add rows to a table using a bulk insert.
It uses an in-memory client. 
"""


from normlite import (
    create_engine, 
    insert, 
    select,
    Table, 
    Column,
    String, 
    Date,
    DateTimeRange,
    Integer, 
    MetaData,
)

# create bind to in-memory database
engine = create_engine('normlite:///:memory:')

# declare a table
metadata = MetaData()
projects = Table(
    "projects",
    metadata,
    Column("name", String(is_title=True)),
    Column("start", Date()),
    Column("duration", Integer())
)

# create the table
projects.create(bind=engine)

# insert a batch of data
stmt = (
    insert(projects)
    .returning(*projects.c)
)

with engine.connect() as connection:
    result = connection.execute(
        stmt,
        parameters=[
            {"name": "project_x", "start": DateTimeRange(start_datetime="2022-01-01"), "duration": 32},
            {"name": "project alpha", "start": DateTimeRange(start_datetime="2023-02-01"), "duration": 64},
            {"name": "project_beta", "start": DateTimeRange(start_datetime="2025-06-01"), "duration": 128},            
        ]
    )
    rows = result.all()

for row in rows:
    print(row)