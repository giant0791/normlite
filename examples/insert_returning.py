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

def print_result(result: CursorResult, max_col_width: int = 12, indent: int = 0):    
    # 1. Fetch all rows to calculate widths (note: fetchall() consumes the result)
    rows = result.fetchall()

    # 2. Extract column names from the CursorResult
    headers = list(rows[0].keys())

    indent_prefix = " " * indent

    # Helper function to truncate text and add "..."
    def format_val(val):
        s = str(val)
        if len(s) > max_col_width:
            return s[:max_col_width - 3] + "..."
        return s

    # 3. Process all data and calculate dynamic widths (up to max_col_width)
    processed_rows = []
    col_widths = [len(format_val(h)) for h in headers]

    for row in rows:
        formatted_row = [format_val(val) for val in row]
        processed_rows.append(formatted_row)
        # Update widths based on formatted (truncated) values
        for i, val in enumerate(formatted_row):
            col_widths[i] = max(col_widths[i], len(val))

    # 4. Create formatting template
    format_str = "  ".join([f"{{:<{w}}}" for w in col_widths])
    
    # 5. Print Header
    print(f"{indent_prefix}{format_str.format(*[format_val(h) for h in headers])}")
    
    # 6. Print Separator
    print(f"{indent_prefix}{'--'.join(['-' * w for w in col_widths])}")
    
    # 7. Print Data
    for row in processed_rows:
        print(f"{indent_prefix}{format_str.format(*row)}")


def print_use_case(
    result: CursorResult,
    title: str,
    execution_options: ExecutionOptions,
    expected_rowcount: int,
    max_col_width: int = 12,
    indent: int = 0,
) -> None:
    if title:
        print(f"  {title}")
    print(f"  implicit_returning = {execution_options.get("implicit_returning")}")
    print(f"  preserve_row_count = {execution_options.get("preserve_rowcount")}")
    print(f"  expected row count = {expected_rowcount}")
    print(f"  actual row count = {result.rowcount}")
    if result.returned_primary_keys_rows:
        print("  returned primary keys as rows (elements):")
        for pkr in result.returned_primary_keys_rows:
            print(f"    {pkr}")
    else:
        print(f"  returned primary keys as rows = {result.returned_primary_keys_rows}")

    if result.returns_rows:
        print(f"  returned row(s):")
        print_result(result, max_col_width, indent)
    else:
        print("  No rows returned (returned_rows is False)")

    print("=====================================================================================")
    

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

    result = connection.execute(stmt)

    print("\nINSERT implicit eturning behavior examples")
    print_use_case(
        result, 
        "No returning clause, insert sigle row",
        engine.get_execution_options(),
        expected_rowcount=1
    )
    stmt = students.insert()
    connection.execution_options(implicit_returning=True)
    result = connection.execute(
        stmt, 
        {
            'id': 123457, 
            'name': 'Isaac Newton', 
            'grade': 'B', 
            'is_active': False, 
            'started_on': date(1661, 9, 1)
        }
    )

    print_use_case(
        result, 
        "No returning clause, insert sigle row with implicit returning",
        connection.get_execution_options(),
        expected_rowcount=1,
    )

    result = connection.execute(
        stmt,
        parameters=[
            {"id": 123458, "name": "Alice", "grade": "A", "is_active": True, "started_on": "2020-01-01"},
            {"id": 123459, "name": "Bob", "grade": "A", "is_active": True, "started_on": "2021-01-01"},
            {"id": 123460, "name": "Tucker", "grade": "B", "is_active": True, "started_on": "2022-01-01"},
        ],
   )

    print_use_case(
        result,
        "No returning clause, insert multiple rows with implicit returning",
        connection.get_execution_options(),
        expected_rowcount=3
    )

    print("\nINSERT explicit returning behavior examples")
    stmt = (
        insert(students)
        .values(
            id=200000, 
            name='Sarah', 
            grade='A', 
            is_active=False, 
            started_on="2009-09-01"
        )
        .returning(students.c.object_id, students.c.created_at)
    )
    
    connection.execution_options(implicit_returning=False)
    result = connection.execute(stmt)
    print_use_case(
        result,
        "Returning clause with system columns only, insert single row",
        connection.get_execution_options(),
        expected_rowcount=1,
        max_col_width=64,
        indent=4
    )

    stmt = insert(students).returning(*students.c)
    result = connection.execute(
        stmt,
        parameters=[
            {"id": 123458, "name": "John", "grade": "A", "is_active": True, "started_on": "2026-01-01"},
            {"id": 123459, "name": "Mark", "grade": "A", "is_active": True, "started_on": "2026-01-01"},
            {"id": 123460, "name": "Paula", "grade": "B", "is_active": True, "started_on": "2026-01-01"},
        ]
    )
    print_use_case(
        result,
        "Returning clause with system columns only, insert multiple rows",
        connection.get_execution_options(),
        expected_rowcount=3,
        max_col_width=16,
        indent=4
    )
