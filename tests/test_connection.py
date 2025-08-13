from __future__ import annotations
import pdb
from typing import Any, List, Optional, Protocol
import pytest

from normlite.cursor import CursorResult
from normlite.sql.dml import Insert, insert
from normlite.sql.sql import CreateTable, text
from normlite.connection import Connection



@pytest.mark.skip('Integration test not ready yet.')
def test_connection_execute_insert_in_txn():
    """This is an integration test combining all the pieces together."""
    conn = Connection()
    students: CreateTable = text("""
        CREATE TABLE students (
            studentid int, 
            name TITLE_VARCHAR(32), 
            grade VARCHAR(1)
        )
    """)

    # create the insert statement
    stmt: Insert = insert(students)

    # first call to exeuct() begins an implicit transaction
    result: CursorResult = conn.execute(stmt, {
        "student_id": 12345678,
        "name": "Galileo Galilei",
        "grade": "A"
    })

    # no result available because no commit issued
    assert not result.first()

    # commit implicit transaction
    conn.commit()

    # now the newly inserted row is available as result
    row = result.first()
    assert row
    assert row["student_id"] == 12345678
    assert row["name"] == "Galileo Galilei"
    assert row["grade"] == "A"
    