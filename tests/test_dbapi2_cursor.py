import pdb
import uuid

import pytest
from normlite.notiondbapi.dbapi2 import Cursor, InterfaceError


def test_rowcount_result_set_not_empty(dbapi_cursor: Cursor):
    # Given: 
    #   I called any of the cursor .execute*() methods and the result set contains 2 rows
    #
    # When:
    #   I call .rowcount
    # 
    # Then:
    #   .rowcount returns 2 
    rowcount = dbapi_cursor.rowcount
    assert 2 == rowcount

def test_rowcount_result_set_empty(dbapi_cursor: Cursor):
    # Given: 
    #   I called any of the cursor .execute*() methods and the result set contains 2 rows
    #
    # When:
    #   I call .fetchall()
    # 
    # Then:
    #   - The returned rows are 2 and it is a list
    #   - .rowcount returns 0 (result set is empty after .fetchall())
    rows = dbapi_cursor.fetchall()
    assert 2 == len(rows)
    assert isinstance(rows, list)
    assert 0 == dbapi_cursor.rowcount

def test_row_count_w_consecutive_fetchones(dbapi_cursor: Cursor):
    # Given: 
    #   I called any of the cursor .execute*() methods and the result set contains 2 rows
    #
    # When:
    #   I perform subsequent .fetchone() calls 
    # 
    # Then:
    #   - The returned row is a tuple
    #   - .rowcount decreases after each call until it gets 0 when the result set is empty
    assert 2  == dbapi_cursor.rowcount
    row = dbapi_cursor.fetchone()
    assert isinstance(row, tuple)
    assert 1 == dbapi_cursor.rowcount
    row = dbapi_cursor.fetchone()
    assert 0 == dbapi_cursor.rowcount
    row = dbapi_cursor.fetchone()
    assert row is None
    assert dbapi_cursor.rowcount == -1

def test_row_count_undefined_result_set(dbapi_cursor: Cursor):
    # Given: 
    #   I did not call any of the cursor .execute*() methods so that result set it undefined
    #
    # When:
    #   I call .rowcount
    # 
    # Then:
    #   - The returned row is a -1
    dbapi_cursor._result_set = None
    assert -1 == dbapi_cursor.rowcount
   
def test_lastrowid_one_modified_row(dbapi_cursor: Cursor):
    # Given: 
    #   I called any of the cursor .execute*() methods and the result set contains 2 rows
    #
    # When:
    #   I call .lastrowid
    # 
    # Then:
    #   - It returns the rowid of the last modified row

    # last modified row, for example after an INSERT statement
    dbapi_cursor._parse_result_set(
        {
            "object": "page",
            "id": '680dee41-b447-451d-9d36-c6eaff13fb45',
            "archived": False,
            "in_trash": False,
            "properties": {
                "id": {"id": "%3AUPp","type": "number", "number": 12345},
                "grade": {"id": "A%40Hk", "type": "rich_text", "rich_text": [{"text": {"content": "B"}}]},
                "name": {"id": "BJXS", "type": "title", "title": [{"text": {"content": "Isaac Newton"}}]},
            }
        }
    )

    assert '680dee41-b447-451d-9d36-c6eaff13fb45' == str(uuid.UUID(int=dbapi_cursor.lastrowid))

def test_lastrowid_many_modified_rows(dbapi_cursor: Cursor):
    # Given: 
    #   I called the cursor .executemany() methods and inserted 2 rows
    #
    # When:
    #   I call .lastrowid
    # 
    # Then:
    #   - It returns the rowid of the last modified row (second row in the result set)

    # last modified row, for example after an INSERT statement
    assert '680dee41-b447-451d-9d36-c6eaff13fb46' == str(uuid.UUID(int=dbapi_cursor.lastrowid))

def test_fetchall_returning_multiple_rows(dbapi_cursor: Cursor):
    # Given: 
    #   I called the cursor .executemany() methods and inserted 2 rows
    #
    # When:
    #   I call .rowcount and .fetchall()
    # 
    # Then:
    #   - .rowcount returns the number of inserted rows: 2
    #   - .fetchall() returns the 2 inserted rows
    #   - A subsequent call to .fetchall() returns []

    expected = [
        (
            'page', '680dee41-b447-451d-9d36-c6eaff13fb45', False, False, 
            'id', '%3AUPp', 'number', 12345,
            'grade', 'A%40Hk', 'rich_text', 'B',
            'name', 'BJXS', 'title', 'Isaac Newton',
        ),
        (
            'page', '680dee41-b447-451d-9d36-c6eaff13fb46', True, True,
            'id', 'Iowm', 'number', 67890,
            'grade', 'Jsfb', 'rich_text', 'A',
            'name', 'WOd%3B', 'title', 'Galileo Galilei',
        ),
    ]
    
    rowcount = dbapi_cursor.rowcount
    rows = dbapi_cursor.fetchall()
    assert len(rows) == rowcount
    assert rows == expected

    assert [] == dbapi_cursor.fetchall()

def test_fetchall_undefined_result_set(dbapi_cursor: Cursor):
    # Given: 
    #   I did not call any of the cursor .execute*() methods so that result set is undefined
    #
    # When:
    #   I call .fetchall()
    # 
    # Then:
    #   - It raises an InterfaceError
    dbapi_cursor._result_set = None

    with pytest.raises(InterfaceError, match='Cursor result set is empty.'):
        # shall raise InterfaceError
        new_rows = dbapi_cursor.fetchall()
        assert new_rows is None
        assert dbapi_cursor.lastrowid is None
        assert dbapi_cursor.rowcount == -1

def test_fetchone_returning_first_row(dbapi_cursor: Cursor):
    # Given: 
    #   I called the cursor .executemany() methods and inserted 2 rows
    #
    # When:
    #   I call .fetchone() and .rowcount
    # 
    # Then:
    #   - .fetchone() returns first row inserted 
    #   - .rowcount returns the number of the remaining rows: 1
    expected_1 = (
        'page', '680dee41-b447-451d-9d36-c6eaff13fb45', False, False, 
        'id', '%3AUPp', 'number', 12345,
        'grade', 'A%40Hk', 'rich_text', 'B',
        'name', 'BJXS', 'title', 'Isaac Newton',
    )

    rowcount = dbapi_cursor.rowcount
    assert rowcount == 2
    
    row = dbapi_cursor.fetchone()
    assert row == expected_1
    assert dbapi_cursor.rowcount == rowcount - 1

def test_fetchone_until_result_set_empty(dbapi_cursor: Cursor):
    # Given: 
    #   I called the cursor .executemany() methods and inserted 2 rows
    #
    # When:
    #   I call .fetchone() and .rowcount 2 times
    # 
    # Then:
    #   - .fetchone() returns the remaining row in the result set  
    #   - .rowcount returns the number of the remaining rows
    expected_1 = (
        'page', '680dee41-b447-451d-9d36-c6eaff13fb45', False, False, 
        'id', '%3AUPp', 'number', 12345,
        'grade', 'A%40Hk', 'rich_text', 'B',
        'name', 'BJXS', 'title', 'Isaac Newton',
    )

    expected_2 = (
        'page', '680dee41-b447-451d-9d36-c6eaff13fb46', True, True,
        'id', 'Iowm', 'number', 67890,
        'grade', 'Jsfb', 'rich_text', 'A',
        'name', 'WOd%3B', 'title', 'Galileo Galilei',
    )

    rowcount = dbapi_cursor.rowcount
    assert rowcount == 2
    
    row = dbapi_cursor.fetchone()
    assert row == expected_1
    assert dbapi_cursor.rowcount == rowcount - 1

    rowcount = dbapi_cursor.rowcount
    assert rowcount == 1

    row = dbapi_cursor.fetchone()
    assert row == expected_2
    assert dbapi_cursor.rowcount == 0

    row = dbapi_cursor.fetchone()
    assert row is None
    assert dbapi_cursor.rowcount == -1

def test_fetchone_undefined_result_set(dbapi_cursor: Cursor):
    # Given: 
    #   I did not call any of the cursor .execute*() methods so that result set is undefined
    #
    # When:
    #   I call .fetchone()
    # 
    # Then:
    #   - It raises an InterfaceError
    dbapi_cursor._result_set = None

    with pytest.raises(InterfaceError, match='Cursor result set is empty.'):
        # shall raise InterfaceError
        new_rows = dbapi_cursor.fetchone()
        assert new_rows is None
        assert dbapi_cursor.lastrowid is None
        assert dbapi_cursor.rowcount == -1
