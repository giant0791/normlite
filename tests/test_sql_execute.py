from abc import ABC, abstractmethod

import pytest

from normlite.sql import CreateTable, text

@pytest.fixture
def create_table_stmt() -> str:
    return """CREATE TABLE students (studentid int, name TITLE_VARCHAR(32), grade VARCHAR(1))"""

def test_text_construct(create_table_stmt: str):
    node = text(create_table_stmt)
    assert isinstance(node, CreateTable)
    assert node.table_name == 'students'
    assert node.columns[0].name == 'studentid'


def test_execute_createtable(create_table_stmt: str):
    table_node = text(create_table_stmt)
    assert isinstance(table_node, CreateTable)

    table_node.compile()

    # IMPORTANT: This approach is very clean and requires the Cursor.execute() to accept
    # JSON objects like this: 
    assert table_node.operation['endpoint'] == 'databases'
    assert table_node.operation['request'] == 'create'
    assert table_node.operation['payload'] == {
        "title": [
            {
                "type": "text", 
                "text": {"content": "students"}
            }
        ],
        "properties": {
            "studentid": {"number": {}},
            "name": {"title": {}},
            "grade": {"rich_text": {}}
        }
    }
