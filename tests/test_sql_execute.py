from abc import ABC, abstractmethod

import pytest

from normlite.sql import ColumnDef, CreateTable, MetaData, SqlToJsonVisitor, Visitor, Where, text

@pytest.fixture
def create_table_stmt() -> str:
    return """CREATE TABLE students (studentid int, name TITLE_VARCHAR(32), grade VARCHAR(1))"""

@pytest.fixture
def expression() -> str:
    return """"""

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

@pytest.mark.parametrize('sql, notion_json',
    [
        (
            "where (id > 0 and grade = 'C') or name = 'Isaac'",
            {
                "filter": {
                    "or": [
                        {
                            "and": [
                            {
                                "property": "id",
                                "number": {
                                "greater_than": 0
                                }
                            },
                            {
                                "property": "grade",
                                "rich_text": {
                                "equals": "C"
                                }
                            }
                            ]
                        },
                        {
                            "property": "name",
                            "title": {
                            "equals": "Isaac"
                            }
                        }
                    ]
                }
            }
        ),
        (
            "where id != 0 and (grade = 'C' or name = 'Isaac')",
            {
                "filter": {
                    "and": [
                        {
                            "property": "id",
                            "number": {
                                "does_not_equal": 0
                            }
                        },
                        {
                            "or": [
                            {
                                "property": "grade",
                                    "rich_text": {
                                    "equals": "C"
                                    }
                            },
                            {
                                "property": "name",
                                "title": {
                                    "equals": "Isaac"
                                }
                            }
                            ]
                        },
                    ]
                }
            }
        ),       
    ]
)
def test_where_clause(sql: str, notion_json: dict):
    meta = MetaData()
    meta.add(CreateTable(
        'students',
        [
            ColumnDef('id', 'number'),
            ColumnDef('name', 'title'),
            ColumnDef('grade', 'rich_text'),
        ]
    ))
    ast: Where = text(sql)
    ast.table_clause = meta["students"]
    assert ast.table_clause

    visitor = SqlToJsonVisitor()
    assert ast.accept(visitor) == notion_json