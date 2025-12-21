"""This test module focuses on verifying that compilation and parameters binding of the DML constructs are correct.

Compilation correctness means that tests check 
1. the emitted JSON code is as expected (template with named arguments)
2. the parameters are correctly bound to the named arguments

Note:
    DML construct execution is not in the test scope here.
"""
import pdb
import pytest
from normlite._constants import SpecialColumns
from normlite.engine.base import Connection, Engine, create_engine
from normlite.engine.cursor import CursorResult
from normlite.engine.row import Row
from normlite.sql.dml import Insert, insert
from normlite.sql.ddl import CreateTable
from normlite.sql.schema import MetaData, Table

@pytest.fixture
def engine() -> Engine:
    return create_engine(
        'normlite:///:memory:',
        _mock_ws_id = '12345678-0000-0000-1111-123456789012',
        _mock_ischema_page_id = 'abababab-3333-3333-3333-abcdefghilmn',
        _mock_tables_id = '66666666-6666-6666-6666-666666666666',
        _mock_db_page_id = '12345678-9090-0606-1111-123456789012'
    )

def create_students_db(engine: Engine) -> None:
    # create a new table students in memory
    db = engine._client._add('database', {
        'parent': {
            'type': 'page_id',
            'page_id': engine._db_page_id
        },
        "title": [
            {
                "type": "text",
                "text": {
                    "content": "students",
                    "link": None
                },
                "plain_text": "students",
                "href": None
            }
        ],
        'properties': {
            'student_id': {'number': {}},
            'name': {'title': {}},
            'grade': {'rich_text': {}},
            'is_active': {'checkbox': {}}
        }
    })

    # add the students to tables
    engine._client._add('page', {
        'parent': {
            'type': 'database_id',
            'database_id': engine._tables_id
        },
        'properties': {
            'table_name': {'title': [{'text': {'content': 'students'}}]},
            'table_schema': {'rich_text': [{'text': {'content': ''}}]},
            'table_catalog': {'rich_text': [{'text': {'content': 'memory'}}]},
            'table_id': {'rich_text': [{'text': {'content': db.get('id')}}]}
        }
    })

def test_insert_can_add_new_row(engine: Engine):
    expected_values= {
        'student_id': 1,
        'name': 'Galileo Galilei',
        'grade': 'A',
        'is_active': False
    }
    create_students_db(engine)
    metadata = MetaData()
    students = Table('students', metadata, autoload_with=engine)
    assert 'is_active' in students.c

    stmt: Insert = insert(students).values(**expected_values)
    with engine.connect() as connection:
        result: CursorResult = connection.execute(stmt)
        row: Row = result.one()
        assert row._no_id
        with pytest.raises(AttributeError):
            # This should raise an AttributeError as special columns only are returned
            assert row.student_id




