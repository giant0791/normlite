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
from normlite.engine.base import Connection
from normlite.engine.context import ExecutionContext
from normlite.sql.compiler import NotionCompiler
from normlite.sql.dml import Insert, insert
from normlite.sql.ddl import CreateTable
from normlite.sql.schema import MetaData, Column, Table
from normlite.sql.type_api import Integer, String

@pytest.fixture
def metadata() -> MetaData:
    return MetaData()

@pytest.fixture
def students(metadata: MetaData) -> Table:
    students = Table(
        'students',
        metadata,
        Column('student_id', Integer()),
        Column('name', String(is_title=True)),
        Column('grade', String())
    )

    # IMPORTANT: mock the parent id to avoid "page_id" in the parent object to be None
    students._database_id = '12345678-0000-0000-1111-123456789012'
    return students

def test_insert_can_compile(students: Table, paccessor):
    expected_params = {
        'database_id': '12345678-0000-0000-1111-123456789012',
        'student_id': 1234567,
        'name': 'Galileo Galilei', 
        'grade': 'A',
     }
    stmt: Insert = insert(students).values(**expected_params)
    compiled = stmt.compile(NotionCompiler())
    compile_dict = compiled.as_dict()
    context = ExecutionContext(None, compiled)
    context.setup()
    payload = compile_dict['operation']['payload']

    # check compilation
    assert 'operation' in compile_dict
    assert compile_dict['operation']['endpoint'] == 'pages'
    assert compiled.params == expected_params

    # check parameter bind
    assert payload['parent']['database_id'] == expected_params['database_id']
    assert paccessor.get_number_property_value('student_id', payload) == expected_params['student_id']
    assert paccessor.get_text_property_value('name', 'title', payload) == expected_params['name']
    assert paccessor.get_text_property_value('grade', 'rich_text', payload) == expected_params['grade']

@pytest.mark.skip(reason='Requires refactoring and new info_schema.')
def test_connection_exec_insert(connection: Connection, students: Table):
    # add the students table
    stmt = CreateTable(students)
    connection.execute(stmt)

    # insert a row
    stmt: Insert = insert(students).values(student_id=1234567, name='Galileo Galilei', grade='A')
    result = connection.execute(stmt)
    row = result.one()
    client = connection._engine._client

    assert row[SpecialColumns.NO_ID]
    assert client._get(row[0])

@pytest.mark.skip(reason='returning clause not implemented in the executable.')
def test_connection_exec_insert_returning(connection: Connection, students: Table):
    # add the students table
    stmt = CreateTable(students)
    connection.execute(stmt)

    # insert a row
    stmt: Insert = insert(students).values(student_id=1234567, name='Galileo Galilei', grade='A')
    stmt.returning(students.c.student_id, students.c.name)
    result = connection.execute(stmt)
    row = result.one()
    client = connection._engine._client

    assert row[SpecialColumns.NO_ID]
    assert client._get(row[0])
    assert row.student_id == 1234567
    assert row.name == 'Galileo Galilei'



