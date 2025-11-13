import pdb
import pytest
from normlite._constants import SpecialColumns
from normlite.engine.base import Connection, Engine, create_engine
from normlite.sql.compiler import NotionCompiler
from normlite.sql.dml import Insert, insert
from normlite.sql.ddl import CreateTable
from normlite.sql.schema import MetaData, Column, Table
from normlite.sql.type_api import Integer, String

@pytest.fixture
def engine() -> Engine:
    return create_engine(
        'normlite:///:memory:',
        _mock_ws_id = '12345678-0000-0000-1111-123456789012',
        _mock_ischema_page_id = 'abababab-3333-3333-3333-abcdefghilmn',
        _mock_tables_id = '66666666-6666-6666-6666-666666666666',
        _mock_db_page_id = '12345678-9090-0606-1111-123456789012'
    )

@pytest.fixture
def connection(engine: Engine) -> Connection:
    return engine.connect()

@pytest.fixture
def metadata() -> MetaData:
    return MetaData()

@pytest.fixture
def students(metadata: MetaData) -> Table:
    return Table(
        'students',
        metadata,
        Column('student_id', Integer()),
        Column('name', String(is_title=True)),
        Column('grade', String())
    )

def test_insert_can_compile(students: Table):
    expected_params = {'student_id': 1234567, 'name': 'Galileo Galilei', 'grade': 'A'}
    stmt: Insert = insert(students).values(**expected_params)
    compiled = stmt.compile(NotionCompiler())
    compiled_dict = compiled.as_dict()

    assert 'operation' in compiled_dict
    assert compiled_dict['operation']['endpoint'] == 'pages'
    assert compiled.params == expected_params

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



