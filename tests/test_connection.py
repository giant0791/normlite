from __future__ import annotations
import copy
import pdb
import pytest

from normlite._constants import SpecialColumns
from normlite.cursor import CursorResult, Row
from normlite.engine.base import Engine, Inspector, create_engine
from normlite.engine.context import ExecutionContext
from normlite.notiondbapi.dbapi2 import Cursor
from normlite.sql.ddl import CreateTable
from normlite.sql.dml import insert
from normlite.sql.schema import Column, Table, MetaData
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

def test_bind_params(engine: Engine):
    # create table
    metadata = MetaData()
    students: Table = Table(
        'students',
        metadata,
        Column('student_id', Integer()),
        Column('name', String(is_title=True)),
        Column('grade', String())
    )

    # create the DDL statement object corresponding to CREATE TABLE
    stmt = insert(students).values(student_id=123456, name='Galileo Galilei', grade='A')    

    # compile the statement
    compiled = stmt.compile(engine._sql_compiler)
    ctx = ExecutionContext(None, compiled)
    ctx.setup()
    bound_payload = ctx._compiled._compiled['operation']['payload']
    payload_properties = bound_payload['properties']

    assert payload_properties['student_id'] == {'number': 123456}
    assert payload_properties['name'] == {'title': [{'text': {'content': 'Galileo Galilei'}}]}
    assert payload_properties['grade'] == {'rich_text': [{'text': {'content': 'A'}}]}

def test_connection_execute_create_table(engine: Engine):
    # create table
    metadata = MetaData()
    students: Table = Table(
        'students',
        metadata,
        Column('student_id', Integer()),
        Column('name', String(is_title=True)),
        Column('grade', String())
    )

    # get the inspector
    inspector: Inspector = engine.inspect()
    assert inspector.get_id(students) is None

    # create the DDL statement object corresponding to CREATE TABLE
    stmt = CreateTable(students)
    connection = engine.connect()
    result = connection.execute(stmt)
    id_ = inspector.get_id(students)
    assert id_
    assert inspector.get_id(students.c.name) == 'title'

def test_connection_execute_insert(engine: Engine):
    # create table
    metadata = MetaData()
    students: Table = Table(
        'students',
        metadata,
        Column('student_id', Integer()),
        Column('name', String(is_title=True)),
        Column('grade', String())
    )

    # create the DDL statement object corresponding to CREATE TABLE
    create_stmt = CreateTable(students)
    insert_stmt = insert(students).values(
            student_id=123456, 
            name='Galileo Galilei', 
            grade='A'
        ).returning(
            students.c.student_id,
            students.c.name,
            students.c.grade
        )    

    connection = engine.connect()
    connection.execute(create_stmt)
    result = connection.execute(insert_stmt)
    row = result.one()
    client = connection._engine._client

    assert row[SpecialColumns.NO_ID]
    assert client._get(row[0])
    
