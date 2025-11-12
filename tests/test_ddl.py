from __future__ import annotations
import pdb
from typing import Protocol
import pytest

from normlite.engine.base import Engine, create_engine
from normlite.sql.compiler import NotionCompiler
from normlite.sql.ddl import CreateColumn, CreateTable
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, Number, String

@pytest.fixture
def engine() -> Engine:
    return create_engine(
        'normlite:///:memory:',
        _mock_ws_id = '12345678-0000-0000-1111-123456789012',
        _mock_ischema_page_id = 'abababab-3333-3333-3333-abcdefghilmn',
        _mock_tables_id = '66666666-6666-6666-6666-666666666666',
        _mock_db_page_id = '12345678-9090-0606-1111-123456789012'
    )

def test_visit_column_number():
    num_col = CreateColumn(Column('id', Number('dollar')))
    compiled = num_col.compile(NotionCompiler())
    #pdb.set_trace()
    num_property = compiled.as_dict()
    assert num_property['id'] == {'number': {'format': 'dollar'}}

def test_visit_table():
    metadata = MetaData()
    students = Table(
        'students',
        metadata,
        Column('id', Integer()),
        Column('name', String(is_title=True)),
        Column('grade', String()),
        Column('is_active', Boolean()),
        Column('started_on', Date())
    )
    # mock the database id
    students._db_parent_id = '12345678-9090-0606-1111-123456789012'
    
    ddl_stmt = CreateTable(students)
    compiled = ddl_stmt.compile(NotionCompiler())
    create_table = compiled.as_dict()
    template = create_table.get('operation').get('template')
    assert template.get('parent')
    assert template['parent']['page_id'] == '12345678-9090-0606-1111-123456789012'
    assert template.get('title')
    assert template['title']['text']['content'] == students.name
    keys = [k for k in students.c.keys() if not k.startswith('_no_')]
    assert list(template['properties'].keys()) == keys
    col_specs = [c.type_.get_col_spec(None) for c in students.columns if not c.name.startswith('_no_')]
    assert list(template['properties'].values()) == col_specs

    





