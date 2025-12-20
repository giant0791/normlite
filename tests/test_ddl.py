"""This test module focuses on verifying that compilation and parameters binding of the DDL constructs are correct.

Compilation correctness means that tests check 
1. the emitted JSON code is as expected (template with named arguments)
2. the parameters are correctly bound to the named arguments

Note:
    DDL construct execution is not in the test scope here.
"""
from __future__ import annotations
import pdb
import pytest

from normlite.engine.context import ExecutionContext
from normlite.sql.compiler import NotionCompiler
from normlite.sql.ddl import CreateColumn, CreateTable, HasTable
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, Number, String

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
    compile_dict = compiled.as_dict()
    context = ExecutionContext(None, compiled)
    context.setup()
    payload = compile_dict['operation']['payload']

    assert payload['parent']['page_id'] == '12345678-9090-0606-1111-123456789012'
    assert payload['title']['text']['content'] == students.name
    keys = [k for k in students.c.keys() if not k.startswith('_no_')]
    assert list(payload['properties'].keys()) == keys
    col_specs = [c.type_.get_col_spec(None) for c in students.columns if not c.name.startswith('_no_')]
    assert list(payload['properties'].values()) == col_specs

def test_visit_has_table():
    metadata = MetaData()
    students = Table('students', metadata)
    ddl_stmt = HasTable(
        students,
        '66666666-6666-6666-6666-666666666666',             # tables_id
        'university'                                        # table_catalog   
    )
    compiled = ddl_stmt.compile(NotionCompiler())
    compile_dict = compiled.as_dict()
    context = ExecutionContext(None, compiled)
    context.setup()
    payload = compile_dict['operation']['payload']
    no_query_obj = {
        'database_id': '66666666-6666-6666-6666-666666666666',                    # "tables" database id
        'filter': {
            'and': [
                {
                    'property': 'table_name',
                    'title' : {
                        'equals': 'students'
                    }
                },
                {
                    'property': 'table_catalog',
                    'rich_text': {
                        'equals': 'university'     # _catalog_name is the database name containing this table
                    }
                }
            ]
        }
    }

    assert compile_dict['operation']['endpoint'] == 'databases'
    assert compile_dict['operation']['request'] == 'query'
    assert payload == no_query_obj




    





