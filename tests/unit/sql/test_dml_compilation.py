"""This test module focuses on verifying that compilation and parameters binding of the DML constructs are correct.

Compilation correctness means that tests check 
1. the emitted JSON code is as expected (template with named arguments)
2. the parameters are correctly bound to the named arguments

Note:
    DML construct execution is not in the test scope here.
"""
from datetime import datetime, date
import uuid
import pdb
import pytest

from normlite.engine.base import Connection, Engine, create_engine
from normlite.engine.context import ExecutionContext
from normlite.engine.row import Row
from normlite.sql.base import _CompileState, Compiled, CompilerState
from normlite.sql.compiler import NotionCompiler
from normlite.sql.dml import insert, select
from normlite.sql.elements import _BindRole, _NoArg, BinaryExpression, BindParameter
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, Money, Numeric, String
from tests.utils.db_helpers import attach_table_oid

def test_insert_bind_values():
    metadata = MetaData()
    students = Table(
        'students', 
        metadata,
        Column('name', String(is_title=True)),
        Column('year_reg', Date())
    )
    stmt = insert(students).values(name='Galileo Galilei', year_reg=datetime(1689, 9, 1))
    name = stmt._values['name']
    year_reg = stmt._values['year_reg']
    assert stmt.is_insert
    assert isinstance(name, BindParameter)
    assert stmt._single_parameters["name"] == 'Galileo Galilei'
    assert isinstance(year_reg, BindParameter)
    assert stmt._single_parameters["year_reg"] == datetime(1689, 9, 1) 

def test_compile_insert_bindparams():
    metadata = MetaData()
    students = Table(
        'students', 
        metadata,
        Column('name', String(is_title=True)),
        Column('year_reg', Date())
    )
    # monkey patch the id to simulate reflection
    database_id = str(uuid.uuid5)
    attach_table_oid(students, database_id)
    stmt = insert(students).values(name='Galileo Galilei', year_reg=datetime(1689, 9, 1))
    sql_compiler = NotionCompiler()
    compiled = stmt.compile(sql_compiler)
    name_bp = compiled._execution_binds['name']
    year_bp = compiled._execution_binds['year_reg']
    
    assert isinstance(name_bp, BindParameter)
    assert name_bp.role is _BindRole.COLUMN_VALUE
    assert name_bp.value == _NoArg.NO_ARG               # exec bindings have no value
    assert isinstance(year_bp, BindParameter)
    assert year_bp.role is _BindRole.COLUMN_VALUE
    assert year_bp.value == _NoArg.NO_ARG

def test_create_ast_for_simple_col_expr():
    metadata = MetaData()
    students = Table('students', metadata, Column('name', String(is_title=True)))
    exp: BinaryExpression = students.c.name == 'Galileo Galilei'
    assert isinstance(exp, BinaryExpression)
    assert isinstance(exp.value, BindParameter)
    assert exp.value.effective_value == 'Galileo Galilei'

def test_compile_binexp_title_eq():
    metadata = MetaData()
    students = Table('students', metadata, Column('name', String(is_title=True)))
    exp: BinaryExpression = students.c.name == 'Galileo Galilei'
    nc = NotionCompiler()
    nc._compiler_state = CompilerState()
    with nc._compiling(new_state=_CompileState.COMPILING_WHERE):
        as_dict = exp._compiler_dispatch(nc)

    assert as_dict['property'] == 'name'
    assert as_dict['title'] == {'equals': ':param_0'}

def test_compile_binexp_title_neq():
    metadata = MetaData()
    students = Table('students', metadata, Column('name', String(is_title=True)))
    exp: BinaryExpression = students.c.name != 'Galileo Galilei'
    nc = NotionCompiler()
    nc._compiler_state = CompilerState()
    with nc._compiling(new_state=_CompileState.COMPILING_WHERE):
        as_dict = exp._compiler_dispatch(nc)

    assert as_dict['property'] == 'name'
    assert as_dict['title'] == {'does_not_equal': ':param_0'}

def test_compile_binexp_title_in():
    metadata = MetaData()
    students = Table('students', metadata, Column('name', String(is_title=True)))
    exp: BinaryExpression = students.c.name.in_('Galileo')
    nc = NotionCompiler()
    nc._compiler_state = CompilerState()
    with nc._compiling(new_state=_CompileState.COMPILING_WHERE):
        as_dict = exp._compiler_dispatch(nc)

    assert as_dict['property'] == 'name'
    assert as_dict['title'] == {'contains': ':param_0'}

def test_compile_binexp_title_not_in():
    metadata = MetaData()
    students = Table('students', metadata, Column('name', String(is_title=True)))
    exp: BinaryExpression = students.c.name.not_in('Galileo')
    nc = NotionCompiler()
    nc._compiler_state = CompilerState()
    with nc._compiling(new_state=_CompileState.COMPILING_WHERE):
        as_dict = exp._compiler_dispatch(nc)

    assert as_dict['property'] == 'name'
    assert as_dict['title'] == {'does_not_contain': ':param_0'}

def test_compile_binexp_title_endswith():
    metadata = MetaData()
    students = Table('students', metadata, Column('name', String(is_title=True)))
    exp: BinaryExpression = students.c.name.endswith('lilei')
    nc = NotionCompiler()
    nc._compiler_state = CompilerState()
    with nc._compiling(new_state=_CompileState.COMPILING_WHERE):
        as_dict = exp._compiler_dispatch(nc)

    assert as_dict['property'] == 'name'
    assert as_dict['title'] == {'ends_with': ':param_0'}

def test_compile_binexp_title_startswith():
    metadata = MetaData()
    students = Table('students', metadata, Column('name', String(is_title=True)))
    exp: BinaryExpression = students.c.name.startswith('lilei')
    nc = NotionCompiler()
    nc._compiler_state = CompilerState()
    with nc._compiling(new_state=_CompileState.COMPILING_WHERE):
        as_dict = exp._compiler_dispatch(nc)

    assert as_dict['property'] == 'name'
    assert as_dict['title'] == {'starts_with': ':param_0'}

def test_compile_binexp_number_operators():
    metadata = MetaData()
    students = Table(
        'students', 
        metadata, 
        Column("name", String(is_title=True)),
        Column('id', Integer()),
        Column('rate', Money(currency='dollar')),
        Column('grade', Numeric())    
    )

    e1 = students.c.id == 123456
    e2 = students.c.rate < 200_000
    e3 = students.c.grade > 1.6

    # reusing the compiler means that
    # the bind param counter increments and does not get reset
    nc = NotionCompiler()                                       
    nc._compiler_state = CompilerState()
    with nc._compiling(new_state=_CompileState.COMPILING_WHERE):
        d1 = e1._compiler_dispatch(nc)
        d2 = e2._compiler_dispatch(nc)
        d3 = e3._compiler_dispatch(nc)

    assert d1['property'] == 'id'
    assert d2['property'] == 'rate'
    assert d3['property'] == 'grade'
    assert d1['number'] == {'equals': ':param_0'}               
    assert d2['number'] == {'less_than': ':param_1'}            # because you are using the same compiler instance
    assert d3['number'] == {'greater_than': ':param_2'}         # because you are using the same compiler instance

def test_compile_binexp_date_before():
    metadata = MetaData()
    students = Table('students', metadata, Column('start_date', Date()), Column("name", String(is_title=True)))
    exp: BinaryExpression = students.c.start_date.before(date.today)
    nc = NotionCompiler()
    nc._compiler_state = CompilerState()
    with nc._compiling(new_state=_CompileState.COMPILING_WHERE):
        as_dict = exp._compiler_dispatch(nc)

    assert as_dict['property'] == 'start_date'
    assert as_dict['date']['before'] == ':param_0'

def test_compile_binexp_date_after():
    metadata = MetaData()
    students = Table('students', metadata, Column('start_date', Date()), Column("name", String(is_title=True)))
    exp: BinaryExpression = students.c.start_date.after(date.today)
    nc = NotionCompiler()
    nc._compiler_state = CompilerState()
    with nc._compiling(new_state=_CompileState.COMPILING_WHERE):
        as_dict = exp._compiler_dispatch(nc)

    assert as_dict['property'] == 'start_date'
    assert as_dict['date']['after'] == ':param_0'

def test_compile_binexp_bool_opertors():
    metadata = MetaData()
    students = Table('students', metadata, Column('is_active', Boolean()), Column("name", String(is_title=True)))
    e1: BinaryExpression = students.c.is_active == True
    e2: BinaryExpression = students.c.is_active != True
    e3: BinaryExpression = students.c.is_active.is_(True)
    e4: BinaryExpression = students.c.is_active.is_not(True)
    nc = NotionCompiler()                                       
    nc._compiler_state = CompilerState()
    with nc._compiling(new_state=_CompileState.COMPILING_WHERE):
        d1 = e1._compiler_dispatch(nc)
        d2 = e2._compiler_dispatch(nc)
        d3 = e3._compiler_dispatch(nc)
        d4 = e4._compiler_dispatch(nc)

    assert d1['property'] == 'is_active'
    assert d1['checkbox']['equals'] == ':param_0'
    assert d2['property'] == 'is_active'
    assert d2['checkbox']['does_not_equal'] == ':param_1'
    assert d3['property'] == 'is_active'
    assert d3['checkbox']['equals'] == ':param_2'
    assert d4['property'] == 'is_active'
    assert d4['checkbox']['does_not_equal'] == ':param_3'

def test_compile_binexp_bool_forbid_truthiness():
    metadata = MetaData()
    students = Table('students', metadata, Column('is_active', Boolean()), Column("name", String(is_title=True)))

    with pytest.raises(TypeError, match='Use explicit comparison.'):
       if students.c.is_active:
           pass

def test_compile_select():
    metadata = MetaData()
    students = Table(
        'students', 
        metadata, 
        Column('name', String(is_title=True)),
        Column('start_date', Date())
    )
    # monkey patch the id to simulate reflection
    database_id = str(uuid.uuid4())
    attach_table_oid(students, database_id)
    stmt = select(students)
    sql_compiler = NotionCompiler()
    compiled = stmt.compile(sql_compiler)
    as_dict = compiled.as_dict()
    assert as_dict['operation']['request'] == 'query'
    assert as_dict['path_params']['database_id'] == ':database_id'
    assert as_dict['payload']['in_trash'] == False

def test_compile_select_w_where():
    metadata = MetaData()
    students = Table('students', metadata, Column('start_date', Date()), Column("name", String(is_title=True)))
    # monkey patch the id to simulate reflection
    database_id = str(uuid.uuid4())
    attach_table_oid(students, database_id)    
    stmt = select(students).where(students.c.start_date.before(date.today))
    sql_compiler = NotionCompiler()
    compiled = stmt.compile(sql_compiler)
    as_dict = compiled.as_dict()
    filter = as_dict['payload']['filter']
    assert filter['property'] == 'start_date'
    start_date_bp = sql_compiler._compiler_state.execution_binds['param_0']
    assert start_date_bp.role is _BindRole.COLUMN_FILTER

def test_compile_select_concat_wheres():
    metadata = MetaData()
    students = Table(
        'students', 
        metadata, 
        Column('name', String(is_title=True)),
        Column('start_date', Date())
    )
    # monkey patch the id to simulate reflection
    database_id = str(uuid.uuid4())
    attach_table_oid(students, database_id)    
    stmt = (
        select(students)
        .where(students.c.name != 'Galileo Galilei')
        .where(students.c.start_date.before(date.today))
    )
    sql_compiler = NotionCompiler()
    compiled = stmt.compile(sql_compiler)
    as_dict = compiled.as_dict()
    filter = as_dict['payload']['filter']
    and_clauselist =  filter['and'] 
    assert len(and_clauselist) == 2
    assert and_clauselist[0]['property'] == 'name'
    assert and_clauselist[1]['property'] == 'start_date'

def test_compile_nested_boolean_clause_list():
    metadata = MetaData()

    projects = Table(
        'projects',
        metadata,
        Column('name', String(is_title=True)),
        Column('done', Boolean()),
        Column('tags', String()),
    )

    # monkey patch the id to simulate reflection
    database_id = str(uuid.uuid4())
    attach_table_oid(projects, database_id)    
    # WHERE:
    # done = true
    # AND (tags contains "A" OR tags contains "B")
    expr = (
        (projects.c.done == True)
        &
        (
            projects.c.tags.in_("A")
            |
            projects.c.tags.in_("B")
        )
    )

    stmt = select(projects).where(expr)

    compiled = stmt.compile(NotionCompiler())
    result = compiled.as_dict()

    assert result['payload']["filter"] == {
        "and": [
            {
                "property": "done",
                "checkbox": {
                    "equals": ":param_0"
                }
            },
            {
                "or": [
                    {
                        "property": "tags",
                        "rich_text": {
                            "contains": ":param_1"
                        }
                    },
                    {
                        "property": "tags",
                        "rich_text": {
                            "contains": ":param_2"
                        }
                    }
                ]
            }
        ]
    }

def test_select_where_filter_binding(engine: Engine):
    metadata = MetaData()

    students = Table(
        'students',
        metadata,
        Column('name', String(is_title=True)),
        Column('start_date', Date()),
    )

    # monkey patch the id to simulate reflection
    database_id = str(uuid.uuid4())
    attach_table_oid(students, database_id)    

    stmt = (
        select(students)
        .where(students.c.start_date.before(date(2020, 1, 1)))
    )

    compiled = stmt.compile(NotionCompiler())
    ctx = ExecutionContext(engine, engine.connect(), engine._dbapi_connection.cursor(), compiled)
    ctx.pre_exec()

    assert ctx.parameters['payload']['filter'] == {
        "property": "start_date",
        "date": {
            "before": "2020-01-01"
        }
    }

