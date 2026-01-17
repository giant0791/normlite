from datetime import date
import uuid
import pytest

from normlite.exceptions import CompileError
from normlite.sql.base import _CompileState
from normlite.sql.compiler import NotionCompiler
from normlite.sql.dml import Select, WhereClause, select
from normlite.sql.elements import _BindRole, BinaryExpression, BindParameter, BooleanClauseList
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, String

"""TDD for all select() use scenarios.

Here you also find the key tests for the big refactor triggered by th new Notion client API.
- The NotionCompiler shall deliver bind parameters in the Compiled.params
"""
@pytest.fixture
def metadata() -> MetaData:
    return MetaData()

@pytest.fixture
def students(metadata: MetaData) -> Table:
    return Table(
        'students',
        metadata,
        Column('name', String(is_title=True)),
        Column('id', Integer()),
        Column('is_active', Boolean()),
        Column('start_on', Date()),
        Column('grade',  String())
    )

@pytest.fixture
def select_stmt(students: Table) -> Select:
    return select(students)

#---------------------------------------------
# Simple column expressions
#---------------------------------------------
def test_str_col_expr(students: Table):
    nc = NotionCompiler()
    exp = students.c.name == 'Galileo Galilei'
    compiled: dict = None
    with nc._compiling(new_state=_CompileState.COMPILING_WHERE):
        compiled = exp._compiler_dispatch(nc)

    assert exp.column is students.c.name
    assert isinstance(exp, BinaryExpression)
    assert isinstance(exp.value, BindParameter)
    assert exp.value.key == 'param_0'
    assert students.c.name.type_ is exp.value.type_
    assert exp.value.effective_value == 'Galileo Galilei'
    assert compiled['property'] == 'name'
    assert compiled['title'] == {'equals': ':param_0'}

def test_int_col_expr(students: Table):
    nc = NotionCompiler()
    exp = students.c.id <= 123456
    compiled: dict = None
    with nc._compiling(new_state=_CompileState.COMPILING_WHERE):
        compiled = exp._compiler_dispatch(nc)

    assert exp.column is students.c.id
    assert isinstance(exp, BinaryExpression)
    assert isinstance(exp.value, BindParameter)
    assert exp.value.key == 'param_0'
    assert students.c.id.type_ is exp.value.type_
    assert exp.value.effective_value == 123456
    assert compiled['property'] == 'id'
    assert compiled['number'] == {'less_than_or_equal_to': ':param_0'}

def test_date_col_expr(students: Table):
    nc = NotionCompiler()
    exp = students.c.start_on.after(date(1690,1,1))
    compiled: dict = None
    with nc._compiling(new_state=_CompileState.COMPILING_WHERE):
        compiled = exp._compiler_dispatch(nc)

    assert exp.column is students.c.start_on
    assert isinstance(exp, BinaryExpression)
    assert isinstance(exp.value, BindParameter)
    assert exp.value.key == 'param_0'
    assert students.c.start_on.type_ is exp.value.type_
    assert exp.value.effective_value == date(1690,1,1)
    assert compiled['property'] == 'start_on'
    assert compiled['date'] == {'after': ':param_0'}

def test_bool_col_expr(students: Table):
    nc = NotionCompiler()
    exp = students.c.is_active.is_(True)
    compiled: dict = None
    with nc._compiling(new_state=_CompileState.COMPILING_WHERE):
        compiled = exp._compiler_dispatch(nc)

    assert exp.column is students.c.is_active
    assert isinstance(exp, BinaryExpression)
    assert isinstance(exp.value, BindParameter)
    assert exp.value.key == 'param_0'
    assert students.c.is_active.type_ is exp.value.type_
    assert exp.value.effective_value == True
    assert compiled['property'] == 'is_active'
    assert compiled['checkbox'] == {'equals': ':param_0'}

#---------------------------------------------
# Generative tests
#---------------------------------------------
def test_where_is_generative(students: Table, select_stmt: Select):
    mocked_db_id = str(uuid.uuid4())
    students.set_oid(mocked_db_id)

    stmt = (
        select_stmt
        .where(students.c.name == 'Galileo Galilei')
        .where(students.c.is_active.is_not(True))   
    )

    nc = NotionCompiler()
    compiled = stmt.compile(nc)

    assert isinstance(stmt._whereclause.expression, BooleanClauseList)
    assert len(stmt._whereclause.expression.clauses) == 2

#---------------------------------------------
# Column projection tests
#---------------------------------------------
def test_colums_projection(students: Table):
    mocked_db_id = str(uuid.uuid4())
    students.set_oid(mocked_db_id)
    stmt: Select = select(students.c.name, students.c.id, students.c.is_active)

    nc = NotionCompiler()
    compiled = stmt.compile(nc)
    asdict = compiled.as_dict()

    assert asdict['query_params']['filter_properties'] == ['name', 'id', 'is_active']
