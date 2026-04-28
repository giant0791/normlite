from operator import itemgetter
import pdb
import uuid
import pytest

from normlite.exceptions import ArgumentError
from normlite.sql.base import _CompileState, CompilerState
from normlite.sql.compiler import NotionCompiler
from normlite.sql.dml import Delete, delete 
from normlite.sql.elements import BooleanClauseList
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
def delete_stmt(students: Table) -> Delete:
    return delete(students)

#---------------------------------------------
# Operation generation tests
#---------------------------------------------
def test_delete_generate_operation_no_where_clause(students: Table, delete_stmt: Delete):
    mocked_db_id = str(uuid.uuid4())
    students._sys_columns["object_id"]._value = mocked_db_id

    stmt = (
        delete_stmt
    )

    nc = NotionCompiler()
    compiled = stmt.compile(nc)
    asdict = compiled.as_dict()

    assert asdict['operation']['endpoint'] == 'databases'
    assert asdict['operation']['request'] == 'query'
    assert asdict['path_params']['database_id'] == ":database_id"
    assert "in_trash" in asdict["payload"]
    assert "page_size" in asdict["payload"]
    assert compiled._compiler_state.is_delete

#---------------------------------------------
# WHERE tests
#---------------------------------------------
def test_delete_generate_operation_with_where_clause(students: Table, delete_stmt: Delete):
    mocked_db_id = str(uuid.uuid4())
    students._sys_columns["object_id"]._value = mocked_db_id

    stmt = (
        delete_stmt
        .where(students.c.name == 'Galileo Galilei')
    )

    nc = NotionCompiler()
    compiled = stmt.compile(nc)
    asdict = compiled.as_dict()

    assert asdict['operation']['endpoint'] == 'databases'
    assert asdict['operation']['request'] == 'query'
    assert asdict['path_params']['database_id'] == ":database_id"
    assert "in_trash" in asdict["payload"]
    assert "page_size" in asdict["payload"]
    assert "filter" in asdict["payload"]
    assert asdict["payload"]["filter"]["property"] == "name"
    assert "title" in asdict["payload"]["filter"]
    assert asdict["payload"]["filter"]["title"] == {"equals": ":param_0"}
    assert 'param_0' in compiled._execution_binds
    assert 'Galileo Galilei' == compiled._execution_binds['param_0'].effective_value

def test_where_generative_multi_clause(students: Table, delete_stmt: Delete):
    mocked_db_id = str(uuid.uuid4())
    students._sys_columns["object_id"]._value = mocked_db_id

    stmt = (
        delete_stmt
        .where(students.c.name == 'Galileo Galilei')
        .where(students.c.id <= 1000)
    )

    nc = NotionCompiler()
    compiled = stmt.compile(nc)
    asdict = compiled.as_dict()

    assert isinstance(stmt._whereclause.expression, BooleanClauseList)
    assert len(stmt._whereclause.expression.clauses) == 2
    assert 'filter' in asdict['payload']
    assert asdict['payload']['filter'] == {
        'and': [
            {
                'property': 'name', 
                'title': {
                    'equals': ':param_0'
                }
            },
            {
                'property': 'id',
                'number': {
                    'less_than_or_equal_to': ':param_1'
                }
            }
        ]        
    }

    assert 'param_0' in compiled._execution_binds
    assert 'param_1' in compiled._execution_binds
    assert len(compiled._execution_binds) == 2 + 1        # :database_id is also a bind parameter!

#---------------------------------------------
# generative returning() tests
#---------------------------------------------
get_oid = itemgetter(0)
get_is_archived = itemgetter(1)
get_is_deleted = itemgetter(2)
get_created_at = itemgetter(3)

def test_delete_always_returns_no_cols_without_returning(students: Table, delete_stmt: Delete):
    mocked_db_id = str(uuid.uuid4())
    students._sys_columns["object_id"]._value = mocked_db_id

    assert len(delete_stmt._returning) == 0

def test_returning_generative(students: Table, delete_stmt: Delete):
    mocked_db_id = str(uuid.uuid4())
    students._sys_columns["object_id"]._value = mocked_db_id

    stmt = (
        delete_stmt
        .returning(students.c.name)
        .returning(students.c.grade)
    )

    get_name = itemgetter(0)
    get_grade = itemgetter(1)
    assert len(stmt._returning) == 2
    assert get_name(stmt._returning) is students.c.name
    assert get_grade(stmt._returning) is students.c.grade

def test_returning_raises_if_col_not_in_table(students: Table, metadata: MetaData, delete_stmt: Delete):
    wrong_table = Table("wrong_table", metadata, Column("wrong_column", String(is_title=True)))

    with pytest.raises(ArgumentError) as exc:
        delete_stmt.returning(wrong_table.c.wrong_column)

    error = str(exc.value)
    assert "students" in error
    assert "wrong_column" in error

def test_accumulation_has_deduplication_and_warns(students: Table, delete_stmt: Delete):
    mocked_db_id = str(uuid.uuid4())
    students._sys_columns["object_id"]._value = mocked_db_id

    with pytest.warns(UserWarning):
        stmt = (
            delete_stmt
            .returning(students.c.name)
            .returning(students.c.name)
        )


    assert len(stmt._returning) == 1
    assert stmt._returning == (students.c.name,)
