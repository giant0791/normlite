from datetime import date
import uuid
import pytest

from normlite.exceptions import CompileError
from normlite.sql.base import _CompileState
from normlite.sql.compiler import NotionCompiler
from normlite.sql.dml import ValuesBase, insert
from normlite.sql.elements import _BindRole, BindParameter
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, String

"""TDD for all insert() use scenarios.

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
def insert_values() -> dict:
    return dict(
        name = 'Galileo Galilei',
        id=123456,
        is_active=False,
        start_on=date(1690,1,1),
        grade='A'
    )

#---------------------------------------------
# Generative values()
#---------------------------------------------
def test_values_is_generative(students: Table):
    stmt = ValuesBase(students)
    stmt = stmt.values(name='Galileo Galilei').values(is_active=True)
    
    assert len(stmt._values) == 2
    assert stmt._values['name'].value == 'Galileo Galilei'
    assert stmt._values['is_active'].value

#---------------------------------------------
# Compilation tests
#---------------------------------------------

def test_compiler_dbapi_param_correctness(students: Table, insert_values: dict):
    mocked_db_id = str(uuid.uuid4())
    students.set_oid(mocked_db_id)
    stmt = insert(students).values(**insert_values)

    nc = NotionCompiler()
    compiled = stmt.compile(nc)
    as_dict = compiled.as_dict()

    assert not compiled.is_ddl
    assert as_dict['payload']['parent']['database_id'].lstrip(':') in compiled.params
    assert 'database_id' in compiled.params

def test_compiler_correctness(students: Table, insert_values: dict):
    """Does the refactored compiler generates code correctly according to new client?"""
    stmt = insert(students).values(**insert_values)

    compiled = stmt.compile(NotionCompiler())
    as_dict = compiled.as_dict()

    assert not compiled.is_ddl
    assert 'operation' in as_dict
    assert 'payload' in as_dict
    assert as_dict['operation'] == dict(endpoint='pages', request='create')
    assert all(
        [   
            key in as_dict['payload']['properties'].keys() 
            for key in insert_values.keys() 
        ]
    )
    pairs = zip(stmt._values.keys(), as_dict['payload']['properties'].values()) 
    assert all(
        [
            f':{key}' == param
            for key, param in pairs
        ]
    )

def test_compiler_generates_parent_id_as_bindparams(students: Table):
    """Does the compiler generates bind parameters for the payload?"""
    mocked_db_id = str(uuid.uuid4())
    students.set_oid(mocked_db_id)
    stmt = insert(students).values(
        name = 'Galileo Galilei',
        id=123456,
        is_active=False,
        start_on=date(1690,1,1),
        grade='A'
    )

    compiled = stmt.compile(NotionCompiler())

    assert 'database_id' in compiled.params
    assert isinstance(compiled.params['database_id'], BindParameter)
    assert compiled.params['database_id'].value == mocked_db_id
    assert compiled.params['database_id'].role == _BindRole.DBAPI_PARAM
    assert compiled.as_dict()['payload']['parent']['database_id'] == ':database_id'

def test_compiler_generates_values_as_bindparams(students: Table, insert_values: dict):
    """Does the compiler generates bind parameters for the Insert.values()?"""
    nc = NotionCompiler()
    mocked_db_id = str(uuid.uuid4())
    students.set_oid(mocked_db_id)
    stmt = insert(students).values(**insert_values)

    compiled = stmt.compile(nc)

    assert nc._compiler_state.is_insert
    assert compiled.params['name'].value == 'Galileo Galilei'
    assert compiled.params['name'].role == _BindRole.COLUMN_VALUE
    assert compiled.params['id'].value == 123456
    assert compiled.params['id'].role == _BindRole.COLUMN_VALUE
    assert not compiled.params['is_active'].value 
    assert compiled.params['is_active'].role == _BindRole.COLUMN_VALUE
    assert compiled.params['start_on'].value == date(1690,1,1)
    assert compiled.params['start_on'].role == _BindRole.COLUMN_VALUE
    assert compiled.params['grade'].value == 'A'
    assert compiled.params['grade'].role == _BindRole.COLUMN_VALUE

def test_compiler_detects_missing_values(students: Table, insert_values: dict):        
    nc = NotionCompiler()
    mocked_db_id = str(uuid.uuid4())
    students.set_oid(mocked_db_id)
    stmt = insert(students)
    stmt.values(**insert_values)

    with pytest.raises(CompileError, match='in INSERT statment: 5 != 4'):
        insert_values.pop('name')
        stmt._values = insert_values
        compiled = stmt.compile(nc)

#---------------------------------------------
# NotionCompiler._add_bindparam() failure
# modes tests
#---------------------------------------------

def test_bindparam_role_already_assigned_error():
    nc = NotionCompiler()
    with pytest.raises(CompileError, match='role already assigned'):
        bp = BindParameter(key='fake')
        bp.role = _BindRole.COLUMN_VALUE
        nc._add_bindparam(bp)

def test_bindparam_invalid_compile_state_error(students: Table):
    nc = NotionCompiler()
    with pytest.raises(expected_exception=CompileError, match='Invalid compiler state: ') as exc_info:
        bp = BindParameter(key='fake')
        bp.role = _BindRole.NO_BINDROLE
        nc._compiler_state.compile_state = _CompileState.NOT_STARTED
        nc._compiler_state.stmt = insert(students)
        nc._add_bindparam(bp)

def test_bindparam_require_column_name_error(students: Table):
    nc = NotionCompiler()
    with pytest.raises(expected_exception=CompileError, match='insert/update require a column name') as exc_info:
        bp = BindParameter(key='fake')
        bp.role = _BindRole.NO_BINDROLE
        nc._compiler_state.compile_state = _CompileState.COMPILING_VALUES
        nc._compiler_state.stmt = insert(students)
        nc._add_bindparam(bp)

def test_bindparam_column_name_required_error(students: Table):
    nc = NotionCompiler()
    with pytest.raises(expected_exception=CompileError, match='in a where clause shall not have a column name') as exc_info:
        bp = BindParameter(key='fake')
        bp.role = _BindRole.NO_BINDROLE
        nc._compiler_state.compile_state = _CompileState.COMPILING_WHERE
        nc._compiler_state.stmt = insert(students)
        nc._add_bindparam(bp, column_name='another_fake')

def test_bindparam_column_name_not_found_error(students: Table):
    nc = NotionCompiler()
    with pytest.raises(expected_exception=CompileError, match='Column name: another_fake') as exc_info:
        bp = BindParameter(key='fake')
        bp.role = _BindRole.NO_BINDROLE
        nc._compiler_state.compile_state = _CompileState.COMPILING_VALUES
        nc._compiler_state.stmt = insert(students)
        nc._add_bindparam(bp, column_name='another_fake')



