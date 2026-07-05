from datetime import date
import uuid
import pytest

from normlite.exceptions import CompileError
from normlite.sql.base import _CompileState, CompilerState
from normlite.sql.compiler import NotionCompiler
from normlite.sql.dml import ValuesBase, insert
from normlite.sql.elements import _BindRole, _NoArg, BindParameter
from normlite.sql.schema import Table


#---------------------------------------------
# Generative values()
#---------------------------------------------
def test_values_is_generative(students: Table):
    stmt = ValuesBase(students)
    stmt = stmt.values(name='Galileo Galilei').values(is_active=True)
    
    assert len(stmt._values) == len(stmt._single_parameters)
    assert stmt._values['name'].value == _NoArg.NO_ARG              # _values is the template
    assert stmt._single_parameters["name"] == "Galileo Galilei"     #  _single_parameters is the actual data
    assert stmt._values['is_active'].value == _NoArg.NO_ARG
    assert stmt._single_parameters["is_active"]

#---------------------------------------------
# Compilation tests
#---------------------------------------------

# superseded by test_compiler_parents_insert_to_data_source_id
# (2025-09-03: page parent is data_source_id, not database_id)

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

# superseded by test_compiler_parents_insert_to_data_source_id
# (2025-09-03: page parent is data_source_id, not database_id)

def test_compiler_generates_values_as_bindparams(students: Table, insert_values: dict):
    """Does the compiler generates bind parameters for the Insert.values()?"""
    nc = NotionCompiler()
    mocked_db_id = str(uuid.uuid4())
    students._sys_columns["object_id"]._value = mocked_db_id
    students._sys_columns["data_source_id"]._value = str(uuid.uuid4())
    stmt = insert(students).values(**insert_values)

    compiled = stmt.compile(nc)

    assert nc._compiler_state.is_insert
    assert compiled.params["name"] == "Galileo Galilei"
    assert compiled._execution_binds['name'].role == _BindRole.COLUMN_VALUE
    assert compiled.params['id'] == 123456
    assert compiled._execution_binds['id'].role == _BindRole.COLUMN_VALUE
    assert not compiled.params['is_active'] 
    assert compiled._execution_binds['is_active'].role == _BindRole.COLUMN_VALUE
    assert compiled.params['start_on'] == date(1690,1,1)
    assert compiled._execution_binds['start_on'].role == _BindRole.COLUMN_VALUE
    assert compiled.params['grade'] == 'A'
    assert compiled._execution_binds['grade'].role == _BindRole.COLUMN_VALUE

def test_compiler_detects_missing_values(students: Table, insert_values: dict):        
    nc = NotionCompiler()
    mocked_db_id = str(uuid.uuid4())
    students._sys_columns["object_id"]._value = mocked_db_id
    stmt = insert(students)
    stmt.values(**insert_values)

    with pytest.raises(CompileError, match="'name' not supplied in INSERT statement"):
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
    nc._compiler_state = CompilerState()
    with pytest.raises(expected_exception=CompileError, match='Invalid compiler state: ') as exc_info:
        bp = BindParameter(key='fake')
        bp.role = _BindRole.NO_BINDROLE
        nc._compiler_state.compile_state = _CompileState.NOT_STARTED
        nc._compiler_state.stmt = insert(students)
        nc._add_bindparam(bp)

def test_bindparam_require_column_name_error(students: Table):
    nc = NotionCompiler()
    nc._compiler_state = CompilerState()
    with pytest.raises(expected_exception=CompileError, match='insert/update require a column name') as exc_info:
        bp = BindParameter(key='fake')
        bp.role = _BindRole.NO_BINDROLE
        nc._compiler_state.compile_state = _CompileState.COMPILING_VALUES
        nc._compiler_state.stmt = insert(students)
        nc._add_bindparam(bp)

def test_bindparam_column_name_required_error(students: Table):
    nc = NotionCompiler()
    nc._compiler_state = CompilerState()
    with pytest.raises(expected_exception=CompileError, match='in a where clause shall not have a column name') as exc_info:
        bp = BindParameter(key='fake')
        bp.role = _BindRole.NO_BINDROLE
        nc._compiler_state.compile_state = _CompileState.COMPILING_WHERE
        nc._compiler_state.stmt = insert(students)
        nc._add_bindparam(bp, column_name='another_fake')

def test_bindparam_column_name_not_found_error(students: Table):
    nc = NotionCompiler()
    nc._compiler_state = CompilerState()
    with pytest.raises(expected_exception=CompileError, match='Column name: another_fake') as exc_info:
        bp = BindParameter(key='fake')
        bp.role = _BindRole.NO_BINDROLE
        nc._compiler_state.compile_state = _CompileState.COMPILING_VALUES
        nc._compiler_state.stmt = insert(students)
        nc._add_bindparam(bp, column_name='another_fake')


def test_compiler_parents_insert_to_data_source_id(students: Table):
    mocked_db_id = str(uuid.uuid4())
    mocked_ds_id = str(uuid.uuid4())
    students._sys_columns["object_id"]._value = mocked_db_id
    students._sys_columns["data_source_id"]._value = mocked_ds_id
    stmt = insert(students).values(
        name='Galileo Galilei',
        id=123456,
        is_active=False,
        start_on=date(1690, 1, 1),
        grade='A',
    )

    compiled = stmt.compile(NotionCompiler())
    parent = compiled.as_dict()['payload']['parent']

    # A page's parent is its data source, not the database container.
    assert parent['type'] == 'data_source_id'
    assert parent['data_source_id'] == ':data_source_id'
    assert 'data_source_id' in compiled._execution_binds
    assert compiled._execution_binds['data_source_id'].value == mocked_ds_id
    assert compiled._execution_binds['data_source_id'].role == _BindRole.DBAPI_PARAM
