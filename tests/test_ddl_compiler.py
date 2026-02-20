import uuid
import pytest

from normlite.engine.base import Engine, create_engine
from normlite.exceptions import CompileError
from normlite.notion_sdk.getters import get_property
from normlite.notiondbapi.dbapi2 import ProgrammingError
from normlite.sql.base import DDLCompiled
from normlite.sql.ddl import CreateTable, DropTable, ReflectTable
from normlite.sql.elements import _BindRole, BindParameter
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, String

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
def engine() -> Engine:
    return create_engine(
        'normlite:///:memory:',
        _mock_ws_id = '12345678-0000-0000-1111-123456789012',
        _mock_ischema_page_id = 'abababab-3333-3333-3333-abcdefghilmn',
        _mock_tables_id = '66666666-6666-6666-6666-666666666666',
        _mock_db_page_id = '12345678-9090-0606-1111-123456789012'
    )

#---------------------------------------------
# CREATE TABLE tests
#---------------------------------------------

def test_compile_create_table_is_ddl(students: Table, engine: Engine):
    students._db_parent_id = engine._user_tables_page_id
    stmt = CreateTable(students)
    compiled = stmt.compile(engine._sql_compiler)

    assert compiled.is_ddl
    assert isinstance(compiled, DDLCompiled)

def test_compile_create_table_parent_id(students: Table, engine: Engine):
    students._db_parent_id = engine._user_tables_page_id
    stmt = CreateTable(students)
    compiled = stmt.compile(engine._sql_compiler)

    assert 'page_id' in compiled.params

    db_id_param: BindParameter = compiled.params['page_id']
    assert db_id_param.type_ is None
    assert db_id_param.role == _BindRole.DBAPI_PARAM
    assert db_id_param.effective_value == students._db_parent_id

def test_compile_create_table_no_database_id_raises(students: Table, engine: Engine):
    stmt = CreateTable(students)

    with pytest.raises(ProgrammingError, match='neither created or reflected.'):
        _ = stmt.compile(engine._sql_compiler)

def test_compile_create_table_title_as_table_name(students: Table, engine: Engine):
    students._db_parent_id = engine._user_tables_page_id
    stmt = CreateTable(students)
    compiled = stmt.compile(engine._sql_compiler)

    assert 'table_name' in compiled.params

    title_param: BindParameter = compiled.params['table_name']
    assert title_param.type_ is None
    assert title_param.role == _BindRole.DBAPI_PARAM
    assert title_param.effective_value == 'students'

def test_compile_create_table_operation(students: Table, engine: Engine):
    students._db_parent_id = engine._user_tables_page_id
    stmt = CreateTable(students)
    compiled = stmt.compile(engine._sql_compiler)
    as_dict = compiled.as_dict()

    assert as_dict['operation']['endpoint'] == 'databases'
    assert as_dict['operation']['request'] == 'create'

def test_compile_create_table_columns_as_properties(students: Table, engine: Engine):
    students._db_parent_id = engine._user_tables_page_id
    stmt = CreateTable(students)
    compiled = stmt.compile(engine._sql_compiler)
    as_dict = compiled.as_dict()
    payload = as_dict['payload']

    assert get_property(payload, 'name') == {'title': {}}
    assert get_property(payload, 'id') == {'number': {'format': 'number'}}
    assert get_property(payload, 'is_active') == {'checkbox': {}}
    assert get_property(payload, 'start_on') == {'date': {}}
    assert get_property(payload, 'grade') == {'rich_text': {}}

#---------------------------------------------
# DROP TABLE tests
#---------------------------------------------

def test_compile_drop_table_is_ddl(students: Table, engine: Engine):
    stmt = DropTable(students)
    compiled = stmt.compile(engine._sql_compiler)

    assert compiled.is_ddl
    assert isinstance(compiled, DDLCompiled)

def test_compile_drop_table_database_id(students: Table, engine: Engine):
    stmt = DropTable(students)
    compiled = stmt.compile(engine._sql_compiler)
    as_dict = compiled.as_dict()
    path_params = as_dict['path_params']

    assert 'database_id' in compiled.params

    db_id_param: BindParameter = compiled.params['database_id']
    assert db_id_param.type_ is None
    assert db_id_param.role == _BindRole.DBAPI_PARAM
    assert db_id_param.effective_value == students._database_id
    assert path_params['database_id'] == ':database_id'

def test_compile_drop_table_no_database_id_does_not_raise(students: Table, engine: Engine):
    stmt = DropTable(students)
    _ = stmt.compile(engine._sql_compiler)
        
def test_compile_drop_table_operation(students: Table, engine: Engine):
    stmt = DropTable(students)
    compiled = stmt.compile(engine._sql_compiler)
    as_dict = compiled.as_dict()

    assert as_dict['operation']['endpoint'] == 'databases'
    assert as_dict['operation']['request'] == 'update'

def test_compile_drop_table_payload(students: Table, engine: Engine):
    stmt = DropTable(students)
    compiled = stmt.compile(engine._sql_compiler)
    as_dict = compiled.as_dict()
    payload = as_dict['payload']
    
    assert 'in_trash' in compiled.params

    in_trash_param: BindParameter = compiled.params['in_trash']
    assert in_trash_param.type_ is None
    assert in_trash_param.role == _BindRole.DBAPI_PARAM
    assert in_trash_param.effective_value == True
    assert payload['in_trash'] == ':in_trash'

#---------------------------------------------
# REFLECT TABLE tests
#---------------------------------------------

def test_compile_reflect_table_is_ddl(students: Table, engine: Engine):
    stmt = ReflectTable(students)
    compiled = stmt.compile(engine._sql_compiler)

    assert compiled.is_ddl
    assert isinstance(compiled, DDLCompiled)

def test_compile_reflect_table_database_id(students: Table, engine: Engine):
    stmt = ReflectTable(students)
    compiled = stmt.compile(engine._sql_compiler)
    as_dict = compiled.as_dict()
    path_params = as_dict['path_params']

    assert 'database_id' in compiled.params

    db_id_param: BindParameter = compiled.params['database_id']
    assert db_id_param.type_ is None
    assert db_id_param.role == _BindRole.DBAPI_PARAM
    assert db_id_param.effective_value == students._database_id
    assert path_params['database_id'] == ':database_id'

def test_compile_reflect_table_no_database_id_does_not_raise(students: Table, engine: Engine):
    stmt = ReflectTable(students)
    _ = stmt.compile(engine._sql_compiler)

def test_compile_reflect_table_operation(students: Table, engine: Engine):
    stmt = ReflectTable(students)
    compiled = stmt.compile(engine._sql_compiler)
    as_dict = compiled.as_dict()

    assert as_dict['operation']['endpoint'] == 'databases'
    assert as_dict['operation']['request'] == 'retrieve'
