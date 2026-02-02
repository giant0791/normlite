import pytest

from normlite.engine.base import Engine, create_engine
from normlite.notion_sdk.getters import get_property
from normlite.sql.ddl import CreateTable
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

def test_compile_create_table_parent_id(students: Table, engine: Engine):
    students._db_parent_id = engine._user_tables_page_id
    stmt = CreateTable(students)
    compiled = stmt.compile(engine._sql_compiler)

    assert 'database_id' in compiled.params

    db_id_param: BindParameter = compiled.params['database_id']
    assert db_id_param.type_ is None
    assert db_id_param.role == _BindRole.DBAPI_PARAM
    assert db_id_param.effective_value == students._db_parent_id

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


