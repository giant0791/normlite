import pytest

from normlite.engine.base import Engine
from normlite.exceptions import CompileError
from normlite.notion_sdk.getters import get_property
from normlite.sql.base import DDLCompiled
from normlite.sql.ddl import CreateTable, DropTable, ReflectTable
from normlite.sql.elements import _BindRole, BindParameter
from normlite.sql.schema import Column, ForeignKey, Table
from normlite.sql.type_api import String


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

    assert 'page_id' in compiled._execution_binds

    db_id_param: BindParameter = compiled._execution_binds['page_id']
    assert db_id_param.type_ is None
    assert db_id_param.role == _BindRole.DBAPI_PARAM
    assert db_id_param.effective_value == students._db_parent_id

def test_compile_create_table_no_database_id_raises(students: Table, engine: Engine):
    stmt = CreateTable(students)

    with pytest.raises(CompileError, match='neither created or reflected.'):
        _ = stmt.compile(engine._sql_compiler)

def test_compile_create_table_no_parent_id_raises(students: Table, engine: Engine):
    stmt = CreateTable(students)

    with pytest.raises(CompileError, match='neither created or reflected.'):
        _ = stmt.compile(engine._sql_compiler)

def test_compile_create_table_title_as_table_name(students: Table, engine: Engine):
    students._db_parent_id = engine._user_tables_page_id
    stmt = CreateTable(students)
    compiled = stmt.compile(engine._sql_compiler)

    assert 'table_name' in compiled._execution_binds

    title_param: BindParameter = compiled._execution_binds['table_name']
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

# superseded by test_compile_create_table_columns_under_initial_data_source
# (2025-09-03: column schema moved from flat payload['properties'] onto
# payload['initial_data_source']['properties'])

#---------------------------------------------
# DROP TABLE tests
#---------------------------------------------

def test_compile_drop_table_is_ddl(students: Table, engine: Engine):
    students._sys_columns["object_id"]._value = "12345678-9090-0606-1111-123456789012"
    stmt = DropTable(students)
    compiled = stmt.compile(engine._sql_compiler)

    assert compiled.is_ddl
    assert isinstance(compiled, DDLCompiled)

def test_compile_drop_table_database_id(students: Table, engine: Engine):
    students._sys_columns["object_id"]._value = "12345678-9090-0606-1111-123456789012"
    stmt = DropTable(students)
    compiled = stmt.compile(engine._sql_compiler)
    as_dict = compiled.as_dict()
    path_params = as_dict['path_params']

    assert 'database_id' in compiled._execution_binds

    db_id_param: BindParameter = compiled._execution_binds['database_id']
    assert db_id_param.type_ is None
    assert db_id_param.role == _BindRole.DBAPI_PARAM
    assert db_id_param.effective_value == students.get_oid()
    assert path_params['database_id'] == ':database_id'

def test_compile_drop_table_no_database_id_raises(students: Table, engine: Engine):
    stmt = DropTable(students)

    with pytest.raises(CompileError, match='neither created or reflected.'):
        _ = stmt.compile(engine._sql_compiler)
        
def test_compile_drop_table_operation(students: Table, engine: Engine):
    students._sys_columns["object_id"]._value = "12345678-9090-0606-1111-123456789012"
    stmt = DropTable(students)
    compiled = stmt.compile(engine._sql_compiler)
    as_dict = compiled.as_dict()

    assert as_dict['operation']['endpoint'] == 'databases'
    assert as_dict['operation']['request'] == 'update'

def test_compile_drop_table_payload(students: Table, engine: Engine):
    students._sys_columns["object_id"]._value = "12345678-9090-0606-1111-123456789012"
    stmt = DropTable(students)
    compiled = stmt.compile(engine._sql_compiler)
    as_dict = compiled.as_dict()
    payload = as_dict['payload']
    
    assert 'in_trash' in compiled._execution_binds

    in_trash_param: BindParameter = compiled._execution_binds['in_trash']
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

def test_compile_reflect_table_data_source_id(students: Table, engine: Engine):
    # Catalog-first reflection (2025-09-03): a table's schema lives on its data
    # source, so REFLECT retrieves the DATA SOURCE, not the database container.
    # Distinct ids so the assertion pins routing onto data_source_id, not object_id.
    students._sys_columns["object_id"]._value = "db-students-0000-0000-000000000001"
    students._sys_columns["data_source_id"]._value = "ds-students-0000-0000-000000000002"
    stmt = ReflectTable(students)
    compiled = stmt.compile(engine._sql_compiler)
    as_dict = compiled.as_dict()
    path_params = as_dict['path_params']

    assert 'data_source_id' in compiled._execution_binds

    ds_id_param: BindParameter = compiled._execution_binds['data_source_id']
    assert ds_id_param.type_ is None
    assert ds_id_param.role == _BindRole.DBAPI_PARAM
    assert ds_id_param.effective_value == students.get_data_source_id()
    assert path_params['data_source_id'] == ':data_source_id'

def test_compile_reflect_table_no_data_source_id_does_not_raise(students: Table, engine: Engine):
    stmt = ReflectTable(students)
    _ = stmt.compile(engine._sql_compiler)

def test_compile_reflect_table_operation(students: Table, engine: Engine):
    stmt = ReflectTable(students)
    compiled = stmt.compile(engine._sql_compiler)
    as_dict = compiled.as_dict()

    assert as_dict['operation']['endpoint'] == 'data_sources'
    assert as_dict['operation']['request'] == 'retrieve'

def test_compile_create_table_columns_under_initial_data_source(students: Table, engine: Engine):
    students._db_parent_id = engine._user_tables_page_id
    stmt = CreateTable(students)
    compiled = stmt.compile(engine._sql_compiler)
    payload = compiled.as_dict()['payload']

    # The container is created with exactly one data source, and the column
    # schema is declared on THAT data source (2025-09-03), not on the database.
    initial_data_source = payload['initial_data_source']

    assert get_property(initial_data_source, 'name') == {'title': {}}
    assert get_property(initial_data_source, 'id') == {'number': {'format': 'number'}}
    assert get_property(initial_data_source, 'is_active') == {'checkbox': {}}
    assert get_property(initial_data_source, 'start_on') == {'date': {}}
    assert get_property(initial_data_source, 'grade') == {'rich_text': {}}

def test_compile_create_table_relation_spec_targets_data_source_id(engine: Engine):
    # A Relation column's DDL spec must target the referenced table's
    # DATA SOURCE (2025-09-03), not its database. The compiler resolves the
    # FK to reftable.get_data_source_id() and emits it under the
    # {"relation": {"data_source_id": ...}} key.
    from normlite import Relation
    from normlite.sql.schema import MetaData

    metadata = MetaData()
    courses = Table("courses", metadata, Column("title", String(is_title=True)))
    # Distinct database id vs data source id so the assertion pins the RIGHT one.
    courses._sys_columns["object_id"]._value = "db-courses-0000-0000-000000000001"
    courses._sys_columns["data_source_id"]._value = "ds-courses-0000-0000-000000000002"

    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )
    students._db_parent_id = engine._user_tables_page_id

    stmt = CreateTable(students)
    compiled = stmt.compile(engine._sql_compiler)
    initial_data_source = compiled.as_dict()['payload']['initial_data_source']

    relation_spec = get_property(initial_data_source, 'enrolled_in')['relation']
    assert relation_spec['data_source_id'] == courses.get_data_source_id()
