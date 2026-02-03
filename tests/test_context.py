from datetime import date, datetime
import pdb
import uuid
import pytest

from normlite._constants import SpecialColumns
from normlite.engine.base import Engine, create_engine
from normlite.engine.context import ExecutionContext, ExecutionStyle
from normlite.engine.interfaces import _distill_params
from normlite.engine.reflection import ReflectedTableInfo
from normlite.notion_sdk.getters import get_object_id, rich_text_to_plain_text
from normlite.sql.compiler import NotionCompiler
from normlite.sql.ddl import CreateTable
from normlite.sql.dml import insert, select
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, String

@pytest.fixture
def mocked_db_id() -> str:
    return str(uuid.uuid4())

@pytest.fixture
def metadata() -> MetaData:
    return MetaData()

@pytest.fixture
def students(metadata: MetaData, mocked_db_id: str) -> Table:
    students = Table(
        'students',
        metadata,
        Column('name', String(is_title=True)),
        Column('id', Integer()),
        Column('is_active', Boolean()),
        Column('start_on', Date()),
        Column('grade',  String())
    )
    students.set_oid(mocked_db_id)      # ensure table is in reflected state
    return students

@pytest.fixture
def insert_values() -> dict:
    return dict(
        name = 'Galileo Galilei',
        id=123456,
        is_active=False,
        start_on=date(1690,1,1),
        grade='A'
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

def create_students_db(engine: Engine) -> str:
    # create a new table students under the user tables page
    db = engine._client._add('database', {
        'parent': {
            'type': 'page_id',
            'page_id': engine._user_tables_page_id
        },
        "title": [
            {
                "type": "text",
                "text": {
                    "content": "students",
                    "link": None
                },
                "plain_text": "students",
                "href": None
            }
        ],
        'properties': {
            'name': {'title': {}},
            'id': {'number': {}},
            'is_active': {'checkbox': {}},
            'start_on': {'date': {}},
            'grade': {'rich_text': {}},
        }
    })

    # add the students to tables
    engine._client._add('page', {
        'parent': {
            'type': 'database_id',
            'database_id': engine._tables_id
        },
        'properties': {
            'table_name': {'title': [{'text': {'content': 'students'}}]},
            'table_schema': {'rich_text': [{'text': {'content': ''}}]},
            'table_catalog': {'rich_text': [{'text': {'content': 'memory'}}]},
            'table_id': {'rich_text': [{'text': {'content': db.get('id')}}]}
        }
    })

    return db.get('id')

def add_students_rows(engine: Engine, students: Table) -> list[str]:
    inserted_ids = []
    db_id = students.get_oid()
    page = engine._client.pages_create(
        payload={
            'parent': {
                'type': 'database_id',
                'database_id': db_id
            },
            'properties': {
                'name': {'title': [{'text': {'content': 'Galileo Galilei'}}]},
                'id': {'number': 1500},
                'is_active': {'checkbox': False},
                'start_on': {'date': {'start': '1581-01-01'}},
                'grade': {'rich_text': [{'text': {'content': 'A'}}]},
            }
        }
    )
    inserted_ids.append(get_object_id(page))

    page = engine._client.pages_create(
        payload={
            'parent': {
                'type': 'database_id',
                'database_id': db_id
            },
            'properties': {
                'name': {'title': [{'text': {'content': 'Isaac Newton'}}]},
                'id': {'number': 1600},
                'is_active': {'checkbox': False},
                'start_on': {'date': {'start': '1661-01-01'}},
                'grade': {'rich_text': [{'text': {'content': 'A'}}]},
            }
        }
    )
    inserted_ids.append(get_object_id(page))

    return inserted_ids


#--------------------------------------------------
# Params distillation tests
#--------------------------------------------------
def test_none_distilled_as_seq_w_empty_dict():
    distilled_params = _distill_params()
    assert distilled_params == [{}]

def test_mapping_distilled_as_seq_w_one_dict():
    params = {'name': 'Galileo Galilei', 'id': 123456}
    distilled_params = _distill_params(params)
    assert len(distilled_params) == 1
    assert distilled_params[0] == params

def test_empty_seq_distilled_as_empty_seq():
    params = []
    distilled_params = _distill_params(params)
    assert distilled_params == []

def test_mappings_seq_distilled_as_mappings_seq():
    params = [
        {'name': 'Galileo Galilei', 'id': 123456},
        {'name': 'Isaac Newton', 'id':123457},
    ]
    distilled_params = _distill_params(params)
    assert distilled_params == params

def test_params_nor_mapping_nor_seq():
    params = 12345
    with pytest.raises(TypeError, match='must be a mapping or a sequence of mappings'):
        distilled_params = _distill_params(params)
    
def test_not_all_mappings():
    params = [{'name': 'Gianmarco'}, 12345, ]
    with pytest.raises(TypeError, match='multi-execute parameter sequence must be a mapping'):
        distilled_params = _distill_params(params)

#---------------------------------------------------
# Resolve parameters tests
#---------------------------------------------------
def test_resolve_params_for_insert(engine: Engine, students: Table, insert_values: dict):
    insert_stmt = insert(students).values(
        **insert_values
    )

    compiled = insert_stmt.compile(NotionCompiler())
    distilled_params = [{"id": 6789012}]
    ctx = ExecutionContext(
        engine, 
        engine.connect(), 
        engine.raw_connection().cursor(), 
        compiled, 
        distilled_params
    )
    ctx.pre_exec()

    assert ctx.execution_style == ExecutionStyle.SINGLE
    payload = ctx.payload[0]
    assert payload['parent']['database_id'] == students.get_oid()
    assert payload['properties']['id']['number'] == 6789012

def test_resolve_params_for_select(engine: Engine, students: Table):
    select_stmt = select(students.c.id, students.c.is_active).where(students.c.name == 'Galileo Galilei')
    compiled = select_stmt.compile(NotionCompiler())
    ctx = ExecutionContext(
        engine, 
        engine.connect(), 
        engine.raw_connection().cursor(), 
        compiled, 
    )
    ctx.pre_exec()

    assert ctx.execution_style == ExecutionStyle.SINGLE
    assert ctx.path_params['database_id'] == students.get_oid()
    assert ctx.query_params.get('filter_properties') is not None
    assert 'id' in ctx.query_params['filter_properties']
    name_col_val = ctx.payload[0]['filter']['title']['equals']
    assert name_col_val == 'Galileo Galilei'

def test_resolve_params_for_create_table(students: Table, engine: Engine):
    students._db_parent_id = engine._user_tables_page_id
    stmt = CreateTable(students)
    compiled = stmt.compile(engine._sql_compiler)
    ctx = ExecutionContext(
        engine, 
        engine.connect(), 
        engine.raw_connection().cursor(), 
        compiled, 
    )
    ctx.pre_exec()

    assert ctx.execution_style == ExecutionStyle.SINGLE

    payload = ctx.payload[0]
    assert payload['parent']['page_id'] == engine._user_tables_page_id
    assert rich_text_to_plain_text(payload['title']) == 'students'

#---------------------------------------------------
# Execute context tests
#---------------------------------------------------
def test_execute_dml_context_returning(engine: Engine, students: Table, insert_values: dict):
    # create database and mock reflection
    database_id = create_students_db(engine)
    students.set_oid(database_id)
    insert_stmt = insert(students).returning(students.c.name, students.c.id)

    # Connection.execute(insert_stmt, insert_values)
    distilled_params = _distill_params(insert_values)

    # stmt._execute_on_connection(connection, distilled_params, execution_options)
    compiled = insert_stmt.compile(engine._sql_compiler)
    cursor = engine.raw_connection().cursor()
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=cursor,
        compiled=compiled,
        distilled_params=distilled_params
    )

    # Connection._execute_context(context) -> CursorResult:
    ctx.pre_exec()
    engine.do_execute(cursor, ctx.operation, ctx.parameters)
    ctx.post_exec()
    result = ctx.setup_cursor_result()
    row = result.one()
    mapping = row.mapping()

    assert students.c.name.name in mapping
    assert students.c.id.name in mapping
    assert not students.c.is_active.name in mapping

def test_execute_dml_context_projection(engine: Engine, students: Table):
    # create database and mock reflection
    database_id = create_students_db(engine)
    students.set_oid(database_id)

    # add rows
    inserted_ids = add_students_rows(engine, students)

    # construct select with projection
    select_stmt = (
        select(students.c.is_active)
        .where(students.c.start_on.after(date(1580,1,1)))
    )

    # stmt._execute_on_connection(connection, distilled_params, execution_options)
    compiled = select_stmt.compile(engine._sql_compiler)
    cursor = engine.raw_connection().cursor()
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=cursor,
        compiled=compiled,
    )

    # Connection._execute_context(context) -> CursorResult:
    ctx.pre_exec()
    engine.do_execute(cursor, ctx.operation, ctx.parameters)
    ctx.post_exec()
    result = ctx.setup_cursor_result()
    rows = result.all()
    row_0_mapping = rows[0].mapping()
    row_1_mapping = rows[1].mapping()
    
    assert len(rows) == 2
    assert students.c.is_active.name in row_0_mapping
    assert 'name' not in row_0_mapping
    assert rows[0]['is_active'] == False
    assert 'is_active' in row_1_mapping
    assert 'start_on' not in row_1_mapping
    assert rows[1]['is_active'] == False

def is_valid_uuid4(uuid_string: str) -> bool:
    try:
        # The version=4 argument checks if the format matches v4 specifically
        val = uuid.UUID(uuid_string, version=4)
    except (ValueError, AttributeError, TypeError):
        # ValueError: badly formed hex string
        # AttributeError/TypeError: input isn't a string-like object
        return False
    
    return True

def test_execute_ddl_context_create_table_returning_id_and_title(engine: Engine, students: Table):
    students._db_parent_id = engine._user_tables_page_id
    stmt = CreateTable(students)
    compiled = stmt.compile(engine._sql_compiler)
    cursor = engine.raw_connection().cursor()
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=cursor,
        compiled=compiled,
    )

    # Connection._execute_context(context) -> CursorResult:
    ctx.pre_exec()
    engine.do_execute(cursor, ctx.operation, ctx.parameters)
    ctx.post_exec()
    result = ctx.setup_cursor_result()
    rows = result.all()
    reflected_table_info = ReflectedTableInfo.from_rows(rows)
    
    assert reflected_table_info.name == 'students'
    assert is_valid_uuid4(reflected_table_info.id)

def est_execute_ddl_context_create_table_returning_property_ids(engine: Engine, students: Table):
    students._db_parent_id = engine._user_tables_page_id
    stmt = CreateTable(students)
    compiled = stmt.compile(engine._sql_compiler)
    cursor = engine.raw_connection().cursor()
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=cursor,
        compiled=compiled,
    )

    # Connection._execute_context(context) -> CursorResult:
    ctx.pre_exec()
    engine.do_execute(cursor, ctx.operation, ctx.parameters)
    ctx.post_exec()
    result = ctx.setup_cursor_result()
    rows = result.all()
    reflected_table_info = ReflectedTableInfo.from_rows(rows)
    name_col, id_col, is_active_col, start_on_col, grade_col = reflected_table_info.get_user_columns()

    assert name_col.type == 'title'
    assert id_col.type == 'number'
    assert is_active_col.type == 'checkbox'
    assert start_on_col.type == 'date'
    assert grade_col.type == 'rich_text'

def test_connection_exec_pipeline_simulated(engine: Engine, students: Table):
    students._db_parent_id = engine._user_tables_page_id
    stmt = CreateTable(students)

    # Connection._execute_context(context) -> CursorResult:
    compiled = stmt.compile(engine._sql_compiler)
    cursor = engine.raw_connection().cursor()
    distilled_params = _distill_params()
    context = ExecutionContext(
        engine,
        engine.connect(),
        cursor=cursor,
        compiled=compiled,
        distilled_params=distilled_params,
    )
    
    elem = context.invoked_stmt

    context.pre_exec()                  # normalize params, options, payload
    elem._setup_execution(context)      # statement prepares execution
    engine.do_execute(                  # side effects happen (HTTP)
        cursor, 
        context.operation, 
        context.parameters
    )

    context.post_exec()                 # The execution is mechanically complete before semantic interpretation begins
    elem._finalize_execution(context)   # semantic reconstruction / reflection
    
    result = context.setup_cursor_result()

    assert not result.returns_rows
    assert is_valid_uuid4(students.get_oid())

    name_col, id_col, is_active_col, start_on_col, grade_col = students.get_user_defined_colums()
    assert name_col._id 
    assert id_col._id
    assert is_active_col._id
    assert start_on_col._id
    assert grade_col._id

#---------------------------------------------
# Execution pipeline tests
#---------------------------------------------
def test_connection_exec_dml_context_returning(engine: Engine, students: Table, insert_values: dict):
    # create database and mock reflection
    database_id = create_students_db(engine)
    students.set_oid(database_id)
    insert_stmt = insert(students).returning(students.c.name, students.c.id)

    with engine.connect() as connection:
        execution_options = {
            "isolation_level": "AUTOCOMMIT"
        }
        result = connection.execute(
            insert_stmt, 
            insert_values, 
            execution_options=execution_options
        )

    row = result.one()
    mapping = row.mapping()

    assert students.c.name.name in mapping
    assert students.c.id.name in mapping
    assert not students.c.is_active.name in mapping

def test_connection_exec_dml_context_projection(engine: Engine, students: Table):
    # create database and mock reflection
    database_id = create_students_db(engine)
    students.set_oid(database_id)

    # add rows
    inserted_ids = add_students_rows(engine, students)

    # construct select with projection
    select_stmt = (
        select(students.c.is_active)
        .where(students.c.start_on.after(date(1580,1,1)))
    )

    with engine.connect() as connection:
        execution_options = {
            "isolation_level": "refetch"
        }
        result = connection.execute(
            select_stmt, 
            execution_options=execution_options
        )

    rows = result.all()
    row_0_mapping = rows[0].mapping()
    row_1_mapping = rows[1].mapping()
    
    assert len(rows) == 2
    assert students.c.is_active.name in row_0_mapping
    assert 'name' not in row_0_mapping
    assert rows[0]['is_active'] == False
    assert 'is_active' in row_1_mapping
    assert 'start_on' not in row_1_mapping
    assert rows[1]['is_active'] == False

def test_connection_exec_ddl_context_create_table(engine: Engine, students: Table):
    students._db_parent_id = engine._user_tables_page_id
    stmt = CreateTable(students)

    with engine.connect() as connection:
        execution_options = {
            "isolation_level": "AUTO COMMIT"
        }

        result = connection.execute(
            stmt, 
            execution_options=execution_options
        )
    
    assert not result.returns_rows
    assert is_valid_uuid4(students.get_oid())
    assert all(
        [c._id is not None for c in students.get_user_defined_colums()]
    )