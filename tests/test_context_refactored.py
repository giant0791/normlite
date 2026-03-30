from datetime import date
import pdb
import uuid
import pytest

from normlite.engine.base import Engine, create_engine
from normlite.engine.context import ExecutionContext, ExecutionStyle
from normlite.engine.interfaces import _distill_params
from normlite.exceptions import ArgumentError, CompileError, ResourceClosedError
from normlite.sql.reflection import ReflectedTableInfo
from normlite.notion_sdk.getters import get_object_id
from normlite.sql.ddl import CreateTable, DropTable
from normlite.sql.dml import insert, select, delete
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, String


# =========================================================
# Fixtures
# =========================================================

@pytest.fixture
def engine() -> Engine:
    return create_engine(
        'normlite:///:memory:',
        _mock_ws_id='12345678-0000-0000-1111-123456789012',
        _mock_ischema_page_id='abababab-3333-3333-3333-abcdefghilmn',
        _mock_tables_id='66666666-6666-6666-6666-666666666666',
        _mock_db_page_id='12345678-9090-0606-1111-123456789012'
    )


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
        Column('grade', String())
    )


@pytest.fixture
def insert_values():
    return dict(
        name='Galileo Galilei',
        id=123456,
        is_active=False,
        start_on=date(1690, 1, 1),
        grade='A'
    )


# =========================================================
# Test helpers (CORE REFACTOR)
# =========================================================

def create_students_db(engine: Engine) -> str:
    db = engine._client._add('database', {
        'parent': {'type': 'page_id', 'page_id': engine._user_tables_page_id},
        "title": [{"text": {"content": "students"}}],
        'properties': {
            'name': {'title': {}},
            'id': {'number': {}},
            'is_active': {'checkbox': {}},
            'start_on': {'date': {}},
            'grade': {'rich_text': {}},
        }
    })

    engine._client._add('page', {
        'parent': {'type': 'database_id', 'database_id': engine._tables_id},
        'properties': {
            'table_name': {'title': [{'text': {'content': 'students'}}]},
            'table_schema': {'rich_text': [{'text': {'content': ''}}]},
            'table_catalog': {'rich_text': [{'text': {'content': 'memory'}}]},
            'table_id': {'rich_text': [{'text': {'content': db.get('id')}}]}
        }
    })

    return db.get('id')


def add_students_rows(engine: Engine, students: Table):
    db_id = students.get_oid()

    for name, sid in [("Galileo Galilei", 1500), ("Isaac Newton", 1600)]:
        engine._client.pages_create(
            payload={
                'parent': {'type': 'database_id', 'database_id': db_id},
                'properties': {
                    'name': {'title': [{'text': {'content': name}}]},
                    'id': {'number': sid},
                    'is_active': {'checkbox': False},
                    'start_on': {'date': {'start': '1600-01-01'}},
                    'grade': {'rich_text': [{'text': {'content': 'A'}}]},
                }
            }
        )


@pytest.fixture
def students_db(engine, students):
    db_id = create_students_db(engine)
    students._sys_columns["object_id"]._value = db_id
    return db_id


@pytest.fixture
def populated_students(engine, students, students_db):
    add_students_rows(engine, students)
    return students


# =========================================================
# Execution harness
# =========================================================

def run_context(engine, stmt, params=None, execution_options=None):
    compiled = stmt.compile(engine._sql_compiler)
    cursor = engine.raw_connection().cursor()

    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=cursor,
        compiled=compiled,
        distilled_params=_distill_params(params),
        execution_options=execution_options or {},
    )

    ctx.pre_exec()
    ctx.invoked_stmt._setup_execution(ctx)

    if ctx.execution_style == ExecutionStyle.EXECUTE:
        engine.do_execute(cursor, ctx.operation, ctx.parameters)
    else:
        engine.do_executemany(cursor, ctx.bulk_operation, ctx.bulk_parameters)

    ctx.post_exec()
    ctx.invoked_stmt._finalize_execution(ctx)

    return ctx.setup_cursor_result(), ctx


def run_execute(engine, stmt, params=None, execution_options=None):
    with engine.connect() as conn:
        return conn.execute(stmt, params, execution_options=execution_options)


def is_valid_uuid4(value: str) -> bool:
    try:
        uuid.UUID(value, version=4)
        return True
    except Exception:
        return False


# =========================================================
# Unit tests (pure logic)
# =========================================================

def test_distill_params():
    assert _distill_params() == [{}]
    assert _distill_params({'a': 1}) == [{'a': 1}]
    assert _distill_params([]) == []

    with pytest.raises(TypeError):
        _distill_params(123)

    with pytest.raises(TypeError):
        _distill_params([{'a': 1}, 123])


def test_unused_bind_params_raises(engine, students):
    stmt = insert(students).values(name="A")

    with pytest.raises(CompileError):
        run_context(engine, stmt, params={"unknown": 123})


def test_execution_style_delete(engine, populated_students, students):
    stmt = delete(students)
    _, ctx = run_context(engine, stmt)

    assert ctx.execution_style == ExecutionStyle.EXECUTEMANY

def test_insert_missing_values_raises(engine, students, students_db):
    stmt = insert(students)

    with pytest.raises(ArgumentError) as exc:
        run_context(engine, stmt, params={"name": "Alice"})

    msg = str(exc.value)
    assert "name" not in msg
    assert "id" in msg
    assert "is_active" in msg
    assert "start_on" in msg
    assert "grade" in msg

def test_execution_options_precedence(engine, students, students_db):
    stmt = insert(students).execution_options(preserve_rowcount=False)

    _, ctx = run_context(
        engine,
        stmt,
        params={"name": "Alice", "id": 123456, "is_active": True, "start_on": date(1999,1,1), "grade": "B"},
        execution_options={"preserve_rowcount": True},
    )

    assert ctx.execution_options["preserve_rowcount"] is True


def test_result_idempotent(engine, students, students_db):
    result, ctx = run_context(
        engine, 
        insert(students), 
        params={
            "name": "Alice", 
            "id": 123456, 
            "is_active": True, 
            "start_on": date(1999,1,1), 
            "grade": "B"
        }
    )

    assert ctx.setup_cursor_result() is ctx.setup_cursor_result()


# =========================================================
# Pipeline tests (single source of truth)
# =========================================================

@pytest.mark.parametrize("preserve_rowcount,expected", [
    (True, 1),
    (False, -1)
])
def test_insert_rowcount(engine, students, students_db, insert_values, preserve_rowcount, expected):
    result = run_execute(
        engine,
        insert(students),
        insert_values,
        execution_options={"preserve_rowcount": preserve_rowcount},
    )

    assert result.rowcount == expected


def test_select_projection(engine, populated_students, students):
    stmt = select(students.c.is_active)

    result = run_execute(engine, stmt)
    rows = result.all()

    assert len(rows) == 2
    assert "is_active" in rows[0].mapping()
    assert "name" not in rows[0].mapping()


@pytest.mark.parametrize("preserve_rowcount,expected", [
    (True, 2),
    (False, -1)
])
def test_select_rowcount(engine, populated_students, students, preserve_rowcount, expected):
    stmt = select(students)

    result = run_execute(
        engine,
        stmt,
        execution_options={"preserve_rowcount": preserve_rowcount},
    )

    assert result.rowcount == expected


# =========================================================
# DDL tests (merged)
# =========================================================

def test_create_table(engine, students):
    students._db_parent_id = engine._user_tables_page_id

    result = run_execute(engine, CreateTable(students))

    assert not result.returns_rows
    assert result.rowcount == -1
    assert is_valid_uuid4(students.get_oid())
    assert all(c._id for c in students.user_columns)


def test_drop_table(engine, students, students_db):
    inspector = engine.inspect()
    result, ctx = run_context(engine, DropTable(students))

    with pytest.raises(ResourceClosedError):
        rows = result.all()

    assert inspector.is_dropped(students)
    assert inspector.is_dropped(students.name)
    assert not result.returns_rows


# =========================================================
# Integration sanity test (pipeline correctness)
# =========================================================

def test_full_pipeline_create_table(engine, students):
    students._db_parent_id = engine._user_tables_page_id

    result = run_execute(engine, CreateTable(students))

    assert not result.returns_rows
    assert is_valid_uuid4(students.get_oid())