import uuid
import pytest

from normlite.exceptions import CompileError
from normlite.sql.compiler import NotionCompiler
from normlite.sql.dml import Update, update
from normlite.sql.elements import BooleanClauseList
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, String


@pytest.fixture
def metadata():
    return MetaData()


@pytest.fixture
def students(metadata):
    return Table(
        'students',
        metadata,
        Column('name', String(is_title=True)),
        Column('id', Integer()),
        Column('is_active', Boolean()),
        Column('start_on', Date()),
        Column('grade', String()),
    )


@pytest.fixture
def db_id(students):
    oid = str(uuid.uuid4())
    students._sys_columns["object_id"]._value = oid
    return oid


def compile_update(stmt):
    nc = NotionCompiler()
    compiled = stmt.compile(nc)
    return compiled, compiled.as_dict()


# ── helper ────────────────────────────────────────────────────────────────────

ALL_COLUMNS = {'name', 'id', 'is_active', 'start_on', 'grade'}

# ── 1 & 2. Tracer bullet + compiled dict shape ────────────────────────────────

def test_update_compiles_basic_values(students, db_id):
    stmt = update(students).values(name='Newton')
    compiled, asdict = compile_update(stmt)
    assert asdict is not None


def test_update_compiled_dict_shape(students, db_id):
    stmt = update(students).values(name='Newton')
    compiled, asdict = compile_update(stmt)

    assert asdict['operation']['endpoint'] == 'databases'
    assert asdict['operation']['request'] == 'query'
    assert 'path_params' in asdict
    assert asdict['path_params']['database_id'] == ':database_id'
    assert 'payload' in asdict
    assert 'update_payload' in asdict


def test_update_payload_contains_values_template(students, db_id):
    stmt = update(students).values(name='Newton', grade='A')
    compiled, asdict = compile_update(stmt)

    update_payload = asdict['update_payload']
    # update_payload must have a named placeholder for each supplied column
    assert 'name' in update_payload
    assert 'grade' in update_payload
    assert update_payload['name'].startswith(':')
    assert update_payload['grade'].startswith(':')
    # the values params must be registered as execution binds
    name_key = update_payload['name'][1:]
    grade_key = update_payload['grade'][1:]
    assert name_key in compiled._execution_binds
    assert grade_key in compiled._execution_binds
    # filter payload must NOT contain the values params
    assert 'name' not in asdict['payload']
    assert 'grade' not in asdict['payload']


# ── 4. Partial VALUES ─────────────────────────────────────────────────────────

def test_update_partial_values(students, db_id):
    # only one column — must compile fine and not include the other columns
    stmt = update(students).values(name='Newton')
    compiled, asdict = compile_update(stmt)

    update_payload = asdict['update_payload']
    assert set(update_payload.keys()) == {'name'}
    omitted = ALL_COLUMNS - {'name'}
    for col in omitted:
        assert col not in update_payload


# ── 5 & 6. WHERE clause ───────────────────────────────────────────────────────

def test_update_no_where_no_filter(students, db_id):
    stmt = update(students).values(name='Newton')
    compiled, asdict = compile_update(stmt)

    assert 'filter' not in asdict['payload']


def test_update_with_where_filter(students, db_id):
    stmt = update(students).values(name='Newton').where(students.c.is_active == True)
    compiled, asdict = compile_update(stmt)

    assert 'filter' in asdict['payload']
    f = asdict['payload']['filter']
    assert f['property'] == 'is_active'
    assert 'checkbox' in f


# ── 7. Chained WHERE ──────────────────────────────────────────────────────────

def test_update_chained_where(students, db_id):
    stmt = (
        update(students)
        .values(name='Newton')
        .where(students.c.is_active == True)
        .where(students.c.id <= 1000)
    )
    compiled, asdict = compile_update(stmt)

    assert isinstance(stmt._whereclause.expression, BooleanClauseList)
    f = asdict['payload']['filter']
    assert 'and' in f
    assert len(f['and']) == 2
    props = {clause['property'] for clause in f['and']}
    assert props == {'is_active', 'id'}


# ── 8 & 9. Dict syntax + chained .values() ───────────────────────────────────

def test_update_values_dict_syntax(students, db_id):
    stmt = update(students).values({'name': 'Newton', 'grade': 'A'})
    compiled, asdict = compile_update(stmt)

    update_payload = asdict['update_payload']
    assert 'name' in update_payload
    assert 'grade' in update_payload


def test_update_chained_values(students, db_id):
    stmt = update(students).values(name='Newton').values(grade='A')
    compiled, asdict = compile_update(stmt)

    update_payload = asdict['update_payload']
    assert 'name' in update_payload
    assert 'grade' in update_payload


# ── 10. CompileError without .values() ───────────────────────────────────────

def test_update_compile_error_no_values(students, db_id):
    stmt = update(students)
    with pytest.raises(CompileError):
        compile_update(stmt)


# ── 11. RETURNING sets result_columns ────────────────────────────────────────

def test_update_returning_sets_result_columns(students, db_id):
    stmt = (
        update(students)
        .values(name='Newton')
        .returning(students.c.name, students.c.object_id)
    )
    compiled, asdict = compile_update(stmt)

    result_cols = compiled.result_columns()
    assert 'name' in result_cols
    assert 'object_id' in result_cols


# ── 12. Public exports ────────────────────────────────────────────────────────

def test_update_public_exports():
    import normlite
    import normlite.sql

    assert hasattr(normlite, 'update')
    assert hasattr(normlite, 'Update')
    assert hasattr(normlite.sql, 'update')
    assert hasattr(normlite.sql, 'Update')
