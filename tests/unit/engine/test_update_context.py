import pytest

from normlite.engine.context import ExecutionStyle
from normlite.sql.dml import update, select

from tests.utils.execution import run_context
from tests.utils.db_helpers import (
    create_students_db,
    attach_table_oid,
    populate_students,
)


@pytest.fixture
def prepared_students(engine, students):
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=5)
    return students


# ── 1. execution_style ────────────────────────────────────────────────────────

def test_update_execution_style_is_executemany(engine, prepared_students):
    stmt = update(prepared_students).values(name='Newton')

    _, ctx = run_context(engine, stmt)

    assert ctx.execution_style == ExecutionStyle.EXECUTEMANY


# ── 2. resolved_params holds VALUES bind params ───────────────────────────────

def test_update_resolved_params_holds_values_params(engine, prepared_students):
    stmt = update(prepared_students).values(name='Newton', grade='B')

    _, ctx = run_context(engine, stmt)

    assert ctx.resolved_params is not None
    assert 'name' in ctx.resolved_params
    assert 'grade' in ctx.resolved_params


# ── 3. payload has only filter params, not VALUES ─────────────────────────────

def test_update_payload_has_only_filter_after_pre_exec(engine, prepared_students):
    stmt = (
        update(prepared_students)
        .values(name='Newton')
        .where(prepared_students.c.is_active == True)
    )

    _, ctx = run_context(engine, stmt)

    # VALUES param must not appear in the query-filter payload
    assert 'name' not in str(ctx.payload)
    # filter payload must contain the query structure
    assert 'filter' in ctx.payload


# ── debug: bulk_parameters ────────────────────────────────────────────────────

def test_update_bulk_parameters_structure(engine, prepared_students):
    stmt = update(prepared_students).values(grade='Z')

    _, ctx = run_context(engine, stmt)

    assert ctx.bulk_parameters is not None
    assert len(ctx.bulk_parameters) == 5
    first = ctx.bulk_parameters[0]
    assert 'path_params' in first
    assert 'payload' in first
    assert 'properties' in first['payload']
    assert 'grade' in first['payload']['properties']
