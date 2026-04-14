import pdb

import pytest

from normlite.engine.context import ExecutionStyle
from normlite.sql.dml import delete, select

from tests.utils.execution import run_context
from tests.utils.db_helpers import (
    create_students_db,
    attach_table_oid,
    populate_students,
)


# --------------------------------------
# Fixtures
# --------------------------------------

@pytest.fixture
def prepared_students(engine, students):
    """
    Students table prepared for execution-context testing.
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=5)
    return students


# --------------------------------------
# ExecutionContext tests (DELETE)
# --------------------------------------

def test_delete_execution_style_is_executemany(engine, prepared_students):
    stmt = delete(prepared_students).where(
        prepared_students.c.is_active.is_(True)
    )

    _, ctx = run_context(engine, stmt)

    assert ctx.execution_style == ExecutionStyle.EXECUTEMANY


def test_delete_prefetches_rows_into_bulk_parameters(engine, prepared_students):
    stmt = delete(prepared_students).where(
        prepared_students.c.is_active.is_(True)
    )

    _, ctx = run_context(engine, stmt)

    # number of operations equals number of rows to delete
    assert len(ctx.bulk_parameters) == 5


def test_delete_bulk_parameters_structure(engine, prepared_students):
    stmt = delete(prepared_students).where(
        prepared_students.c.is_active.is_(True)
    )

    # fetch expected ids BEFORE running delete
    sel = select(prepared_students.c.object_id).where(
        prepared_students.c.is_active.is_(True)
    )
    with engine.connect() as conn:
        expected_ids = [row.object_id for row in conn.execute(sel).all()]

    _, ctx = run_context(engine, stmt)

    expected = [
        {
            "path_params": {"page_id": oid},
            "payload": {"in_trash": True},
        }
        for oid in expected_ids
    ]

    assert ctx.bulk_parameters == expected


def test_delete_execution_removes_all_rows(engine, prepared_students):
    stmt = delete(prepared_students).where(
        prepared_students.c.is_active.is_(True)
    )

    result, ctx = run_context(engine, stmt)

    # verify side-effect (rows removed)
    sel = select(prepared_students).where(
        prepared_students.c.is_active.is_(True)
    )

    with engine.connect() as conn:
        remaining = conn.execute(sel).all()

    assert remaining == []
    assert result.rowcount == 5


def test_delete_returning_syscols_context(engine, prepared_students):
    stmt = (
        delete(prepared_students)
        .where(prepared_students.c.is_active.is_(True))
        .returning(prepared_students.c.object_id)
    )

    # capture expected ids BEFORE execution
    sel = select(prepared_students.c.object_id).where(
        prepared_students.c.is_active.is_(True)
    )
    with engine.connect() as conn:
        expected_ids = [row.object_id for row in conn.execute(sel).all()]

    result, ctx = run_context(engine, stmt)
    rows = result.all()

    assert [r.object_id for r in rows] == expected_ids
    assert ctx.execution_style == ExecutionStyle.EXECUTEMANY


def test_delete_implicit_returning_true_context(engine, prepared_students):
    stmt = delete(prepared_students).where(
        prepared_students.c.is_active.is_(True)
    )

    # capture expected ids BEFORE execution
    sel = select(prepared_students.c.object_id).where(
        prepared_students.c.is_active.is_(True)
    )
    with engine.connect() as conn:
        expected_ids = [(row.object_id,) for row in conn.execute(sel).all()]

    result, ctx = run_context(
        engine,
        stmt,
        execution_options={"implicit_returning": True},
    )

    assert result.returned_primary_keys_rows == expected_ids
    assert not result.returns_rows


def test_delete_implicit_returning_false_context(engine, prepared_students):
    stmt = delete(prepared_students).where(
        prepared_students.c.is_active.is_(True)
    )

    result, ctx = run_context(engine, stmt)

    assert result.returned_primary_keys_rows is None
    assert not result.returns_rows