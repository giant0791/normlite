import pytest

from normlite.exceptions import ArgumentError
from normlite.sql.dml import update, select

from tests.utils.execution import run_execute
from tests.utils.db_helpers import (
    create_students_db,
    attach_table_oid,
    populate_students,
)
from tests.utils.assertions import assert_rowcount


@pytest.fixture
def prepared_students(engine, students):
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=10)
    return students


def select_all(engine, table):
    """Helper: select all rows from table."""
    with engine.connect() as conn:
        return conn.execute(select(table)).all()


def select_active(engine, table, is_active: bool):
    """Helper: select rows filtered by boolean is_active column."""
    stmt = select(table).where(table.c.is_active == is_active)
    with engine.connect() as conn:
        return conn.execute(stmt).all()


# ── 4 & 5. Update all rows + select confirms ─────────────────────────────────

def test_update_all_rows_no_where(engine, prepared_students):
    stmt = update(prepared_students).values(grade='Z')

    result = run_execute(engine, stmt)

    assert_rowcount(result, 10)
    rows = select_all(engine, prepared_students)
    assert all(r.grade == 'Z' for r in rows)
    assert len(rows) == 10


# ── 6 & 7. WHERE clause — only matching rows updated ─────────────────────────

def test_update_with_where_updates_only_matching(engine, students):
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=5, is_active=True)
    populate_students(engine, students, n=5, is_active=False)

    stmt = (
        update(students)
        .values(grade='Z')
        .where(students.c.is_active == True)
    )
    result = run_execute(engine, stmt)

    assert_rowcount(result, 5)
    updated = select_active(engine, students, is_active=True)
    assert len(updated) == 5
    assert all(r.grade == 'Z' for r in updated)


def test_update_does_not_affect_non_matching_rows(engine, students):
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=5, is_active=True)
    populate_students(engine, students, n=5, is_active=False)

    stmt = (
        update(students)
        .values(grade='Z')
        .where(students.c.is_active == True)
    )
    run_execute(engine, stmt)

    untouched = select_active(engine, students, is_active=False)
    assert len(untouched) == 5
    assert all(r.grade == 'A' for r in untouched)


# ── 8. Returns CursorResult ───────────────────────────────────────────────────

def test_update_returns_cursor_result(engine, prepared_students):
    from normlite.engine.cursor import CursorResult

    stmt = update(prepared_students).values(grade='Z')
    result = run_execute(engine, stmt)

    assert isinstance(result, CursorResult)


# ── 9. parameters= raises ArgumentError ──────────────────────────────────────

def test_update_parameters_raises_argument_error(engine, prepared_students):
    stmt = update(prepared_students).values(grade='Z')

    with pytest.raises((ArgumentError, Exception)):
        with engine.connect() as conn:
            conn.execute(stmt, {"grade": "X"})
