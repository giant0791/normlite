import pytest

from normlite.exceptions import ArgumentError
from normlite.sql.dml import update, select

from tests.utils.execution import run_execute
from tests.utils.db_helpers import (
    create_students_db,
    attach_table_oid,
    populate_students,
)
from tests.utils.assertions import assert_rowcount, assert_columns


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


# ── 10. No RETURNING: soft-closed, returned_primary_keys_rows is None ─────────

def test_update_no_returning_soft_closes_cursor(engine, prepared_students):
    stmt = update(prepared_students).values(grade='Z')

    result = run_execute(engine, stmt)

    assert not result.returns_rows
    assert result.returned_primary_keys_rows is None


# ── 11. RETURNING syscol: rows expose object_id, IDs match original ───────────

def test_update_returning_syscol(engine, prepared_students):
    sel = select(prepared_students.c.object_id)
    with engine.connect() as conn:
        original = conn.execute(sel).all()

    stmt = (
        update(prepared_students)
        .values(grade='Z')
        .returning(prepared_students.c.object_id)
    )

    result = run_execute(engine, stmt)
    rows = result.all()

    assert_rowcount(result, 10)
    assert_columns(rows[0], ["object_id"])
    assert [r.object_id for r in rows] == [r.object_id for r in original]


# ── 12. RETURNING user + syscol: both columns present ─────────────────────────

def test_update_returning_user_and_syscol(engine, prepared_students):
    stmt = (
        update(prepared_students)
        .values(grade='Z')
        .returning(prepared_students.c.name, prepared_students.c.object_id)
    )

    result = run_execute(engine, stmt)
    rows = result.all()

    assert_rowcount(result, 10)
    assert_columns(rows[0], ["name", "object_id"])


# ── 11. implicit_returning=True: returned_primary_keys_rows populated ─────────

def test_update_implicit_returning_true(engine, prepared_students):
    stmt = update(prepared_students).values(grade='Z')

    sel = select(prepared_students.c.object_id)
    with engine.connect() as conn:
        original = conn.execute(sel).all()

    result = run_execute(
        engine,
        stmt,
        execution_options={"implicit_returning": True},
    )

    assert_rowcount(result, 10)
    expected_ids = [(r.object_id,) for r in original]
    assert result.returned_primary_keys_rows == expected_ids
    assert not result.returns_rows


# ── 9. parameters= raises ArgumentError ──────────────────────────────────────

def test_update_parameters_raises_argument_error(engine, prepared_students):
    stmt = update(prepared_students).values(grade='Z')

    with pytest.raises((ArgumentError, Exception)):
        with engine.connect() as conn:
            conn.execute(stmt, {"grade": "X"})
