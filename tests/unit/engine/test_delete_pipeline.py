import pdb

import pytest

from normlite.sql.dml import delete, select

from tests.utils.execution import run_execute
from tests.utils.db_helpers import (
    create_students_db,
    attach_table_oid,
    populate_students,
)
from tests.utils.assertions import (
    assert_rowcount,
    assert_no_rows,
    assert_columns,
)

# --------------------------------------
# Fixtures
# --------------------------------------


@pytest.fixture
def prepared_students(engine, students):
    """
    Fully prepared students table:
    - database created
    - table attached (object_id set)
    - populated with rows
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=10)
    return students


# --------------------------------------
# Pipeline tests
# --------------------------------------

def test_delete_all_rows(engine, prepared_students):
    stmt = delete(prepared_students).where(
        prepared_students.c.is_active.is_(True)
    )

    result = run_execute(engine, stmt)

    # behavior assertions
    assert_rowcount(result, 10)
    assert_no_rows(result)

    # verify state
    sel = select(prepared_students).where(
        prepared_students.c.is_active.is_(True)
    )
    with engine.connect() as conn:
        remaining = conn.execute(sel).all()

    assert remaining == []


def test_delete_returning_syscols(engine, prepared_students):
    stmt = (
        delete(prepared_students)
        .where(prepared_students.c.is_active.is_(True))
        .returning(prepared_students.c.object_id)
    )

    # fetch original ids
    sel = select(prepared_students.c.object_id).where(
        prepared_students.c.is_active.is_(True)
    )
    with engine.connect() as conn:
        original = conn.execute(sel).all()

    result = run_execute(engine, stmt)
    rows = result.all()

    # behavior assertions
    assert_rowcount(result, 10)

    # ensure returned ids match original
    assert [r.object_id for r in rows] == [r.object_id for r in original]

    # ensure only system column is present
    assert_columns(rows[0], ["object_id"])

def test_delete_returning_all_cols(engine, prepared_students):
    stmt = (
        delete(prepared_students)
        .where(prepared_students.c.is_active.is_(True))
        .returning(*prepared_students.c)
    )

    result = run_execute(engine, stmt)
    rows = result.all()

    assert_rowcount(result, 10)
    assert_columns(rows[0], [c.name for c in prepared_students.c])


def test_delete_returning_user_columns(engine, prepared_students):
    stmt = (
        delete(prepared_students)
        .where(prepared_students.c.is_active.is_(True))
        .returning(
            prepared_students.c.object_id,
            prepared_students.c.name,
            prepared_students.c.id,
        )
    )

    # fetch original rows
    sel = select(
        prepared_students.c.object_id,
        prepared_students.c.name,
        prepared_students.c.id,
    ).where(prepared_students.c.is_active.is_(True))

    with engine.connect() as conn:
        original = conn.execute(sel).all()

    result = run_execute(engine, stmt)
    rows = result.all()

    assert_rowcount(result, 10)

    # row-by-row comparison
    for i, row in enumerate(rows):
        assert row.object_id == original[i].object_id
        assert row.name == original[i].name
        assert row.id == original[i].id

    assert_columns(rows[0], ["object_id", "name", "id"])


def test_delete_implicit_returning_true(engine, prepared_students):
    stmt = delete(prepared_students).where(
        prepared_students.c.is_active.is_(True)
    )

    # capture original ids
    sel = select(prepared_students.c.object_id).where(
        prepared_students.c.is_active.is_(True)
    )
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


def test_delete_implicit_returning_false(engine, prepared_students):
    stmt = delete(prepared_students).where(
        prepared_students.c.is_active.is_(True)
    )

    result = run_execute(engine, stmt)

    assert_rowcount(result, 10)
    assert result.returned_primary_keys_rows is None
    assert not result.returns_rows


def test_delete_does_not_affect_other_rows(engine, students):
    """
    Sanity test: ensure WHERE clause is respected.
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)

    # create mixed dataset
    populate_students(engine, students, n=5, is_active=True)
    populate_students(engine, students, n=5, is_active=False)

    stmt = delete(students).where(students.c.is_active.is_(True))

    result = run_execute(engine, stmt)

    assert_rowcount(result, 5)

    # verify inactive rows still exist
    sel = select(students).where(students.c.is_active.is_(False))
    with engine.connect() as conn:
        remaining = conn.execute(sel).all()

    assert len(remaining) == 5