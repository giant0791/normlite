import re

import pytest

from normlite.exceptions import ArgumentError, CompileError
from normlite.sql.compiler import (
    _FILTER_MAX_DEPTH_EXCEEDED,
    _UNPUSHABLE_FILTER_TERM,
)
from normlite.sql.dml import update, select
from normlite.sql.elements import not_

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


# ── 10. A WHERE with no exact Notion filter is refused (#383) ────────────────

def test_update_with_a_negated_conjunct_is_refused_and_writes_nothing(
    engine, prepared_students
):
    """An UPDATE WHERE that cannot be pushed EXACTLY must raise, never prune (#383).

    The UPDATE twin of
    ``test_delete_pipeline.test_delete_with_a_negated_conjunct_is_refused_and_deletes_nothing``.
    Every populated row has ``grade == "A"``, so ``id == 1 AND NOT grade == "A"``
    is false for all ten rows, and SQL updates nothing.

    ``not`` has no Notion filter form. UPDATE has no recheck (#397), so the
    pushed filter IS the decision. A filter that drops the negated conjunct
    reads ``id == 1`` and writes ``grade = 'Z'`` into ``name_1``.

    The refusal can have one reason only: the unpushable term. The WHERE has
    two leaves under one ``and``, so its filter cannot exceed the nesting cap.

    The grade assertion is not redundant with the raise. It fails a gate that
    raises after the ``pages.update`` writes are staged.
    """
    stmt = (
        update(prepared_students)
        .values(grade="Z")
        .where(
            (prepared_students.c.id == 1)
            & not_(prepared_students.c.grade == "A")
        )
    )

    with pytest.raises(CompileError, match=re.escape(_UNPUSHABLE_FILTER_TERM)):
        run_execute(engine, stmt)

    rows = select_all(engine, prepared_students)
    assert sorted((r.id, r.grade) for r in rows) == [(i, "A") for i in range(10)]


def test_update_refuses_a_where_whose_own_shape_exceeds_the_nesting_cap(
    engine, prepared_students
):
    """An UPDATE WHERE nested ``and -> or -> and`` must raise, never prune (#383).

    The UPDATE twin of
    ``test_delete_pipeline.test_delete_refuses_a_where_whose_own_shape_exceeds_the_nesting_cap``.
    No leaf is a ``!=``, so the depth does not depend on the ``!=`` repair::

        WHERE name='name_0' AND (id=1 OR (grade='A' AND is_active IS TRUE))

    Every term has a Notion filter form, so the refusal can have one reason
    only: the depth. The operators alternate, so the fold has nothing to
    splice. The emitted filter is ``{and: [leaf, {or: [leaf, {and: [leaf,
    leaf]}]}]}``: depth 3. The Notion API caps nesting at 2, so production
    answers HTTP 400. The in-memory client has no cap and would write
    ``grade = 'Z'`` into ``name_0``, the one row SQL selects.

    UPDATE has no recheck (#397), so it may not prune the filter to fit the
    cap. A pruned filter is a superset and would write rows this WHERE
    excludes.

    The grade assertion is not redundant with the raise. It fails a gate that
    raises after the ``pages.update`` writes are staged.
    """
    stmt = (
        update(prepared_students)
        .values(grade="Z")
        .where(
            (prepared_students.c.name == "name_0")
            & (
                (prepared_students.c.id == 1)
                | (
                    (prepared_students.c.grade == "A")
                    & prepared_students.c.is_active.is_(True)
                )
            )
        )
    )

    with pytest.raises(CompileError, match=re.escape(_FILTER_MAX_DEPTH_EXCEEDED)):
        run_execute(engine, stmt)

    rows = select_all(engine, prepared_students)
    assert sorted((r.id, r.grade) for r in rows) == [(i, "A") for i in range(10)]


def test_update_by_an_empty_string_comparison_is_refused_with_a_true_reason(
    engine, prepared_students
):
    """An UPDATE refusal must name the reason it refuses (#382).

    Measured against the live API (``Notion-Version: 2026-03-11``): Notion
    IGNORES ``rich_text.equals ""`` and returns every row. UPDATE has no
    recheck (#397), so against real Notion this WHERE writes ``grade = 'Z'``
    into every row.

    The refusal must name the empty-string literal. ``grade == ""`` is neither
    a negated term nor a ``None`` comparison, so a reason that names only those
    is false for this WHERE, and a user who reads a false reason cannot fix it.

    The grade assertion fails a gate that raises after the ``pages.update``
    writes are staged.
    """
    stmt = (
        update(prepared_students)
        .values(grade="Z")
        .where(prepared_students.c.grade == "")
    )

    with pytest.raises(CompileError, match=r"(?i)empty[- ]string"):
        run_execute(engine, stmt)

    rows = select_all(engine, prepared_students)
    assert sorted((r.id, r.grade) for r in rows) == [(i, "A") for i in range(10)]

