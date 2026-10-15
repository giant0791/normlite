import re
from datetime import date

import pytest

from normlite.exceptions import ArgumentError, CompileError
from normlite.sql.compiler import _FILTER_MAX_DEPTH_EXCEEDED
from normlite.sql.dml import insert, update, select
from normlite.sql.elements import not_

from tests.utils.execution import run_execute
from tests.utils.spies import QueryParamsRecordingClient
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


def test_update_by_an_empty_string_comparison_changes_only_the_blank_row(
    engine, prepared_students
):
    """The recheck decides ``grade == ""``, not Notion (#382, #413).

    Measured against the live API (``Notion-Version: 2026-03-11``): Notion
    IGNORES ``rich_text.equals ""`` and returns every row. So the
    compiler does not push this leaf. No filter goes to Notion, the scan reads
    every row, and the recheck decides.

    Two-way cast:

    * the ten ``name_i`` rows have ``grade == "A"``. UPDATE leaves them unchanged;
    * ``Blank`` has an empty grade. The WHERE is true, so it changes to "Z".

    An UPDATE that trusts the unfiltered Scan changes all eleven rows,
    so this test can tell the two behaviors apart.
    """
    with engine.connect() as conn:
        conn.execute(insert(prepared_students).values(
            name="Blank", id=100, is_active=True,
            start_on=date(1600, 1, 1), grade="",
        ))

    update_stmt = (
        update(prepared_students)
        .where(prepared_students.c.grade == "")
        .values(grade="Z")
        .returning(prepared_students.c.name)
    )
    result = run_execute(engine, update_stmt)

    assert [row.name for row in result] == ["Blank"]
    assert_rowcount(result, 1)

    rows = select_all(engine, prepared_students)
    assert sorted((r.name, r.grade) for r in rows) == sorted(
        [("Blank", "Z")] + [(f"name_{i}", "A") for i in range(10)]
    )


def test_update_asks_notion_only_for_the_properties_its_where_reads(
    engine, prepared_students, monkeypatch
):
    # Arrange: ten rows with ids 0..9, and an UPDATE whose WHERE reads only
    # `id`. A spy at the Notion boundary records the query params of every
    # `data_sources.query` request that the UPDATE sends.
    spy = QueryParamsRecordingClient(engine._client)
    monkeypatch.setattr(engine.raw_connection(), "_client", spy)

    stmt = (
        update(prepared_students)
        .values(grade="Z")
        .where(prepared_students.c.id != 1)
    )

    # Act: run the UPDATE through the public API.
    result = run_execute(engine, stmt)

    # Assert: the UPDATE changes nine rows, and every query to Notion asks
    # for the `id` property only. `object_id` never goes into
    # filter_properties (ADR-0006).
    assert_rowcount(result, 9)
    assert spy.query_params_seen
    for query_params in spy.query_params_seen:
        assert query_params is not None
        assert query_params.get("filter_properties") == ["id"]


def test_update_with_no_where_asks_notion_only_for_the_title(
    engine, prepared_students, monkeypatch
):
    # Arrange: ten rows, and an UPDATE with no WHERE. The UPDATE reads no
    # property, but Notion cannot return zero properties: an empty
    # filter_properties means "all properties". The title is the one property
    # that every Notion data source has. In `students`, the title column is
    # `name`. A spy at the Notion boundary records the query params of every
    # `data_sources.query` request that the UPDATE sends.
    spy = QueryParamsRecordingClient(engine._client)
    monkeypatch.setattr(engine.raw_connection(), "_client", spy)

    stmt = update(prepared_students).values(grade="Z")

    # Act: run the UPDATE through the public API.
    result = run_execute(engine, stmt)

    # Assert: the UPDATE changes all ten rows, and every query to Notion asks
    # for the title property only, by its column name. `object_id` never goes
    # into filter_properties (ADR-0006).
    assert_rowcount(result, 10)
    assert spy.query_params_seen
    for query_params in spy.query_params_seen:
        assert query_params is not None
        assert query_params.get("filter_properties") == ["name"]


def test_update_with_a_negated_conjunct_changes_exactly_what_select_returns(
    engine, prepared_students
):
    # Arrange: ten rows with ids 0..9 and grade "A". The WHERE keeps ids 0
    # and 2 only. Each statement gets a new WHERE object: compiling an
    # expression assigns roles to its binds, so one object cannot go into
    # two statements (#421).
    def where():
        return (prepared_students.c.id < 3) & not_(prepared_students.c.id == 1)

    with engine.connect() as conn:
        selected = conn.execute(select(prepared_students.c.id).where(where())).all()

    # Act
    result = run_execute(
        engine, update(prepared_students).values(grade="Z").where(where())
    )

    # Assert: UPDATE changes the SELECT's rows, and no other row.
    assert sorted(row.id for row in selected) == [0, 2]
    assert_rowcount(result, 2)

    rows = select_all(engine, prepared_students)
    assert sorted((r.id, r.grade) for r in rows) == [
        (i, "Z" if i in (0, 2) else "A") for i in range(10)
    ]
