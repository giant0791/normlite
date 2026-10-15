from datetime import date

import pytest

from normlite.sql.compiler import _depth
from normlite.sql.dml import delete, insert, select
from normlite.sql.elements import not_
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Integer, String

from tests.utils.execution import run_execute
from tests.utils.spies import QueryParamsRecordingClient, QueryPayloadRecordingClient
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


def test_delete_rechecks_a_negation_nested_under_an_or(engine, prepared_students):
    """The recheck decides a NOT that is buried under an OR (#412).

    The tree is a MIXED operator alternation, ``and -> or -> and -> not``.
    ``BooleanClauseList.__init__`` flattens same-operator nesting, so only
    alternating operators keep the ``not`` deep. The ``or`` has a ``not`` arm, so
    it is unpushable WHOLE. The pushed filter keeps only ``is_active IS TRUE`` and
    matches all eleven rows. The recheck must decide.

    Three-way cast:

    * the ten ``name_i`` rows have ``grade == "A"``, so the WHERE is false;
    * ``name_1`` has ``id == 1`` too. A recheck that drops the ``not`` trashes it;
    * ``Galileo`` has ``id == 1`` and ``grade == "C"``, so the WHERE is true.
      DELETE trashes him and only him.
    """
    with engine.connect() as conn:
        conn.execute(insert(prepared_students).values(
            name="Galileo Galilei", id=1, is_active=True,
            start_on=date(1600, 1, 1), grade="C",
        ))

    stmt = delete(prepared_students).where(
        prepared_students.c.is_active.is_(True)
        & (
            (prepared_students.c.grade == "B")
            | (
                (prepared_students.c.id == 1)
                & not_(prepared_students.c.grade == "A")
            )
        )
    )

    result = run_execute(engine, stmt)

    with engine.connect() as conn:
        survivors = conn.execute(select(prepared_students.c.name)).all()

    assert sorted(row.name for row in survivors) == [f"name_{i}" for i in range(10)]
    assert_rowcount(result, 1)

def test_delete_by_a_not_equal_where_keeps_a_valueless_row(engine, prepared_students):
    """A ``!=`` push must not delete the rows SQL calls UNKNOWN (#383).

    ``id != 1500`` compiles to Notion's ``number.does_not_equal``, whose negative
    operators are the boolean complement of their positive twins -- so it means
    "not 1500 OR empty" and MATCHES a valueless cell. SQL's ``<>`` against NULL is
    UNKNOWN, and the WHERE policy drops UNKNOWN, so the two disagree on exactly
    one row.

    On SELECT that disagreement is invisible: the push is a hint and ADR-0022's
    recheck re-applies the predicate over raw cells (see
    ``test_select_pipeline.test_a_valueless_cell_is_dropped_by_a_not_equal_where``,
    which pins the SAME predicate on the SAME three-row cast and reaches the
    OPPOSITE verdict -- there the valueless row must be dropped, here it must
    survive, because a row the predicate excludes is a row DELETE must not touch).
    Before #412, DELETE had no recheck: the pushed filter WAS the decision, so a
    filter that over-matched was a row destroyed. The ``!=`` leaf repair
    (``e26d7af``) made this push exact. Since #412 the recheck decides too, so
    this test is a guard on both.

    Three-way cast, because a row count alone would not observe the predicate:

    * the ten ``name_i`` rows (ids 0..9) are valued and unequal to 1500 -- SQL
      deletes them, so a repair that refuses the statement or over-narrows fails;
    * ``Galileo`` (id 1500) is valued and EQUAL, so the predicate is false and he
      survives -- this fails a "repair" that pushed ``is_not_empty`` alone;
    * ``Nullius`` (``{"number": null}`` on the wire) is the defect: UNKNOWN, and
      today he is archived.

    ``rowcount`` is asserted as well, and it is not a restatement of the survivor
    set: it counts what the scan MATCHED, which is the pushed filter's answer read
    directly, before ``Delete._setup_execution`` stages a single ``pages.update``.
    Measured on ``188d4ef`` it reads 11.
    """
    with engine.connect() as conn:
        # Valued and EQUAL: the predicate is false, so DELETE must spare him.
        conn.execute(insert(prepared_students).values(
            name="Galileo Galilei", id=1500, is_active=True,
            start_on=date(1600, 1, 1), grade="A",
        ))
        # Valueless: the predicate is UNKNOWN, so DELETE must spare him too.
        conn.execute(insert(prepared_students).values(
            name="Nullius", id=None, is_active=True,
            start_on=date(1600, 1, 1), grade="A",
        ))

    result = run_execute(engine, delete(prepared_students).where(
        prepared_students.c.id != 1500
    ))

    with engine.connect() as conn:
        survivors = conn.execute(select(prepared_students.c.name)).all()

    # Names, not ids: ``Nullius`` has no id, and sorting None against ints raises.
    assert {row.name for row in survivors} == {"Galileo Galilei", "Nullius"}

    assert_rowcount(result, 10)


def test_delete_prunes_a_where_that_the_ne_repair_makes_too_deep(
    engine, prepared_students, monkeypatch
):
    """A DELETE WHERE that measures 3 through the ``!=`` repair executes (#417).

    Every term of::

        WHERE name='name_0' AND (id != 1 OR grade='A')

    has a Notion filter form. The SHAPE of the emitted filter is the problem.
    The leaf repair (``e26d7af``) makes ``id != 1`` a level-1 compound. The
    fold cannot splice it under an ``or``, and the root ``and`` puts it at
    depth 3. The Notion API caps nesting at 2.

    Before #417 DELETE refused this WHERE (#383). DELETE now rechecks its WHERE
    (#412), so it prunes the filter to a legal superset and the recheck decides.

    ``test_a_too_deep_delete_deletes_exactly_the_rows_sql_selects`` covers a
    WHERE whose own shape is too deep. This test covers the second route to
    depth 3: the ``!=`` repair. SQL selects ``name_0`` only.
    """
    spy = QueryPayloadRecordingClient(engine._client)
    monkeypatch.setattr(engine.raw_connection(), "_client", spy)

    stmt = delete(prepared_students).where(
        (prepared_students.c.name == "name_0")
        & ((prepared_students.c.id != 1) | (prepared_students.c.grade == "A"))
    )

    result = run_execute(engine, stmt)

    assert spy.payloads_seen
    assert all(_depth(p.get("filter")) <= 2 for p in spy.payloads_seen), (
        f"filters pushed to the client exceed Notion's 2-level cap: {spy.payloads_seen}"
    )
    assert_rowcount(result, 1)

    with engine.connect() as conn:
        survivors = conn.execute(select(prepared_students.c.id)).all()

    assert sorted(row.id for row in survivors) == list(range(1, 10))


def test_delete_by_is_empty_deletes_only_the_valueless_row(engine, prepared_students):
    """A ``None``-valued leaf that is NOT a ``None`` literal stays pushable (#383).

    #383 makes a ``None`` literal under ``==`` or ``!=`` unpushable: Notion answers
    a ``null`` comparison value with HTTP 400 (measured 2026-10-05). But
    ``is_empty()`` and ``is_not_empty()`` build their leaf with the operand ``None``
    too (``ColumnOperators.is_empty``). Their ``None`` is a placeholder for "no
    operand", not a literal, and Notion accepts ``{"is_empty": true}``.

    A rule keyed on the value alone cannot tell the two leaves apart. Measured on
    such a rule (session 17): the DML gate refused this DELETE with "does not
    support negated terms" -- a reason that is false -- and the full suite stayed
    green. This test closes that gap. On SELECT the same mistake is invisible in
    the rows, because the recheck decides. Before #412 DELETE had no recheck, so
    the gate's answer was the user's answer.

    Two-way cast, so the test observes the predicate and not a row count:

    * the ten ``name_i`` rows (ids 0..9) are valued -- the predicate is false and
      DELETE must spare them;
    * ``Nullius`` (``{"number": null}`` on the wire) is valueless -- the predicate
      is true and DELETE must archive him.
    """
    with engine.connect() as conn:
        conn.execute(insert(prepared_students).values(
            name="Nullius", id=None, is_active=True,
            start_on=date(1600, 1, 1), grade="A",
        ))

    result = run_execute(engine, delete(prepared_students).where(
        prepared_students.c.id.is_empty()
    ))

    with engine.connect() as conn:
        survivors = conn.execute(select(prepared_students.c.id)).all()

    assert sorted(row.id for row in survivors) == list(range(10))

    assert_rowcount(result, 1)


def test_delete_by_is_not_empty_spares_only_the_valueless_row(engine, prepared_students):
    """The ``is_not_empty()`` twin of the test above (#383).

    ``is_not_empty()`` builds its leaf with the same ``None`` placeholder as
    ``is_empty()``. The sibling test pins only ``is_empty()``, so a rule that
    exempted that one operator from the ``None``-literal check, and not this
    one, passed the full suite.

    The cast is the same, and the verdict is the mirror: the ten valued rows
    match and DELETE archives them; ``Nullius`` is valueless, so the predicate
    is false and he survives.
    """
    with engine.connect() as conn:
        conn.execute(insert(prepared_students).values(
            name="Nullius", id=None, is_active=True,
            start_on=date(1600, 1, 1), grade="A",
        ))

    result = run_execute(engine, delete(prepared_students).where(
        prepared_students.c.id.is_not_empty()
    ))

    with engine.connect() as conn:
        survivors = conn.execute(select(prepared_students.c.name)).all()

    # Names, not ids: ``Nullius`` has no id.
    assert {row.name for row in survivors} == {"Nullius"}

    assert_rowcount(result, 10)


@pytest.mark.xfail(
    strict=True,
    reason="#422: eval3 answers `5 != None` as TRUE, not UNKNOWN",
)
def test_delete_by_a_none_comparison_trashes_nothing(engine, prepared_students):
    """``id != None`` is UNKNOWN for every row, so DELETE trashes nothing (#412).

    In SQL a comparison against NULL is UNKNOWN, and the WHERE policy drops
    UNKNOWN. The recheck decides the rows (ADR-0022), so its answer is the
    user's answer.

    Two-way cast, so both kinds of cell are covered:

    * the ten ``name_i`` rows have a valued ``id``. Today ``eval3`` answers
      ``5 != None`` as TRUE and DELETE trashes them. This is the defect (#422);
    * ``Nullius`` has a valueless ``id``. ``eval3`` already answers UNKNOWN.
    """
    with engine.connect() as conn:
        conn.execute(insert(prepared_students).values(
            name="Nullius", id=None, is_active=True,
            start_on=date(1600, 1, 1), grade="A",
        ))

    result = run_execute(
        engine, delete(prepared_students).where(prepared_students.c.id != None)
    )

    with engine.connect() as conn:
        survivors = conn.execute(select(prepared_students.c.name)).all()

    assert sorted(row.name for row in survivors) == sorted(
        ["Nullius"] + [f"name_{i}" for i in range(10)]
    )
    assert_rowcount(result, 0)


def test_delete_by_an_equal_none_comparison_executes_and_trashes_nothing(
    engine, prepared_students
):
    """``id == None`` executes and is UNKNOWN for every row, so DELETE trashes nothing (#412).

    Before #412, DELETE refused a ``None`` literal with a ``CompileError``. This
    test is a guard: it pins that the DELETE executes. The strict xfail on
    ``id != None`` cannot pin this, because a strict xfail accepts any exception.

    Two-way cast, so both kinds of cell are covered:

    * the ten ``name_i`` rows have a valued ``id``;
    * ``Nullius`` has a valueless ``id``. In SQL, ``NULL = NULL`` is UNKNOWN
      too, so DELETE does not trash him.
    """
    with engine.connect() as conn:
        conn.execute(insert(prepared_students).values(
            name="Nullius", id=None, is_active=True,
            start_on=date(1600, 1, 1), grade="A",
        ))

    result = run_execute(
        engine, delete(prepared_students).where(prepared_students.c.id == None)
    )

    with engine.connect() as conn:
        survivors = conn.execute(select(prepared_students.c.name)).all()

    assert sorted(row.name for row in survivors) == sorted(
        ["Nullius"] + [f"name_{i}" for i in range(10)]
    )
    assert_rowcount(result, 0)


def test_delete_by_an_empty_string_under_not_equal_spares_the_blank_row(
    engine, prepared_students
):
    """The recheck decides ``grade != ""``, not Notion (#382, #412).

    Measured against the live API (``Notion-Version: 2026-03-11``): Notion
    IGNORES ``rich_text.does_not_equal ""`` and returns every row. So the
    compiler does not push this leaf. No filter goes to Notion, the scan reads
    every row, and the recheck decides.

    Two-way cast:

    * the ten ``name_i`` rows have ``grade == "A"``. DELETE trashes them;
    * ``Blank`` has an empty grade. The WHERE is false, so he survives. A DELETE
      that trusts the unfiltered scan trashes him too.
    """
    with engine.connect() as conn:
        conn.execute(insert(prepared_students).values(
            name="Blank", id=100, is_active=True,
            start_on=date(1600, 1, 1), grade="",
        ))

    result = run_execute(
        engine, delete(prepared_students).where(prepared_students.c.grade != "")
    )

    with engine.connect() as conn:
        survivors = conn.execute(select(prepared_students.c.name)).all()

    assert [row.name for row in survivors] == ["Blank"]
    assert_rowcount(result, 10)

def test_delete_asks_notion_only_for_the_properties_its_where_reads(
    engine, prepared_students, monkeypatch
):
    # Arrange: ten rows with ids 0..9, and a DELETE whose WHERE reads only
    # `id`. A spy at the Notion boundary records the query params of every
    # `data_sources.query` request that the DELETE sends.
    spy = QueryParamsRecordingClient(engine._client)
    monkeypatch.setattr(engine.raw_connection(), "_client", spy)

    stmt = delete(prepared_students).where(prepared_students.c.id != 1)

    # Act: run the DELETE through the public API.
    result = run_execute(engine, stmt)

    # Assert: the DELETE trashes the same nine rows as before, and every
    # query to Notion asks for the `id` property only. `object_id` never
    # goes into filter_properties (ADR-0006).
    assert_rowcount(result, 9)
    assert spy.query_params_seen
    for query_params in spy.query_params_seen:
        assert query_params is not None
        assert query_params.get("filter_properties") == ["id"]


def test_delete_returning_a_column_the_scan_does_not_fetch_reads_the_write_response(
    engine, students, monkeypatch
):
    # Arrange: three rows with distinct grades. The DELETE WHERE reads only
    # `id`, so the Scan fetches `id` and never `grade` (ADR-0023 item 3).
    # Only the pages.update response carries `grade`.
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    ds_id = students.get_data_source_id()
    for i, grade in enumerate(["A", "B", "C"]):
        engine._client.pages_create(
            payload={
                "parent": {"type": "data_source_id", "data_source_id": ds_id},
                "properties": {
                    "name": {"title": [{"text": {"content": f"name_{i}"}}]},
                    "id": {"number": i},
                    "is_active": {"checkbox": True},
                    "start_on": {"date": {"start": "1600-01-01"}},
                    "grade": {"rich_text": [{"text": {"content": grade}}]},
                },
            }
        )

    spy = QueryParamsRecordingClient(engine._client)
    monkeypatch.setattr(engine.raw_connection(), "_client", spy)

    stmt = delete(students).where(students.c.id != 1).returning(students.c.grade)

    # Act
    result = run_execute(engine, stmt)
    rows = result.all()

    # Assert: the query never asks Notion for `grade`, yet every RETURNING
    # row has the grade of the page it trashed.
    for query_params in spy.query_params_seen:
        assert "grade" not in query_params.get("filter_properties", [])
    assert sorted(row.grade for row in rows) == ["A", "C"]
    assert_columns(rows[0], ["grade"])


@pytest.mark.parametrize("implicit_returning", [True, False])
def test_delete_returning_rows_hold_only_the_returning_columns_whatever_implicit_returning_says(
    engine, prepared_students, implicit_returning
):
    # Arrange: RETURNING names one user column. ADR-0023 item 4: the rows hold
    # exactly the RETURNING columns, as for INSERT ... RETURNING. The
    # `implicit_returning` option must not change the RETURNING row shape.
    stmt = (
        delete(prepared_students)
        .where(prepared_students.c.is_active.is_(True))
        .returning(prepared_students.c.grade)
    )

    # Act
    result = run_execute(
        engine,
        stmt,
        execution_options={"implicit_returning": implicit_returning},
    )
    rows = result.all()

    # Assert
    assert len(rows) == 10
    assert_columns(rows[0], ["grade"])


def test_delete_across_several_scan_pages_leaves_no_matching_row(engine, students):
    # Arrange: 250 matching rows span three Scan pages (page_size 100). The
    # fake client paginates by offset and drops trashed pages, so a DELETE
    # that trashes page 1 before it reads page 2 skips rows (the Halloween
    # problem, ADR-0023). yield_per=2 must not change the outcome (item 7).
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=250)

    stmt = delete(students).where(students.c.is_active.is_(True))

    # Act
    result = run_execute(engine, stmt, execution_options={"yield_per": 2})

    # Assert: every matching row is gone, and rowcount counts all of them.
    assert_rowcount(result, 250)
    sel = select(students.c.object_id).where(students.c.is_active.is_(True))
    with engine.connect() as conn:
        assert conn.execute(sel).all() == []


def test_delete_with_a_negated_conjunct_trashes_exactly_what_select_returns(
    engine, prepared_students
):
    # Arrange: ten rows with ids 0..9. The WHERE keeps ids 0 and 2 only.
    # Each statement gets a new WHERE object: compiling an expression assigns
    # roles to its binds, so one object cannot go into two statements.
    def where():
        return (prepared_students.c.id < 3) & not_(prepared_students.c.id == 1)

    with engine.connect() as conn:
        selected = conn.execute(select(prepared_students.c.id).where(where())).all()

    # Act
    result = run_execute(engine, delete(prepared_students).where(where()))

    # Assert: DELETE trashes the SELECT's rows, and no other row.
    assert sorted(row.id for row in selected) == [0, 2]
    assert_rowcount(result, 2)

    with engine.connect() as conn:
        survivors = conn.execute(select(prepared_students.c.id)).all()

    assert sorted(row.id for row in survivors) == [1, 3, 4, 5, 6, 7, 8, 9]


def test_delete_by_an_or_with_a_negated_arm_trashes_exactly_what_select_returns(
    engine, prepared_students
):
    # Arrange: ten rows with ids 0..9 and grade "A", plus Bee with grade "B".
    # The WHERE keeps name_1 (id == 1) and Bee (grade is not "A").
    with engine.connect() as conn:
        conn.execute(insert(prepared_students).values(
            name="Bee", id=50, is_active=True,
            start_on=date(1600, 1, 1), grade="B",
        ))

    def where():
        return (prepared_students.c.id == 1) | not_(prepared_students.c.grade == "A")

    with engine.connect() as conn:
        selected = conn.execute(select(prepared_students.c.name).where(where())).all()

    # Act
    result = run_execute(engine, delete(prepared_students).where(where()))

    # Assert: DELETE trashes the SELECT's rows, and no other row.
    assert sorted(row.name for row in selected) == ["Bee", "name_1"]
    assert_rowcount(result, 2)

    with engine.connect() as conn:
        survivors = conn.execute(select(prepared_students.c.name)).all()

    assert sorted(row.name for row in survivors) == sorted(
        f"name_{i}" for i in range(10) if i != 1
    )


def test_delete_returning_gives_only_the_trashed_rows(engine, prepared_students):
    # Arrange: ten rows with ids 0..9 and grade "A", plus Bee with grade "B".
    # The WHERE keeps name_1 (id == 1) and Bee (grade is not "A").
    with engine.connect() as conn:
        conn.execute(insert(prepared_students).values(
            name="Bee", id=50, is_active=True,
            start_on=date(1600, 1, 1), grade="B",
        ))

    stmt = (
        delete(prepared_students)
        .where(
            (prepared_students.c.id == 1) | not_(prepared_students.c.grade == "A")
        )
        .returning(prepared_students.c.name)
    )

    # Act
    result = run_execute(engine, stmt)
    rows = result.all()

    # Assert: one RETURNING row for each trashed row, and no other row.
    assert sorted(row.name for row in rows) == ["Bee", "name_1"]
    assert_rowcount(result, 2)


def test_a_too_deep_delete_deletes_exactly_the_rows_sql_selects(engine, monkeypatch):
    """A DELETE whose WHERE exceeds the depth cap executes and archives SQL's rows (#417).

    The WHERE nests ``and -> or -> and``. No leaf is a ``!=``::

        WHERE id < 1000 AND (grade = 'Z' OR (id > 5 AND is_active))

    The emitted filter measures 3. The Notion API caps nesting at 2.

    Two assertions, and each one fails a different wrong answer:

    * every filter that reaches the client measures 2 or less. The in-memory
      client has no cap, so only this assertion sees an HTTP 400;
    * the table keeps exactly the rows that SQL does not select.

    ``Xavier`` is the discriminator. A legal push can admit him (id > 5), and
    the WHERE excludes him (not active). He must survive. ``Zeta`` is the
    control: the WHERE selects her through ``grade = 'Z'`` alone, so she must go.
    """
    metadata = MetaData()
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("id", Integer()),
        Column("is_active", Boolean()),
        Column("grade", String()),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        # name_0..name_5 are excluded (id <= 5); name_6..name_9 are selected.
        for i in range(10):
            connection.execute(insert(students).values(
                name=f"name_{i}", id=i, is_active=True, grade="A",
            ))
        connection.execute(insert(students).values(
            name="Xavier", id=7, is_active=False, grade="Y",
        ))
        connection.execute(insert(students).values(
            name="Zeta", id=0, is_active=False, grade="Z",
        ))

    spy = QueryPayloadRecordingClient(engine._client)
    monkeypatch.setattr(engine.raw_connection(), "_client", spy)

    run_execute(
        engine,
        delete(students).where(
            (students.c.id < 1000)
            & (
                (students.c.grade == "Z")
                | ((students.c.id > 5) & (students.c.is_active == True))
            )
        ),
    )

    assert spy.payloads_seen, "the DELETE must query the data source"
    assert all(_depth(p.get("filter")) <= 2 for p in spy.payloads_seen), (
        f"filters pushed to the client exceed Notion's 2-level cap: {spy.payloads_seen}"
    )

    with engine.connect() as connection:
        survivors = connection.execute(select(students.c.name)).all()

    assert {row.name for row in survivors} == {
        "name_0", "name_1", "name_2", "name_3", "name_4", "name_5", "Xavier",
    }
