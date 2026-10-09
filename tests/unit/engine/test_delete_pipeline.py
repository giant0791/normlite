import pdb
from datetime import date

import re

import pytest

from normlite.exceptions import CompileError
from normlite.sql.compiler import (
    _FILTER_MAX_DEPTH_EXCEEDED,
    _UNPUSHABLE_FILTER_TERM,
)
from normlite.sql.dml import delete, insert, select
from normlite.sql.elements import not_

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


def test_delete_with_a_negated_conjunct_is_refused_and_deletes_nothing(
    engine, prepared_students
):
    """A DML WHERE that cannot be pushed EXACTLY must raise, never prune (#383).

    Every populated row carries ``grade == "A"``, so ``id == 1 AND NOT grade == "A"``
    is false for all ten of them and SQL deletes nothing.

    ``not`` has no Notion filter form, so the pushed filter cannot mean what this
    WHERE means. On the SELECT path that is harmless: the conjunct is dropped, the
    filter weakens to a superset, and the client-side recheck decides (ADR-0022).
    DELETE has no recheck (#397) -- the pushed filter IS the decision -- so a
    dropped conjunct is a deleted row. Refusing to compile is the only sound
    answer until #397 retires the refusal.

    The survivor assertion is not redundant with the raise: it fails a gate that
    fires *after* :meth:`Delete._setup_execution` has already staged the
    ``pages.update`` archives.
    """
    stmt = delete(prepared_students).where(
        (prepared_students.c.id == 1) & not_(prepared_students.c.grade == "A")
    )

    with pytest.raises(CompileError, match=re.escape(_UNPUSHABLE_FILTER_TERM)):
        run_execute(engine, stmt)

    with engine.connect() as conn:
        survivors = conn.execute(select(prepared_students.c.id)).all()

    assert sorted(row.id for row in survivors) == list(range(10))

def test_delete_refuses_a_negation_nested_under_an_or(engine, prepared_students):
    """The DML gate must judge the WHOLE tree, not just its root (#383).

    Sibling of :func:`test_delete_with_a_negated_conjunct_is_refused_and_deletes_nothing`,
    which pins the gate itself. This one pins the RECURSION: the strict mode is a
    property of the walk, so it has to reach a ``not`` buried at any depth.

    The tree is a MIXED operator alternation, ``and -> or -> and -> not``, and that
    is the whole point of the shape. ``BooleanClauseList.__init__`` flattens
    same-operator nesting at construction, so an ``and`` directly inside an ``and``
    collapses into one node the root-level check already catches; only alternating
    operators preserve the depth. A gate that recursed in lax mode reported this
    tree as pushable, dropped ``NOT grade == "A"``, and archived ``name_1``.

    Every row has ``is_active`` true and ``grade == "A"``, so ``grade == "B"`` is
    false and ``id == 1 AND NOT grade == "A"`` is false: the WHERE is false for all
    ten rows and SQL deletes nothing. The pruned filter would delete exactly one,
    which is why the survivor assertion names the surviving ids.
    """
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

    with pytest.raises(CompileError, match=re.escape(_UNPUSHABLE_FILTER_TERM)):
        run_execute(engine, stmt)

    with engine.connect() as conn:
        survivors = conn.execute(select(prepared_students.c.id)).all()

    assert sorted(row.id for row in survivors) == list(range(10))


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
    DELETE has no recheck (#397): the pushed filter IS the decision, so a filter
    that over-matches is a row destroyed. ``_all_terms_pushable`` cannot see this
    -- no *term* is dropped, the loss is inside one term.

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


def test_delete_refuses_a_where_it_cannot_push_within_the_nesting_cap(
    engine, prepared_students
):
    """A DML WHERE whose filter measures 3 must raise, never prune (#383).

    Third sibling of the two refusal tests above, and the one that does NOT
    turn on a ``not``. Every term of::

        WHERE name='name_0' AND (id != 1 OR grade='A')

    has a Notion filter form, so ``_all_terms_pushable`` admits it and always
    will: its question is "would any term be DROPPED", and none is. What is
    wrong here is not a missing term but the SHAPE of what the terms emit -- the
    leaf repair (``e26d7af``) makes ``id != 1`` a level-1 compound, the fold's
    splice cannot absorb it under an ``or``, and the root ``and`` puts the
    result at depth 3. The Notion API caps compound nesting at 2, so this is an
    HTTP 400. Only the EMITTED dict knows that number, which is why the gate
    cannot answer this question before emission the way it answers the other
    two.

    **What this test refuses is a statement that currently returns the RIGHT
    answer.** Measured on this tree: ``name_0`` has ``id == 0`` and
    ``grade == "A"``, so SQL deletes exactly that one row, and today the
    in-memory client accepts the depth-3 filter and archives exactly that one
    row -- ``rowcount`` 1, ids 1..9 surviving. The fake has no nesting cap, so
    the bug is invisible here and fatal in production. Refusing a legal DELETE
    is the cost of raise-or-nothing: DELETE has no recheck (#397), so pruning
    the filter to fit the cap -- sound on SELECT, where ADR-0022 re-narrows over
    raw cells, and pinned there by
    ``test_compiler_axioms.test_repaired_ne_under_an_or_is_pruned_to_a_legal_depth``
    -- would archive rows this WHERE excludes. A refusal costs a rewrite; a
    prune costs rows.

    The survivor assertion is not redundant with the raise: it fails a gate that
    fires *after* :meth:`Delete._setup_execution` has staged the ``pages.update``
    archives. Emit-then-measure moves the decision later than the existing gate,
    which is exactly the window in which that mistake is available.
    """
    stmt = delete(prepared_students).where(
        (prepared_students.c.name == "name_0")
        & ((prepared_students.c.id != 1) | (prepared_students.c.grade == "A"))
    )

    with pytest.raises(CompileError, match=re.escape(_FILTER_MAX_DEPTH_EXCEEDED)):
        run_execute(engine, stmt)

    with engine.connect() as conn:
        survivors = conn.execute(select(prepared_students.c.id)).all()

    assert sorted(row.id for row in survivors) == list(range(10))


def test_delete_refuses_a_where_whose_own_shape_exceeds_the_nesting_cap(
    engine, prepared_students
):
    """A DML WHERE nested ``and -> or -> and`` must raise, never prune (#383).

    The sibling above reaches depth 3 only through the ``!=`` repair
    (``e26d7af``): revert the repair and that test loses its reason to raise.
    This test does not depend on the repair. No leaf is a ``!=``::

        WHERE name='name_0' AND (id=1 OR (grade='A' AND is_active IS TRUE))

    Every term has a Notion filter form, so ``_all_terms_pushable`` admits it.
    The operators alternate, so the fold has no same-operator child to splice.
    The emitted filter is ``{and: [leaf, {or: [leaf, {and: [leaf, leaf]}]}]}``:
    root ``and`` at level 1, ``or`` at level 2, inner ``and`` at level 3. The
    Notion API caps nesting at 2, so production answers HTTP 400. The in-memory
    client has no cap and would archive ``name_0`` -- the one row SQL selects.

    The survivor assertion fails a gate that raises after the archives are
    staged.
    """
    stmt = delete(prepared_students).where(
        (prepared_students.c.name == "name_0")
        & (
            (prepared_students.c.id == 1)
            | (
                (prepared_students.c.grade == "A")
                & prepared_students.c.is_active.is_(True)
            )
        )
    )

    with pytest.raises(CompileError, match=re.escape(_FILTER_MAX_DEPTH_EXCEEDED)):
        run_execute(engine, stmt)

    with engine.connect() as conn:
        survivors = conn.execute(select(prepared_students.c.id)).all()

    assert sorted(row.id for row in survivors) == list(range(10))


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
    the rows, because the recheck decides; DELETE has no recheck (#397), so the
    gate's answer is the user's answer.

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


def test_delete_by_a_none_comparison_is_refused_and_deletes_nothing(
    engine, prepared_students
):
    """A ``None`` literal under ``!=`` is unpushable, so DELETE refuses it (#383).

    Notion rejects a ``null`` comparison value under ``does_not_equal`` with HTTP 400
    (measured 2026-10-05). DELETE has no recheck (#397), so it cannot drop the
    term. It must refuse the WHERE.

    In SQL, ``id != NULL`` is UNKNOWN for every row, so SQL deletes nothing.

    The survivor assertion fails a gate that fires *after*
    :meth:`Delete._setup_execution` has staged the ``pages.update`` archives.
    """
    stmt = delete(prepared_students).where(prepared_students.c.id != None)

    with pytest.raises(CompileError, match=re.escape(_UNPUSHABLE_FILTER_TERM)):
        run_execute(engine, stmt)

    with engine.connect() as conn:
        survivors = conn.execute(select(prepared_students.c.id)).all()

    assert sorted(row.id for row in survivors) == list(range(10))


def test_delete_with_an_empty_string_under_not_equal_is_refused_and_deletes_nothing(
    engine, prepared_students
):
    """A DELETE must refuse an empty-string literal under ``!=`` (#382).

    Measured against the live API (``Notion-Version: 2026-03-11``): Notion
    IGNORES ``rich_text.does_not_equal ""`` and returns every row of the data
    source. DELETE has no recheck (#397), so the pushed filter IS the decision.
    Against real Notion this WHERE deletes every row, blank cells included.

    The in-memory client evaluates ``does_not_equal ""`` correctly. Today this
    statement deletes all ten rows here, and that looks correct. The defect is
    visible only against real Notion, so the test pins the refusal.

    The survivor assertion fails a gate that fires *after*
    :meth:`Delete._setup_execution` has already staged the ``pages.update``
    archives.
    """
    stmt = delete(prepared_students).where(prepared_students.c.grade != "")

    with pytest.raises(CompileError, match=re.escape(_UNPUSHABLE_FILTER_TERM)):
        run_execute(engine, stmt)

    with engine.connect() as conn:
        survivors = conn.execute(select(prepared_students.c.id)).all()

    assert sorted(row.id for row in survivors) == list(range(10))
