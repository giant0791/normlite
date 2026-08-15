import pytest

from normlite.engine.base import Engine
from normlite.sql.dml import insert, select
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Integer, String


def test_a_valueless_cell_is_dropped_by_a_not_equal_where(engine: Engine):
    # RED 1 for C2 step 4 (#384 / ADR-0022): the recheck, on the scan path.
    #
    # `WHERE id != 1500` is compiled to Notion's `does_not_equal` and pushed into
    # the Scan's payload. Notion's negative operators are the boolean complement
    # of their positive twins -- measured against the live API, not modelled --
    # so `does_not_equal` KEEPS a valueless cell. SQL's `<>` against a NULL is
    # UNKNOWN, and the WHERE policy drops UNKNOWN.
    #
    # Today the push decides alone for a left-table conjunct: it is compiled into
    # `payload['filter']` and never re-applied client-side, so Notion's answer IS
    # normlite's answer and "Nullius" comes back. ADR-0022 makes every pushed
    # conjunct a hint that is re-checked over raw cells by eval3 in a Filter
    # Operator, and it is the recheck that decides.
    #
    # The in-memory client reproduces #384 faithfully on this operator, so this
    # red needs no live API. That is a statement about the model agreeing with a
    # measured API on ONE pair, not a promotion of the model to evidence.
    metadata = MetaData()
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("id", Integer()),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        # Galileo is the discriminator: a VALUED cell the predicate must exclude.
        # Without him, a recheck that kept every row would be indistinguishable
        # from one that worked.
        connection.execute(insert(students).values(name="Galileo Galilei", id=1500))
        # Newton is the other control: a valued cell the predicate must KEEP, so
        # an over-eager recheck that dropped everything cannot pass either.
        connection.execute(insert(students).values(name="Isaac Newton", id=1600))
        # Nullius is #384 itself: a valueless cell, `{"number": null}` on the wire.
        connection.execute(insert(students).values(name="Nullius", id=None))

        rows = connection.execute(
            select(students).where(students.c.id != 1500)
        ).fetchall()

    # Assert: three rows in, exactly one out. UNKNOWN is dropped, and the two
    # valued cells split -- which is what makes this an observation of the
    # predicate rather than of the row count.
    assert {row.name for row in rows} == {"Isaac Newton"}


def test_a_narrow_projection_rechecks_a_column_it_does_not_return(engine: Engine):
    # RED 2 for C2 step 4 (#384 / ADR-0022): the widening, and the trim that
    # must survive it.
    #
    # Same predicate as RED 1, but `id` is now NOT projected. Measured on this
    # tree: the statement compiles to `fetch_columns == ['name']` and
    # `filter_properties == ['name']` -- the predicate column is not fetched at
    # all. One list is doing two jobs (`filter_properties` IS `result_columns`,
    # which IS `fetch_columns` minus specials, compiler.py:717-732), and the
    # recheck forces them apart: `id` must reach Notion's filter_properties or
    # the cell never comes back, and it must NOT reach the user's Row.
    #
    # Today the cell is lost in the SCHEMA, one stage earlier than ADR-0022's
    # Consequences reasoned. It does not fail loudly via AttributeError in
    # resultset.py: `query_params` is only set when `result_columns` is
    # non-empty, so for the shape the delete tests use Notion returns the whole
    # page -- and here the Scan's schema, built from `fetch_columns`, simply has
    # no `id` cell. eval3 answers UNKNOWN for every row, the WHERE policy drops
    # UNKNOWN, and the query returns NOTHING, silently. That ADR bullet is
    # measured wrong and is owed an edit in the commit that fixes this.
    #
    # The discriminating control lives one test up and one query over:
    # `select(students.c.name).where(students.c.name != ...)` -- predicate
    # column projected -- already returns the right rows on this tree. So this
    # red is about the column the projection omits, not about the recheck.
    metadata = MetaData()
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("id", Integer()),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        connection.execute(insert(students).values(name="Galileo Galilei", id=1500))
        connection.execute(insert(students).values(name="Isaac Newton", id=1600))
        connection.execute(insert(students).values(name="Nullius", id=None))

        rows = connection.execute(
            select(students.c.name).where(students.c.id != 1500)
        ).fetchall()

    # (1) The rows. Same three-way split as RED 1 -- Galileo excluded by the
    # predicate, Nullius dropped as UNKNOWN, Newton kept -- so a widening that
    # fetched `id` but never handed it to the recheck cannot pass either.
    assert {row.name for row in rows} == {"Isaac Newton"}

    # (2) The trim, and nothing else in the suite can see it. `keys()` on the
    # plan path comes from the plan ROOT's schema (base.py:321 seeds the result
    # set with `plan.result_schema`), so this pins Project to the PRE-WIDENING
    # fetch_columns rather than to the widened one. Widen without trimming and
    # (1) still passes while `id` rides into every user Row.
    #
    # Written over all rows, not rows[0], so it reds by ASSERTION on today's
    # empty result rather than by IndexError.
    assert [list(row.keys()) for row in rows] == [["name"]]


def test_a_compound_where_on_the_scan_path_rechecks_every_conjunct(engine: Engine):
    # RED 2b for C2 step 4 (#384 / ADR-0022): a compound WHERE on the SCAN path.
    #
    # This is a REGRESSION, not a new feature. `.where()` chained twice folds
    # into a BooleanClauseList (dml.py:541-550) -- `and_()` builds the same AST,
    # no import needed -- and the shape was measured GREEN at 058059d, returning
    # {"Isaac Newton"}. It broke when the hold went uniform: the compiler's
    # non-join branch now holds the WHOLE compound as recheck_where, and the
    # widening condition (queryplan.py:543,546) reads `recheck_where.column`,
    # which a BooleanClauseList does not have.
    #
    # Nothing in tests/unit covered a compound WHERE on a plain SELECT -- the
    # only compound-AND observers are on the join path -- which is why 922 tests
    # stayed green over a crash. Same species as 058059d's join hole (see the
    # handoff's section 4): a shape with no observer, broken silently by a
    # change that measured cheap everywhere it was watched.
    #
    # The fix is item 2, a `_get_expression_columns` fold that
    # `_get_expression_parent_tables` is then derived from: a predicate reads a
    # SET of columns, not one. It is ONLY the widening that needs it --
    # the Filter is already compound-clean, because eval3 recurses through a
    # BooleanClauseList with correct 3VL (eval3.py:163-176, FALSE dominates AND,
    # UNKNOWN survives it) and reads `.column` per LEAF, while the Filter builds
    # its properties dict from the SCHEMA rather than from the predicate. Widen
    # to the full column set and the rest already works.
    metadata = MetaData()
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("id", Integer()),
        Column("is_active", Boolean()),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        # Four rows, and each of the three that must NOT come back is excluded
        # by a DIFFERENT mechanism. A recheck that handled only one conjunct,
        # or that collapsed 3VL to two values, keeps one of them.
        #
        #   Galileo  FALSE   and TRUE   -> FALSE    (left conjunct decides)
        connection.execute(
            insert(students).values(name="Galileo Galilei", id=1500, is_active=True)
        )
        #   Newton   TRUE    and TRUE   -> TRUE     (the only survivor)
        connection.execute(
            insert(students).values(name="Isaac Newton", id=1600, is_active=True)
        )
        #   Kepler   UNKNOWN and TRUE   -> UNKNOWN  (#384, THROUGH the AND)
        connection.execute(
            insert(students).values(name="Johannes Kepler", id=None, is_active=True)
        )
        #   Nullius  TRUE    and FALSE  -> FALSE    (right conjunct decides)
        connection.execute(
            insert(students).values(name="Nullius", id=1600, is_active=False)
        )

        # Chained .where(), the zero-ceremony spelling. Both conjuncts belong to
        # the one table, so the whole compound is pushed AND held.
        rows = connection.execute(
            select(students.c.name)
            .where(students.c.id != 1500)
            .where(students.c.is_active == True)  # noqa: E712 -- SQL equality, not identity
        ).fetchall()

    # (1) Kepler is the row only the recheck can drop: Notion's `does_not_equal`
    # keeps his valueless cell, `checkbox equals true` keeps him too, so the
    # PUSH returns him. UNKNOWN and TRUE is UNKNOWN, and the WHERE policy drops
    # it. He is #384 surviving into a compound.
    assert {row.name for row in rows} == {"Isaac Newton"}

    # (2) The trim again, now over a TWO-column widening: neither `id` nor
    # `is_active` is projected, so both must reach the Scan and neither may
    # reach the user's Row.
    assert [list(row.keys()) for row in rows] == [["name"]]


def test_a_predicate_sharing_a_column_with_the_projection_still_widens_the_other(
    engine: Engine,
):
    # RED 2c for C2 step 4 (#384 / ADR-0022): PARTIAL OVERLAP between the
    # predicate's columns and the projection.
    #
    # RED 2b widens two columns and RED 2 widens one, but in BOTH of them every
    # predicate column is unprojected. This is the third shape: the predicate
    # reads `name`, which IS fetched, and `id`, which is NOT. The widening has
    # to be decided per column -- a set-wide test answers "is ANY predicate
    # column already fetched?", which is not the question, and on this shape it
    # answers True and widens NOTHING.
    #
    # Measured on this tree: `id` never reaches execution_names, the Scan's
    # schema has no `id` cell, eval3 answers UNKNOWN for every row, and the
    # WHERE policy drops all of them --
    #
    #     PROBE where_cols=['id', 'name']  exec_before=['name']
    #     PROBE exec_after=['name']
    #     rows == set()
    #
    # -- silently, with the other 923 tests green. Same species as the three
    # holes in the handoff's section 4, and the fourth instance of the same
    # generalisation: going uniform widens the set of AST shapes reaching the
    # code below, and every newly-admitted shape needs its own observer. This is
    # the shape "compound WHERE" admits that RED 2b cannot see, because RED 2b's
    # two predicate columns are both unprojected.
    metadata = MetaData()
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("id", Integer()),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        # Four rows. Kepler is the only one the RECHECK uniquely decides -- that
        # is inherent, not a thin fixture: push and recheck agree on every
        # VALUED cell (ADR-0022's whole point is that only the valueless ones
        # diverge), so a valued row the recheck must drop is already gone from
        # the push. The other three pin the surrounding machinery instead.
        #
        #   Galileo  FALSE   and TRUE    -> FALSE    (the PROJECTED conjunct
        #                                             decides; excluded by the
        #                                             push too)
        connection.execute(
            insert(students).values(name="Galileo Galilei", id=1700)
        )
        #   Newton   TRUE    and TRUE    -> TRUE     (the only survivor, and the
        #                                             guard against a widening
        #                                             that drops everything)
        connection.execute(insert(students).values(name="Isaac Newton", id=1600))
        #   Kepler   TRUE    and UNKNOWN -> UNKNOWN  (#384, on the UNPROJECTED
        #                                             column: the push KEEPS him
        #                                             because does_not_equal is
        #                                             the complement of equals)
        connection.execute(insert(students).values(name="Johannes Kepler", id=None))
        #   Nullius  TRUE    and FALSE   -> FALSE    (a VALUED cell excluded by
        #                                             the unprojected conjunct;
        #                                             push and recheck agree, so
        #                                             this one pins the push)
        connection.execute(insert(students).values(name="Nullius", id=1500))

        # `name` is projected AND read by the predicate; `id` is read only by
        # the predicate. The push returns {Newton, Kepler}.
        rows = connection.execute(
            select(students.c.name)
            .where(students.c.name != "Galileo Galilei")
            .where(students.c.id != 1500)
        ).fetchall()

    # (1) The rows. Reds by ASSERTION on today's `set()`, not by exception --
    # the unwidened column fails silently, one stage before any decode could
    # raise (the handoff's section 3.2, and the ADR-0022 Consequences bullet
    # that is measured wrong).
    assert {row.name for row in rows} == {"Isaac Newton"}

    # (2) The trim, over a widening whose predicate columns partly overlap the
    # projection: `id` must reach the Scan and must not reach the Row, while
    # `name` -- fetched for BOTH reasons -- must survive the trim exactly once.
    assert [list(row.keys()) for row in rows] == [["name"]]
