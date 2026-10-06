import pytest

from normlite.engine.base import Engine
from normlite.sql.compiler import _depth
from normlite.sql.dml import insert, select
from normlite.sql.elements import not_
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


def test_a_blank_text_cell_is_kept_by_a_not_equal_where(engine: Engine):
    # The text twin of the test above, with the opposite expected result (#383).
    #
    # A blank text cell is a PRESENT value, not NULL: it decodes to "" (CONTEXT.md,
    # "Raw cell <=> decoded NULL"). Notion stores no valueless text cell. So SQL
    # evaluates `'' <> 'A'` to TRUE, eval3 agrees, and the row belongs in the answer.
    #
    # Measured against the live API (2026-10-04, `notion_probe.py query`):
    # `title.does_not_equal(lit)` matches the blank `[]` cells, and
    # `title.is_not_empty` excludes them. A pushed `!=` that conjoins
    # `is_not_empty` therefore removes a row SQL keeps. The recheck cannot add it
    # back: a row the push never returned is a row the recheck never sees.
    #
    # The in-memory client stores `values(grade="")` verbatim, which the live API
    # normalises to `[]`. Both are present blank text, and both decode to "".
    metadata = MetaData()
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("grade", String()),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        # Galileo is the discriminator: a valued cell the predicate must exclude.
        connection.execute(insert(students).values(name="Galileo Galilei", grade="A"))
        # Newton is the valued control the predicate must keep.
        connection.execute(insert(students).values(name="Isaac Newton", grade="B"))
        # Blanca is the defect: a blank cell the predicate must keep.
        connection.execute(insert(students).values(name="Blanca", grade=""))

        rows = connection.execute(
            select(students).where(students.c.grade != "A")
        ).fetchall()

    assert {row.name for row in rows} == {"Isaac Newton", "Blanca"}


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


def test_a_disjunction_with_a_negated_arm_pushes_nothing_and_still_answers(
    engine: Engine,
):
    # RED for #383, the bind leak: `WHERE name = 'Galileo' OR NOT (grade = 'A')`.
    #
    # The `or` is unpushable WHOLE -- one of its arms is a `not`, and dropping a
    # disjunct NARROWS the result, which is the one direction ADR-0022 forbids
    # (a push may only over-keep; the recheck can drop rows, never resurrect
    # them). So nothing at all may reach `payload['filter']` and the whole
    # predicate is answered client-side by eval3.
    #
    # `visit_boolean_clause_list` dispatches every child BEFORE it decides
    # whether the node is pushable, and dispatch registers a bind as a side
    # effect. When the `or` branch then discards its JSON, the binds stay
    # registered and unreferenced, and `_assert_all_params_consumed`
    # (engine/context.py) raises at EXECUTION -- which is why this is an
    # execution test and not a compile test. `visit_unary_expression` already
    # obeys the opposite discipline: it returns None WITHOUT dispatching its
    # inner element, which is exactly why the `and` path is clean today.
    #
    # Three rows, each excluded or kept by a different mechanism:
    metadata = MetaData()
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("grade", String()),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        #   Galileo  TRUE  or FALSE -> TRUE   (only the LEFT arm keeps him)
        connection.execute(insert(students).values(name="Galileo", grade="A"))
        #   Kepler   FALSE or TRUE  -> TRUE   (only the NEGATED arm keeps him:
        #                                      the row a "drop the unpushable
        #                                      disjunct" fix silently loses,
        #                                      because Notion would then return
        #                                      Galileo alone and the recheck
        #                                      cannot resurrect Kepler)
        connection.execute(insert(students).values(name="Kepler", grade="B"))
        #   Newton   FALSE or FALSE -> FALSE  (the discriminator against
        #                                      "pushed nothing, so keep
        #                                      everything")
        connection.execute(insert(students).values(name="Newton", grade="A"))

        rows = connection.execute(
            select(students.c.name).where(
                (students.c.name == "Galileo") | not_(students.c.grade == "A")
            )
        ).fetchall()

    assert {row.name for row in rows} == {"Galileo", "Kepler"}


def test_a_pruned_select_executes_and_the_recheck_narrows_the_superset(
    engine: Engine, monkeypatch
):
    # The EXECUTION-level red for #383's SELECT half. Its siblings
    # `test_compiler_axioms.test_repaired_ne_under_an_or_is_pruned_to_a_legal_depth`
    # and `..test_user_written_and_or_and_is_pruned_to_a_legal_depth` pin the
    # emitted dict, and both are compile-level by necessity: only the real API
    # observes the nesting cap. But a compile-level green says nothing about
    # whether a pruned query can RUN, and measured on `e26d7af` with the scan
    # site temporarily wired, it cannot:
    #
    #   ArgumentError: Unused bind parameters in parameter set 0: param_3
    #
    # The prune removes a leaf AFTER emission registered its bind (decision L,
    # emit-then-measure), so the placeholder never reaches the payload template,
    # `_bind_params` never pops the key, and `_assert_all_params_consumed`
    # (engine/context.py) reads the hole as a compiler bug. It is not one. The
    # bind is consumed by the RECHECK: `eval3` reads the value off the AST's own
    # BindParameter (`predicate.value.effective_value`), never off
    # `execution_binds`. A SELECT holding a `recheck_where` is a second consumer,
    # exactly as UPDATE is, which is why UPDATE is the one statement the assert
    # already exempts.
    #
    # Three states, and the test reads differently in each:
    #   prune unwired          -> RED at the depth assertion (a depth-3 filter
    #                             reaches the client; the answer is right, the
    #                             payload is illegal)
    #   prune wired, no C      -> RED at `connection.execute`, ArgumentError
    #   prune wired, C landed  -> GREEN, and the rows prove the recheck decides
    #
    # The predicate, no `!=` anywhere so the leaf repair is not involved:
    #
    #   WHERE id < 1000 AND (grade = 'Z' OR (id > 5 AND is_active))
    #
    # Depth 3. The over-deep node sits under an `or`, so the prune replaces it
    # with its first child and `is_active` is the leaf that goes -- taking
    # `:param_N` with it. That is the bind hole, and it is the whole point of
    # running the query rather than reading its dict.
    #
    # This test does NOT pin which prune shipped -- reds 1 and 2 own that. It
    # pins three weaker things the dicts cannot see: a legal filter reaches the
    # client, the statement executes, and the answer is SQL's. The third is not
    # free: it still catches the UNSOUND prune. Dropping a DISJUNCT of the `or`
    # leaves `id < 1000 AND grade = 'Z'`, which measures 2 and passes the depth
    # assertion, and no recheck can add back a row the push never fetched -- so
    # `name_6..name_9` go missing and the row assertion fails.
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
        # Six rows the predicate excludes (id <= 5) and four it keeps (id 6..9),
        # all active and all grade 'A'. Without the keepers a prune that pushed
        # nothing would be indistinguishable from one that worked.
        for i in range(10):
            connection.execute(insert(students).values(
                name=f"name_{i}", id=i, is_active=True, grade="A",
            ))
        # Xavier is the DISCRIMINATOR, and he is the reason the cast is not just
        # a row count: the pruned push ADMITS him (id > 5) and the predicate
        # EXCLUDES him (not active). He comes back from Notion and the recheck
        # must drop him. A wiring that pruned and then trusted the push keeps
        # him.
        connection.execute(insert(students).values(
            name="Xavier", id=7, is_active=False, grade="Y",
        ))
        # Zeta is the control on the surviving disjunct: the predicate keeps her
        # through `grade = 'Z'` alone, so a prune that dropped that disjunct --
        # or an over-eager recheck that dropped everything -- fails here.
        connection.execute(insert(students).values(
            name="Zeta", id=0, is_active=False, grade="Z",
        ))

        pushed_filters = []
        query = engine._client.data_sources_query

        def spy(path_params=None, query_params=None, payload=None):
            pushed_filters.append(payload.get("filter"))
            return query(
                path_params=path_params,
                query_params=query_params,
                payload=payload,
            )

        # The payload that actually reached the client, not a twin compiled
        # beside it: what the compiler emits and what the scan sends are the
        # same object only as long as nothing between them edits it, and this
        # slice edits filters.
        monkeypatch.setattr(engine._client, "data_sources_query", spy)

        rows = connection.execute(
            select(students.c.name).where(
                (students.c.id < 1000)
                & (
                    (students.c.grade == "Z")
                    | ((students.c.id > 5) & (students.c.is_active == True))
                )
            )
        ).fetchall()

    # Depth first, on every page fetched: it fails as one number instead of a
    # wall-of-dict diff, and a pruned SELECT that reached the client with a
    # depth-3 filter is an HTTP 400 against the real API whatever rows the
    # in-memory client returned.
    assert [_depth(f) for f in pushed_filters] == [2], (
        f"filters pushed to the client exceed Notion's 2-level cap: {pushed_filters}"
    )

    assert {row.name for row in rows} == {
        "name_6", "name_7", "name_8", "name_9", "Zeta",
    }, (
        "the pruned push is a SUPERSET and the recheck decides (ADR-0022): "
        "Xavier is fetched and must be dropped, Zeta is kept by the disjunct "
        f"the prune must not touch -- got {sorted(row.name for row in rows)}"
    )


def test_a_none_literal_under_not_equal_is_not_pushed(engine: Engine, monkeypatch):
    # RED for #383, option 1: a `None` literal under `!=` is unpushable.
    #
    # The compiler emits `grade != None` as a bare `rich_text.does_not_equal`
    # leaf, and the bind value `None` puts `null` on the wire. Measured against
    # the live API (2026-10-05, `notion_probe.py query`): a `null` literal under
    # `equals` or `does_not_equal` is an HTTP 400 `validation_error`, for
    # `title` and for `number` ("... should be a string, instead was `null`").
    # Notion does NOT ignore it, as it ignores `""` (#382). The request fails.
    #
    # The in-memory client accepts `null`, so the rows cannot show the defect.
    # Only the payload that reached the client shows it. This test pins the
    # payload and NOT the rows: what the recheck answers for `grade != None`
    # is eval3's decision (ADR-0022), not this fix.
    #
    # A lone leaf that is unpushable leaves nothing to fold. The fold returns
    # `{}`, and the scan site does not assign it. So the expected payload has
    # no "filter" key at all.
    metadata = MetaData()
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("grade", String()),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        connection.execute(insert(students).values(name="Galileo Galilei", grade="A"))

        payloads = []
        query = engine._client.data_sources_query

        def spy(path_params=None, query_params=None, payload=None):
            payloads.append(payload)
            return query(
                path_params=path_params,
                query_params=query_params,
                payload=payload,
            )

        monkeypatch.setattr(engine._client, "data_sources_query", spy)

        connection.execute(
            select(students).where(students.c.grade != None)  # noqa: E711
        ).fetchall()

    assert [p.get("filter") for p in payloads] == [None], (
        "a `null` literal under `does_not_equal` is an HTTP 400 against the "
        f"live API; no filter may reach the client -- got {payloads}"
    )


def test_a_none_literal_under_equal_is_not_pushed(engine: Engine, monkeypatch):
    # The `==` twin of `test_a_none_literal_under_not_equal_is_not_pushed` (#383).
    #
    # `grade == None` compiles to a bare `rich_text.equals` leaf with `null` on
    # the wire. The live API answers HTTP 400 for `equals` exactly as for
    # `does_not_equal` (measured 2026-10-05, title and number). The sibling test
    # pins only `!=`, so a rule that checked `Operator.NE` alone passed the full
    # suite. Same cast, same spy, same expected payload: no "filter" key.
    metadata = MetaData()
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("grade", String()),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        connection.execute(insert(students).values(name="Galileo Galilei", grade="A"))

        payloads = []
        query = engine._client.data_sources_query

        def spy(path_params=None, query_params=None, payload=None):
            payloads.append(payload)
            return query(
                path_params=path_params,
                query_params=query_params,
                payload=payload,
            )

        monkeypatch.setattr(engine._client, "data_sources_query", spy)

        connection.execute(
            select(students).where(students.c.grade == None)  # noqa: E711
        ).fetchall()

    assert [p.get("filter") for p in payloads] == [None], (
        "a `null` literal under `equals` is an HTTP 400 against the live API; "
        f"no filter may reach the client -- got {payloads}"
    )
