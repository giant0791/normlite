import re
from decimal import Decimal

import pytest

from normlite.exceptions import CompileError
from normlite.sql.functions import func
from normlite.engine.base import Engine
from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.sql.compiler import _FILTER_MAX_DEPTH_EXCEEDED, _depth
from normlite.sql.dml import insert, select
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Integer, Numeric, String


def test_sum_returns_one_row_with_the_total_of_the_matched_values(engine: Engine):
    # Arrange: a table seeded with three numeric rows
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("team", String(is_title=True)),
        Column("headcount", Integer()),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        for team, headcount in (("Alpha", 5), ("Bravo", 10), ("Cosmos", 3)):
            connection.execute(
                insert(accounts).values(team=team, headcount=headcount)
            )

        # Act: sum the matched values end-to-end (compile -> databases.query ->
        # drain -> reduce -> result cursor -> result_processor)
        result = connection.execute(select(func.sum(accounts.c.headcount)))
        rows = result.fetchall()

    # Assert: exactly one synthetic row carrying the total as a final int value
    assert len(rows) == 1
    assert rows[0]["sum"] == 18


def test_two_aggregates_over_the_same_column_each_get_their_own_value(engine: Engine):
    # Arrange: seed numeric rows
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("team", String(is_title=True)),
        Column("headcount", Integer()),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        for team, headcount in (("Alpha", 5), ("Bravo", 10), ("Cosmos", 3)):
            connection.execute(
                insert(accounts).values(team=team, headcount=headcount)
            )

        # Act: two aggregates over the SAME operand column. Whether the drained
        # row carries one shared headcount cell or two depends on how the query
        # projects duplicate operands -- this is the case a unit test can't reach.
        result = connection.execute(
            select(
                func.sum(accounts.c.headcount),
                func.avg(accounts.c.headcount),
            )
        )
        rows = result.fetchall()

    # Assert: one synthetic row, each aggregate resolved to the same operand
    assert len(rows) == 1
    assert rows[0]["sum"] == 18
    assert rows[0]["avg"] == 6.0


def test_avg_final_value_is_a_python_float(engine: Engine):
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("team", String(is_title=True)),
        Column("headcount", Integer()),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        for team, headcount in (("Alpha", 5), ("Bravo", 10)):
            connection.execute(
                insert(accounts).values(team=team, headcount=headcount)
            )

        result = connection.execute(select(func.avg(accounts.c.headcount)))
        rows = result.fetchall()

    # the slice's whole reason for the Float return type: avg surfaces as a real
    # Python float end-to-end, not a Decimal (which == 7.5 but is the wrong type)
    assert rows[0]["avg"] == 7.5
    assert isinstance(rows[0]["avg"], float)


def test_labeled_and_unlabeled_aggregates_surface_under_their_result_keys(engine: Engine):
    # The ADR-0011 headline: a labeled sum beside an unlabeled avg over the same
    # operand column, in one whole-set aggregate select.
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("team", String(is_title=True)),
        Column("headcount", Integer()),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        for team, headcount in (("Alpha", 5), ("Bravo", 10), ("Cosmos", 3)):
            connection.execute(
                insert(accounts).values(team=team, headcount=headcount)
            )

        result = connection.execute(
            select(
                func.sum(accounts.c.headcount).label("total_headcount"),
                func.avg(accounts.c.headcount),
            )
        )
        rows = result.fetchall()

    # one synthetic row; the labeled sum is fetchable under its custom key, the
    # avg under its auto function-name key
    assert len(rows) == 1
    assert rows[0]["total_headcount"] == 18
    assert rows[0]["avg"] == 6.0


def test_two_sums_over_different_columns_get_disambiguated_result_keys(engine: Engine):
    # Two func.sum() over DIFFERENT columns collide on the bare "sum" key; the
    # aggregate schema disambiguates them positionally to sum_1 / sum_2. Only the
    # full pipeline exercises how duplicate function names are keyed for fetch.
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("team", String(is_title=True)),
        Column("headcount", Integer()),
        Column("budget", Integer()),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        for team, headcount, budget in (("Alpha", 5, 100), ("Bravo", 10, 200)):
            connection.execute(
                insert(accounts).values(team=team, headcount=headcount, budget=budget)
            )

        result = connection.execute(
            select(
                func.sum(accounts.c.headcount),
                func.sum(accounts.c.budget),
            )
        )
        rows = result.fetchall()

    # one synthetic row; first sum -> sum_1 (headcount), second -> sum_2 (budget)
    assert len(rows) == 1
    assert rows[0]["sum_1"] == 15
    assert rows[0]["sum_2"] == 300


def test_sum_over_a_numeric_column_surfaces_as_a_decimal(engine: Engine):
    # sum preserves the operand's numeric type: over a Numeric (Decimal) column
    # the total must round-trip back to a Python Decimal end-to-end, not collapse
    # to int/float the way an Integer column would.
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("team", String(is_title=True)),
        Column("balance", Numeric()),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        for team, balance in (("Alpha", Decimal("5.50")), ("Bravo", Decimal("10.25"))):
            connection.execute(
                insert(accounts).values(team=team, balance=balance)
            )

        result = connection.execute(select(func.sum(accounts.c.balance)))
        rows = result.fetchall()

    assert len(rows) == 1
    assert rows[0]["sum"] == Decimal("15.75")
    assert isinstance(rows[0]["sum"], Decimal)


def test_columnless_count_star_counts_every_matched_row(engine: Engine):
    # func.count() with no operand is SQL COUNT(*). It has no column to anchor
    # FROM, so select_from() supplies the table explicitly; the count is every
    # matched row end-to-end.
    metadata = MetaData()
    employees = Table(
        "employees",
        metadata,
        Column("name", String(is_title=True)),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        for name in ("Galileo Galilei", "Isaac Newton", "Marie Curie"):
            connection.execute(insert(employees).values(name=name))

        result = connection.execute(select(func.count()).select_from(employees))
        rows = result.fetchall()

    assert len(rows) == 1
    assert rows[0]["count"] == 3


def test_bare_columnless_count_without_select_from_fails_loud(engine: Engine):
    # A columnless COUNT(*) has no operand to infer FROM; without select_from()
    # the table stays unresolved (_table is None). This must fail loud with a
    # clear CompileError, not an opaque AttributeError deep in the compiler
    # (None.get_oid()). No table is even needed: the guard fires on the missing
    # anchor before any backend lookup.
    with engine.connect() as connection:
        with pytest.raises(CompileError):
            connection.execute(select(func.count()))


def test_count_returns_one_row_with_the_number_of_matched_rows(engine: Engine):
    # Arrange: a table seeded with three rows. The aggregate's column operand
    # (employees.c.name) is what anchors the query to the employees table.
    metadata = MetaData()
    employees = Table(
        "employees",
        metadata,
        Column("name", String(is_title=True)),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        for name in ("Galileo Galilei", "Isaac Newton", "Marie Curie"):
            connection.execute(insert(employees).values(name=name))

        # Act: count the matched rows
        result = connection.execute(select(func.count(employees.c.name)))
        rows = result.fetchall()

    # Assert: exactly one synthetic row carrying the count
    assert len(rows) == 1
    assert rows[0]["count"] == 3


# ---------------------------------------------------------------------------
# #383 -- the aggregate depth gate.
#
# Every test above this line runs an aggregate with NO WHERE clause, which is
# why a gate that refused EVERY aggregate WHERE (including a depth-0 leaf) once
# shipped with the whole suite green. These two close that hole.
#
# The cast is the execution red's cast (test_select_pipeline.py), deliberately:
# the same twelve rows make the aggregate and the plain SELECT comparable, and
# Xavier and Zeta are already the discriminators there.
# ---------------------------------------------------------------------------

def _students(engine: Engine) -> Table:
    # A helper, against this file's habit of inlining the table, because the
    # seed below is twelve rows and three copies of it would bury the assertions.
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
    return students


def _seed(connection, students: Table) -> None:
    # Ten rows the predicates split on id, all active and all grade 'A'.
    for i in range(10):
        connection.execute(insert(students).values(
            name=f"name_{i}", id=i, is_active=True, grade="A",
        ))
    # Xavier: id > 5 but NOT active and grade 'Y'. He is what separates a filter
    # that carries every term from one that lost one -- a push that dropped the
    # grade term or the is_active term still admits him.
    connection.execute(insert(students).values(
        name="Xavier", id=7, is_active=False, grade="Y",
    ))
    # Zeta: id 0, so only the `grade = 'Z'` disjunct can keep her. A push that
    # lost a DISJUNCT loses Zeta, and the count drops instead of rising.
    connection.execute(insert(students).values(
        name="Zeta", id=0, is_active=False, grade="Z",
    ))


def _spy_on_pushed_filters(engine: Engine, monkeypatch, sink: list) -> None:
    # The payload that actually reached the client, not a twin compiled beside
    # it: this slice edits filters between emission and the wire, so a compiled
    # dict proves nothing about what was sent.
    query = engine._client.data_sources_query

    def spy(path_params=None, query_params=None, payload=None):
        sink.append(payload.get("filter"))
        return query(
            path_params=path_params,
            query_params=query_params,
            payload=payload,
        )

    monkeypatch.setattr(engine._client, "data_sources_query", spy)


def test_an_aggregate_pushes_a_legal_filter_unchanged_at_every_allowed_depth(
    engine: Engine, monkeypatch
):
    # THE GREEN. `Planner.plan` returns at queryplan.py:786 for an aggregate and
    # builds no Filter, so on that branch the push IS the answer: nothing
    # re-narrows it. Two consequences, and this test pins both.
    #
    # 1. A legal WHERE must still be pushed. The depth gate fences the OVER-DEEP
    #    case only, and "only" is the whole content of the assertion: a gate that
    #    refused every aggregate WHERE passed all 948 other tests.
    # 2. What is pushed must be what emission produced, byte for byte. The plain
    #    SELECT path prunes, because it has a recheck to restore the answer; the
    #    aggregate has none, so a pruned push would be a silent over-count. The
    #    exact dicts below are what makes "unchanged" an assertion rather than a
    #    hope -- `_prune` happens to be the identity over every input this branch
    #    can receive, so no behavioural assertion can see the difference. The
    #    dict can.
    #
    # Three depths, derived by hand from the fixture's type map (name->title,
    # id->number, is_active->checkbox, grade->rich_text) and confirmed against a
    # run afterwards -- never snapshotted. Values are the RESOLVED literals: the
    # spy sits downstream of bind resolution, so `:param_N` holes are already
    # filled by the time the payload is seen.
    #
    # Depth 0 and 1 are the regression rows. Depth 2 is the BOUNDARY: it is the
    # only one that dies if the comparison is written `>=` instead of `>`.
    students = _students(engine)

    with engine.connect() as connection:
        _seed(connection, students)
        sink: list = []
        _spy_on_pushed_filters(engine, monkeypatch, sink)

        # -- depth 0: a bare leaf, no compound at all ----------------------
        # id > 5 keeps name_6..name_9 and Xavier (id 7). Five rows.
        del sink[:]
        rows = connection.execute(
            select(func.count(students.c.name)).where(students.c.id > 5)
        ).fetchall()

        assert [_depth(f) for f in sink] == [0], (
            f"depth 0: a bare leaf must reach the client unchanged, got {sink}"
        )
        assert sink == [
            {"property": "id", "number": {"greater_than": 5}},
        ], f"depth 0: emitted filter altered on the way to the client: {sink}"
        assert rows[0]["count"] == 5, (
            "depth 0: a legal aggregate WHERE must be pushed, not refused -- "
            f"got {rows[0]['count']}"
        )

        # -- depth 1: one flat compound ------------------------------------
        # id > 5 AND grade = 'A' drops Xavier on the grade term. Four rows, and
        # the drop is what proves BOTH conjuncts crossed the wire.
        del sink[:]
        rows = connection.execute(
            select(func.count(students.c.name)).where(
                (students.c.id > 5) & (students.c.grade == "A")
            )
        ).fetchall()

        assert [_depth(f) for f in sink] == [1], (
            f"depth 1: a flat compound must reach the client unchanged, got {sink}"
        )
        assert sink == [
            {"and": [
                {"property": "id", "number": {"greater_than": 5}},
                {"property": "grade", "rich_text": {"equals": "A"}},
            ]},
        ], f"depth 1: emitted filter altered on the way to the client: {sink}"
        assert rows[0]["count"] == 4, (
            "depth 1: both conjuncts must be pushed -- dropping the grade term "
            f"readmits Xavier, got {rows[0]['count']}"
        )

        # -- depth 2: the boundary, at the cap and legal --------------------
        # id < 1000 AND (grade = 'Z' OR id > 5) keeps name_6..name_9, Xavier on
        # the id disjunct and Zeta on the grade disjunct. Six rows: Zeta is in
        # the answer only if the nested `or` survived intact.
        del sink[:]
        rows = connection.execute(
            select(func.count(students.c.name)).where(
                (students.c.id < 1000)
                & ((students.c.grade == "Z") | (students.c.id > 5))
            )
        ).fetchall()

        assert [_depth(f) for f in sink] == [2], (
            f"depth 2 is AT the cap and legal, not over it, got {sink}"
        )
        assert sink == [
            {"and": [
                {"property": "id", "number": {"less_than": 1000}},
                {"or": [
                    {"property": "grade", "rich_text": {"equals": "Z"}},
                    {"property": "id", "number": {"greater_than": 5}},
                ]},
            ]},
        ], f"depth 2: emitted filter altered on the way to the client: {sink}"
        assert rows[0]["count"] == 6, (
            "depth 2: the nested `or` must cross the wire whole -- losing a "
            f"disjunct loses Zeta, got {rows[0]['count']}"
        )


def test_an_aggregate_refuses_an_over_deep_where_before_fetching_a_page(
    engine: Engine, monkeypatch
):
    # THE RED. The gate's reason to exist, and the one shape it may refuse.
    #
    #   WHERE id < 1000 AND (grade = 'Z' OR (id > 5 AND is_active))
    #
    # Depth 3, no `!=`, so the leaf repair is not involved. A plain SELECT prunes
    # this to depth 2 -- the over-deep node sits under an `or`, so it is replaced
    # by its first child and `is_active` goes -- and then its Filter re-applies
    # the whole predicate, so the answer is SQL's. The aggregate has no Filter.
    # It would reduce the pruned SUPERSET: measured at 6 against SQL's 5, because
    # Xavier satisfies `id > 5` and the term that excludes him is the one pruned
    # away. Silent, and the bind assert is no backstop -- the compiler sets
    # `recheck_where` on this path even though the planner ignores it, so
    # decision U forgives the orphaned `is_active` bind and the query runs clean.
    #
    # `match=` reads the reason constant the depth helper owns, never the
    # consequence or the remedy: the reason is the contract, the rest of the
    # prose is not. It tells this refusal from the unpushable-term refusal.
    #
    # The empty-sink assertion is not decoration. It pins WHERE the refusal
    # lives: a check moved into the Aggregate operator would also raise, but only
    # after draining the superset it was supposed to prevent. Refusing at compile
    # time means no page is ever fetched.
    students = _students(engine)

    with engine.connect() as connection:
        _seed(connection, students)
        sink: list = []

        over_deep = (
            (students.c.id < 1000)
            & (
                (students.c.grade == "Z")
                | ((students.c.id > 5) & (students.c.is_active == True))
            )
        )

        _spy_on_pushed_filters(engine, monkeypatch, sink)

        with pytest.raises(CompileError, match=re.escape(_FILTER_MAX_DEPTH_EXCEEDED)):
            connection.execute(select(func.count(students.c.name)).where(over_deep))

        assert sink == [], (
            "the refusal must happen at compile time, before any page is "
            f"fetched -- the client was queried with {sink}"
        )

        # The remedy the error message offers, verified rather than asserted in
        # prose: a plain SELECT over the same predicate DOES recheck, so reducing
        # its rows client-side gives the answer the aggregate declined to give.
        # A fresh expression object -- reusing `over_deep` raises
        # "BindParameter role already assigned".
        plain = connection.execute(
            select(students.c.name).where(
                (students.c.id < 1000)
                & (
                    (students.c.grade == "Z")
                    | ((students.c.id > 5) & (students.c.is_active == True))
                )
            )
        ).fetchall()

    assert len(plain) == 5, (
        "the remedy must work: the plain SELECT rechecks and drops Xavier, so "
        f"client-side reduction answers 5 -- got {len(plain)}"
    )
