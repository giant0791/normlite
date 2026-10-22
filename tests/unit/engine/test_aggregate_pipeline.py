from decimal import Decimal

import pytest

from normlite.exceptions import CompileError
from normlite.sql.elements import not_
from normlite.sql.functions import func
from normlite.engine.base import Engine
from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.sql.compiler import _depth
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
# #383, #399 -- an aggregate WHERE: the push and the recheck.
#
# Every test above this line runs an aggregate with NO WHERE clause. That is
# why a #383 depth gate that refused EVERY aggregate WHERE (including a depth-0
# leaf) once shipped with the whole suite green. #399 gave the aggregate a
# recheck and removed the gate. The tests below pin what is pushed and what
# the recheck keeps.
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
    # The aggregate rechecks its WHERE (#399), so the count alone cannot show
    # what was pushed: a dropped push and a full push give the same count.
    # The sink can. This test pins two facts.
    #
    # 1. A legal WHERE is pushed. The push is a hint that narrows the fetch
    #    (ADR-0022). If it is dropped, the answer stays correct, but the Scan
    #    fetches every page of the data source.
    # 2. A legal WHERE reaches the client unchanged, byte for byte. The prune
    #    must change only a filter that is too deep. The exact dicts below make
    #    "unchanged" an assertion: no count can see the difference.
    #
    # Each WHERE here reads `id`, not the operand `name`. So the count also
    # proves that the Scan fetches the columns the recheck reads.
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
            "depth 0: the recheck must read `id` and keep five rows -- "
            f"got {rows[0]['count']}"
        )

        # -- depth 1: one flat compound ------------------------------------
        # id > 5 AND grade = 'A' drops Xavier on the grade term. Four rows. The
        # dict proves that BOTH conjuncts crossed the wire; the count proves that
        # the recheck applies both.
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
            "depth 1: the recheck must apply both conjuncts -- without the grade "
            f"term Xavier stays, got {rows[0]['count']}"
        )

        # -- depth 2: the boundary, at the cap and legal --------------------
        # id < 1000 AND (grade = 'Z' OR id > 5) keeps name_6..name_9, Xavier on
        # the id disjunct and Zeta on the grade disjunct. Six rows. The dict
        # proves that the nested `or` crossed the wire whole; the count proves
        # that the recheck keeps Zeta on the grade disjunct.
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
            "depth 2: the recheck must apply both disjuncts -- without the grade "
            f"disjunct Zeta drops out, got {rows[0]['count']}"
        )


def test_an_aggregate_prunes_an_over_deep_where_and_rechecks_it(
    engine: Engine, monkeypatch
):
    # #399 removes the depth gate. The aggregate now has a recheck, so it
    # prunes an over-deep WHERE as a plain SELECT does.
    #
    #   WHERE id < 1000 AND (grade = 'Z' OR (id > 5 AND is_active))
    #
    # Depth 3, no `!=`, so the leaf repair is not involved. The prune replaces
    # the over-deep node under the `or` by its first child, so `is_active` goes.
    # The pruned push keeps Xavier: he has `id > 5` and he is not active.
    # A push without a recheck counts 6. The recheck drops Xavier: 5.
    #
    # The plain SELECT is the reference. It prunes and rechecks the same WHERE.
    # A fresh expression for each statement: a reused one raises
    # "BindParameter ... already has the role COLUMN_FILTER".
    students = _students(engine)
    over_deep = lambda: (
        (students.c.id < 1000)
        & (
            (students.c.grade == "Z")
            | ((students.c.id > 5) & (students.c.is_active == True))
        )
    )

    with engine.connect() as connection:
        _seed(connection, students)
        sink: list = []
        _spy_on_pushed_filters(engine, monkeypatch, sink)

        counted = connection.execute(
            select(func.count(students.c.name)).where(over_deep())
        ).fetchall()
        pushed = list(sink)

        plain = connection.execute(
            select(students.c.name).where(over_deep())
        ).fetchall()

    # One filter reached the client, pruned to a depth Notion accepts.
    assert [_depth(f) for f in pushed] == [2], (
        f"the over-deep WHERE must be pruned to depth 2 and pushed, got {pushed}"
    )
    # The recheck decides (ADR-0022): Xavier drops out, and the count agrees
    # with the plain SELECT.
    assert counted[0]["count"] == len(plain) == 5, (
        "the recheck must drop Xavier: expected 5 like the plain SELECT, "
        f"got count {counted[0]['count']} and plain {len(plain)}"
    )


def test_an_aggregate_counts_only_the_rows_an_unpushable_where_keeps(
    engine: Engine,
):
    # Arrange: the twelve-row cast. A NOT is unpushable, so no filter
    # reaches Notion. The client-side evaluation must decide (ADR-0022).
    students = _students(engine)
    where = lambda: not_(students.c.name == "name_0")

    with engine.connect() as connection:
        _seed(connection, students)

        # Act: count with the WHERE, and select the plain rows with the same WHERE
        counted = connection.execute(
            select(func.count(students.c.name)).where(where())
        ).fetchall()
        kept = connection.execute(
            select(students.c.name).where(where())
        ).fetchall()

    # Assert: one row, and its count agrees with the plain SELECT.
    # Only name_0 drops out.
    assert len(counted) == 1
    assert counted[0]["count"] == len(kept) == 11
