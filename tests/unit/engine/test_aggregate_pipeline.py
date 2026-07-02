from decimal import Decimal

import pytest

from normlite.exceptions import CompileError
from normlite.sql.functions import func
from normlite.engine.base import Engine
from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.sql.dml import insert, select
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Integer, Numeric, String


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
