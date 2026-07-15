import uuid

import pytest

from normlite.exceptions import ArgumentError
from normlite.sql.compiler import NotionCompiler
from normlite.sql.dml import AggregateExecution, select
from normlite.sql.functions import func
from normlite.sql.resultschema import SchemaInfo
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Float, Integer, Numeric, String


def test_count_of_a_column_is_an_integer_aggregate_anchored_to_its_table():
    metadata = MetaData()
    employees = Table(
        "employees",
        metadata,
        Column("name", String(is_title=True)),
    )

    # Act: build the aggregate over a column
    agg = func.count(employees.c.name)

    # Assert: count yields an integer, regardless of the operand's own type...
    assert isinstance(agg.type_, Integer)
    # ...and it carries the operand so the source table is reachable.
    assert agg.column.parent is employees


def test_select_from_anchors_a_columnless_count_star_to_its_table():
    metadata = MetaData()
    employees = Table(
        "employees",
        metadata,
        Column("name", String(is_title=True)),
    )

    # A columnless COUNT(*) has no operand to anchor FROM, so building the select
    # alone leaves the table unresolved -- rather than crashing on a None operand.
    stmt = select(func.count())
    assert stmt._table is None

    # select_from() supplies the FROM explicitly (SQLAlchemy-style). It is
    # @generative, so it returns a new statement carrying the anchor.
    anchored = stmt.select_from(employees)
    assert anchored._table is employees


def test_select_from_is_rejected_on_a_non_aggregate_select():
    metadata = MetaData()
    employees = Table(
        "employees",
        metadata,
        Column("name", String(is_title=True)),
    )

    # select_from() exists in v1 ONLY to anchor a columnless aggregate (COUNT(*)).
    # On a plain column/table select it must fail loud rather than silently unlock
    # untested explicit-FROM / join constructs (see project_select_from_boundary).
    with pytest.raises(ArgumentError):
        select(employees.c.name).select_from(employees)


def test_from_aggregate_describes_one_provenance_free_column_keyed_by_function_name():
    metadata = MetaData()
    employees = Table(
        "employees",
        metadata,
        Column("name", String(is_title=True)),
    )

    # Act: build the result schema for a single aggregate
    schema = SchemaInfo.from_aggregate(func.count(employees.c.name))

    # Assert: exactly one column...
    assert len(schema.columns) == 1
    result_col = schema.columns[0]
    # ...keyed by the function name, NOT the operand column's name...
    assert result_col.name == "count"
    # ...and provenance-free: an aggregate has no owning table.
    assert result_col.table is None


def test_reduce_folds_matched_rows_into_a_single_synthetic_count_row():
    metadata = MetaData()
    employees = Table(
        "employees",
        metadata,
        Column("name", String(is_title=True)),
    )
    projection = (func.count(employees.c.name),)

    # operand cells arrive as {type: value} dicts off ResultSet._process_page,
    # keyed by the operand column's own on-wire type ("title" here) — never a
    # bare Python value. All three are non-empty.
    matched_rows = [({"title": "Galileo"},), ({"title": "Isaac"},), ({"title": "Marie"},)]

    schema, rows = AggregateExecution(projection).reduce(matched_rows)

    # exactly one synthetic row...
    assert len(rows) == 1
    # ...carrying the count as a final value at the "count" position
    count_idx = schema.column_index("count")
    assert rows[0][count_idx] == {"number": 3}


def test_reduce_count_skips_present_but_null_operand_cells():
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("title", String(is_title=True)),
        Column("headcount", Integer()),
    )
    projection = (func.count(accounts.c.headcount),)

    # count(col) is COUNT(column): non-empty cells only (CONTEXT.md), NOT the row
    # count. The middle row's headcount is present-but-null ({"number": None}) —
    # the SQL-NULL shape _process_page emits — so it must not be counted.
    matched_rows = [({"number": 5},), ({"number": None},), ({"number": 3},)]

    schema, rows = AggregateExecution(projection).reduce(matched_rows)

    # two non-empty cells, not three rows
    count_idx = schema.column_index("count")
    assert rows[0][count_idx] == {"number": 2}


def test_reduce_count_over_a_non_number_column_counts_non_empty_cells():
    metadata = MetaData()
    employees = Table(
        "employees",
        metadata,
        Column("name", String(is_title=True)),
    )
    projection = (func.count(employees.c.name),)

    # count accepts any type (CONTEXT.md). Non-empty checking must read the
    # operand column's own key ("title"), not count's numeric return key — the
    # middle row is an absent property (None cell) and must be skipped.
    matched_rows = [({"title": "Ada"},), (None,), ({"title": "Alan"},)]

    schema, rows = AggregateExecution(projection).reduce(matched_rows)

    # two non-empty titles; the result is still a numeric cell because count
    # returns Integer regardless of the operand column's type
    count_idx = schema.column_index("count")
    assert rows[0][count_idx] == {"number": 2}


def test_reduce_columnless_count_folds_to_the_matched_row_count():
    # func.count() with NO column is SQL COUNT(*): it counts every matched row
    # regardless of column values (empties included) -- unlike count(col), which
    # counts only non-empty cells. There is no operand column to position against.
    projection = (func.count(),)

    # three matched rows; the middle one is entirely empty (None) yet still counts
    matched_rows = [({"number": 5},), (None,), ({"number": 3},)]

    schema, rows = AggregateExecution(projection).reduce(matched_rows)

    assert len(rows) == 1
    count_idx = schema.column_index("count")
    # COUNT(*) = 3 (all matched rows), NOT 2 (what count(col) would give here)
    assert rows[0][count_idx] == {"number": 3}


def test_reduce_folds_matched_rows_into_a_single_synthetic_sum_row():
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("title", String(is_title=True)),
        Column("headcount", Integer()),
    )
    projection = (func.sum(accounts.c.headcount),)

    # operand cells arrive raw, one per drained row: each is a {type: value}
    # dict straight off ResultSet._process_page (notiondbapi/resultset.py)
    matched_rows = [({"number": 5},), ({"number": 10},), ({"number": 3},)]

    schema, rows = AggregateExecution(projection).reduce(matched_rows)

    # exactly one synthetic row...
    assert len(rows) == 1
    # ...carrying the total (not the row count) as a raw cell at the "sum" position
    sum_idx = schema.column_index("sum")
    assert rows[0][sum_idx] == {"number": 18}


def test_reduce_sum_skips_null_operand_cells():
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("title", String(is_title=True)),
        Column("headcount", Integer()),
    )
    projection = (func.sum(accounts.c.headcount),)

    # the middle row has no headcount property: an absent property surfaces as a
    # None cell (ResultSet._process_page), not a {"number": 0}
    matched_rows = [({"number": 5},), (None,), ({"number": 3},)]

    schema, rows = AggregateExecution(projection).reduce(matched_rows)

    # SQL NULL semantics: the None cell is skipped, not treated as zero and not
    # crashing the fold — 5 + 3, the missing row simply does not participate
    sum_idx = schema.column_index("sum")
    assert rows[0][sum_idx] == {"number": 8}


def test_reduce_sum_skips_present_but_null_operand_cells():
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("title", String(is_title=True)),
        Column("headcount", Integer()),
    )
    projection = (func.sum(accounts.c.headcount),)

    # A present-but-null number property is the shape ResultSet._process_page
    # actually emits (notiondbapi/resultset.py: {typ: prop[typ]}): the cell is
    # the dict {"number": None}, NOT a bare None. That dict is truthy, so a naive
    # `is not None` filter lets it through and the fold crashes on `int + None`.
    matched_rows = [({"number": 5},), ({"number": None},), ({"number": 3},)]

    schema, rows = AggregateExecution(projection).reduce(matched_rows)

    # SQL NULL semantics: the null-valued cell is skipped just like an absent one
    # — 5 + 3, the null row does not participate and does not crash the fold.
    sum_idx = schema.column_index("sum")
    assert rows[0][sum_idx] == {"number": 8}


def test_reduce_sum_over_zero_rows_is_null_not_zero():
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("title", String(is_title=True)),
        Column("headcount", Integer()),
    )
    projection = (func.sum(accounts.c.headcount),)

    # no rows matched at all
    matched_rows = []

    schema, rows = AggregateExecution(projection).reduce(matched_rows)

    # SQL semantics: SUM over an empty set is NULL, not 0. In raw-cell space a
    # NULL cell is the bare None (an absent property), never {"number": 0} — the
    # latter would round-trip to 0 through the result processor.
    sum_idx = schema.column_index("sum")
    assert rows[0][sum_idx] is None


def test_reduce_folds_matched_rows_into_a_single_synthetic_avg_row():
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("title", String(is_title=True)),
        Column("headcount", Integer()),
    )
    projection = (func.avg(accounts.c.headcount),)

    matched_rows = [({"number": 5},), ({"number": 10},)]

    schema, rows = AggregateExecution(projection).reduce(matched_rows)

    # avg is true division over the operand values: (5 + 10) / 2 = 7.5, a
    # fractional float even though the operand column holds whole numbers
    avg_idx = schema.column_index("avg")
    assert rows[0][avg_idx] == {"number": 7.5}


def test_reduce_avg_over_all_null_rows_is_null_not_zero():
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("title", String(is_title=True)),
        Column("headcount", Integer()),
    )
    projection = (func.avg(accounts.c.headcount),)

    # every row is missing the headcount property: no value survives the skip
    matched_rows = [(None,), (None,)]

    schema, rows = AggregateExecution(projection).reduce(matched_rows)

    # avg of an empty (post-skip) set is NULL — and crucially never a division
    # by zero: with no surviving values there is nothing to divide.
    avg_idx = schema.column_index("avg")
    assert rows[0][avg_idx] is None


def test_reduce_avg_skips_present_but_null_operand_cells():
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("title", String(is_title=True)),
        Column("headcount", Integer()),
    )
    projection = (func.avg(accounts.c.headcount),)

    # Same present-but-null shape {"number": None} straight off _process_page.
    matched_rows = [({"number": 5},), ({"number": None},), ({"number": 3},)]

    schema, rows = AggregateExecution(projection).reduce(matched_rows)

    # The null row must neither crash the fold nor inflate the divisor: avg is
    # over the two surviving values only — (5 + 3) / 2 = 4.0, never 8 / 3.
    avg_idx = schema.column_index("avg")
    assert rows[0][avg_idx] == {"number": 4.0}


def test_labeled_aggregate_surfaces_under_the_custom_result_key():
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("title", String(is_title=True)),
        Column("headcount", Integer()),
    )

    # a user-supplied label overrides the auto function-name key
    schema = SchemaInfo.from_aggregate(
        func.sum(accounts.c.headcount).label("total_payroll")
    )

    assert len(schema.columns) == 1
    assert schema.columns[0].name == "total_payroll"


def test_from_aggregate_disambiguates_colliding_keys_with_an_ordinal_suffix():
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("title", String(is_title=True)),
        Column("headcount", Integer()),
        Column("salary", Numeric()),
    )

    # two sums over different columns collide on the auto key "sum"; an aggregate
    # is provenance-free (table=None) so it can't be qualified like a join column
    schema = SchemaInfo.from_aggregate(
        func.sum(accounts.c.headcount),
        func.sum(accounts.c.salary),
    )

    names = [c.name for c in schema.columns]
    # every member of a colliding group gets a 1-based ordinal suffix
    # (SQLAlchemy-style: sum_1, sum_2 — not sum, sum_1). A lone, non-colliding
    # key stays bare (see the count/label single-aggregate tests above).
    assert names == ["sum_1", "sum_2"]


def test_reduce_fills_each_colliding_aggregate_from_its_own_operand():
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("title", String(is_title=True)),
        Column("headcount", Integer()),
        Column("salary", Numeric()),
    )
    projection = (
        func.sum(accounts.c.headcount),
        func.sum(accounts.c.salary),
    )

    # each drained row carries both operands in projection order:
    # (headcount_cell, salary_cell)
    matched_rows = [
        ({"number": 5}, {"number": 100}),
        ({"number": 10}, {"number": 200}),
    ]

    schema, rows = AggregateExecution(projection).reduce(matched_rows)

    # the two sums share the func key "sum" but must not collapse onto the same
    # operand: sum_1 folds headcount, sum_2 folds salary — read by ordinal
    # position in the projection, never by the (ambiguous) func.key.
    assert rows[0][schema.column_index("sum_1")] == {"number": 15}
    assert rows[0][schema.column_index("sum_2")] == {"number": 300}


def test_all_aggregate_select_compiles_to_a_data_source_query():
    metadata = MetaData()
    employees = Table(
        "employees",
        metadata,
        Column("name", String(is_title=True)),
    )
    employees._sys_columns["data_source_id"]._value = str(uuid.uuid4())

    stmt = select(func.count(employees.c.name))

    compiled = stmt.compile(NotionCompiler())
    asdict = compiled.as_dict()

    # the all-aggregate projection routes to data_sources.query
    # rather than crashing on the missing `.parent`
    assert asdict["operation"]["endpoint"] == "data_sources"
    assert asdict["operation"]["request"] == "query"


def test_sum_over_a_non_numeric_column_fails_at_construction():
    metadata = MetaData()
    employees = Table(
        "employees",
        metadata,
        Column("name", String(is_title=True)),  # a title, not a number
    )

    # Building the aggregate must fail right here — at the func.sum() call —
    # never deferring the type error to execute().
    with pytest.raises(ArgumentError):
        func.sum(employees.c.name)


def test_sum_preserves_the_operand_columns_numeric_type():
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("title", String(is_title=True)),
        Column("headcount", Integer()),       # whole numbers
        Column("balance", Numeric()),         # decimals
    )

    # sum carries the operand's numeric type through, rather than
    # collapsing every numeric column to one fixed return type.
    assert isinstance(func.sum(accounts.c.headcount).type_, Integer)
    assert isinstance(func.sum(accounts.c.balance).type_, Numeric)


def test_avg_returns_a_float_return_type_even_over_an_integer_column():
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("title", String(is_title=True)),
        Column("headcount", Integer()),       # whole numbers
    )

    # Unlike sum (which preserves the operand's type), avg always reduces to a
    # dedicated Float type — averaging integers yields a fractional result.
    assert isinstance(func.avg(accounts.c.headcount).type_, Float)

