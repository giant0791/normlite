import uuid

from normlite.sql.compiler import NotionCompiler
from normlite.sql.dml import AggregateExecution, select
from normlite.sql.functions import func
from normlite.sql.resultschema import SchemaInfo
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Integer, String


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

    # three matched rows; the count skeleton only needs to count them
    matched_rows = [("Galileo",), ("Isaac",), ("Marie",)]

    schema, rows = AggregateExecution(projection).reduce(matched_rows)

    # exactly one synthetic row...
    assert len(rows) == 1
    # ...carrying the count as a final value at the "count" position
    count_idx = schema.column_index("count")
    assert rows[0][count_idx] == {"number": 3}


def test_all_aggregate_select_compiles_to_a_databases_query():
    metadata = MetaData()
    employees = Table(
        "employees",
        metadata,
        Column("name", String(is_title=True)),
    )
    employees._sys_columns["object_id"]._value = str(uuid.uuid4())

    stmt = select(func.count(employees.c.name))

    compiled = stmt.compile(NotionCompiler())
    asdict = compiled.as_dict()

    # the all-aggregate projection routes to databases.query
    # rather than crashing on the missing `.parent`
    assert asdict["operation"]["endpoint"] == "databases"
    assert asdict["operation"]["request"] == "query"
