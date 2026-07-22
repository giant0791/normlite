import pdb

import pytest

from normlite import ForeignKey, Relation
from normlite.exceptions import InvalidRequestError, NoSuchColumnError
from normlite.sql.dml import Join
from normlite.sql.resultschema import SchemaInfo
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import String

def test_schemainfo_always_never_returns_cols_if_projections_empty(
    students: Table
):
    schema_info: SchemaInfo = SchemaInfo.from_table(students)

    assert len(schema_info.columns) == 0

def test_schemainfo_returns_projected_sys_cols_only(
    students: Table
):
    schema_info: SchemaInfo = SchemaInfo.from_table(
        students,
        projected_names=["object_id"]
    )
    columns = {col.name for col in schema_info.columns}

    assert len(schema_info.columns) == 1
    assert "object_id" in columns
    assert "is_deleted" not in columns

def test_schemainfo_excludes_data_source_id_from_description(
    students: Table
):
    # data_source_id is a system column on every Table, but a Notion page has no
    # data_source_id key and a SQL user never selects it (ADR-0014). It must never
    # reach a page-result description, or _process_page KeyErrors walking it.
    schema_info: SchemaInfo = SchemaInfo.from_table(
        students,
        execution_names=["object_id", "data_source_id"],
    )

    names = [entry[0] for entry in schema_info.as_sequence()]

    assert "object_id" in names
    assert "data_source_id" not in names

def test_schemainfo_returns_projected_usr_cols_only(
    students: Table
):
    schema_info: SchemaInfo = SchemaInfo.from_table(
        students,
        projected_names=["name"]
    )

    columns = {col.name for col in schema_info.columns}

    assert len(schema_info.columns) == 1
    assert "name" in columns
    assert "start_on" not in columns
    assert "object_id" not in columns
    assert "is_deleted" not in columns

def test_schemainfo_column_index_returns_index(students: Table):
    schema_info: SchemaInfo = SchemaInfo.from_table(
        students,
        execution_names=["object_id", "is_deleted"],
        projected_names=["name"]
    )

    assert schema_info.column_index("object_id") == 0
    assert schema_info.column_index("is_deleted") == 1
    assert schema_info.column_index("name") == 2       

def test_schemainfo_column_index_raises_if_no_such_col(students: Table):
    schema_info: SchemaInfo = SchemaInfo.from_table(
        students,
        execution_names=["object_id", "is_deleted"],
        projected_names=["name"]
    )

    with pytest.raises(NoSuchColumnError) as exc:
        _ = schema_info.column_index("is_archived")

    assert "is_archived" in str(exc.value)

def test_schemainfo_column_getter_returns_correct_value(students: Table):
    schema_info: SchemaInfo = SchemaInfo.from_table(
        students,
        execution_names=["object_id", "is_deleted"],
        projected_names=["name"]
    )

    row = ("abcd", True, "Alice")
    get_oid = schema_info.column_getter("object_id")
    get_is_deleted = schema_info.column_getter("is_deleted")
    get_name = schema_info.column_getter("name")

    assert get_oid(row) == "abcd"
    assert get_is_deleted(row) 
    assert get_name(row) == "Alice"

def test_schemainfo_column_index_on_unitialized_schema_raises():
    schema = SchemaInfo(columns=list())

    with pytest.raises(InvalidRequestError):
        _ = schema.column_index("fake")


def test_from_join_sides_derives_both_leaf_schemas_from_the_join_and_projection():
    # from_join_sides is the SINGLE home for the two-sided leaf-schema
    # derivation the Planner feeds to each Scan (#364). It must reproduce the
    # two load-bearing rules today inlined in JoinExecution.__init__
    # (dml.py:1142-1160):
    #   (1) each side carries its own hidden ``object_id`` execution name;
    #   (2) the LEFT side always carries the ``onclause`` (join-key) column,
    #       even when the user did not project it — or the join can't match.
    metadata = MetaData()
    courses = Table(
        "courses",
        metadata,
        Column("title", String(is_title=True)),
    )
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )
    join = Join(students, courses, students.c.enrolled_in)

    # Deliberately project ONE user column per side and NOT the join key
    # ``enrolled_in`` — so rule (2) is what makes it appear in the left schema.
    projection = [students.c.name, courses.c.title]

    left_schema, right_schema = SchemaInfo.from_join_sides(join.left, join.right, projection, join.onclause)

    left_names = {c.name for c in left_schema.columns}
    right_names = {c.name for c in right_schema.columns}

    # left: object_id (execution) + name (projected) + enrolled_in (join key, unprojected)
    assert left_names == {"object_id", "name", "enrolled_in"}
    # right: object_id (execution) + title (projected); no left columns leak in
    assert right_names == {"object_id", "title"}