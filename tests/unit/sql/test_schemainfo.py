import pdb

import pytest

from normlite.exceptions import InvalidRequestError, NoSuchColumnError
from normlite.sql.resultschema import SchemaInfo
from normlite.sql.schema import Table

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