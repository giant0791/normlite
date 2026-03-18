import pytest

from normlite.sql.resultschema import SchemaInfo
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, String

@pytest.fixture
def metadata() -> MetaData:
    return MetaData()

@pytest.fixture
def students(metadata: MetaData) -> Table:
    return Table(
        'students',
        metadata,
        Column('name', String(is_title=True)),
        Column('id', Integer()),
        Column('is_active', Boolean()),
        Column('start_on', Date()),
        Column('grade',  String())
    )

def test_schemainfo_always_returns_all_usr_cols_if_empty(
    students: Table
):
    schema_info: SchemaInfo = SchemaInfo.from_table(students)

    assert len(schema_info.columns) == 9

def test_schemainfo_returns_projected_sys_cols_only(
    students: Table
):
    schema_info: SchemaInfo = SchemaInfo.from_table(
        students,
        projected_sys_names=["object_id"]
    )

    assert len(schema_info.columns) == 5 + 1
    assert "object_id" in schema_info.columns
    assert "is_deleted" not in schema_info.columns