import pytest
from normlite.cursor import _CursorMetaData, CursorResult
from normlite.notiondbapi.dbapi2 import Cursor
from normlite.sql import ColumnDef, CreateTable, MetaData


def test_cursor_fetchall(dbapi_cursor: Cursor):
    # Simulate parsed CREATE TABLE
    create_ast = CreateTable(
        table_name="students",
        columns=[
            ColumnDef("id", "int"),
            ColumnDef("name", "varchar(n)"),
            ColumnDef("grade", "varchar(n)")
        ]
    )

    # Connect metadata and cursor result
    metadata = _CursorMetaData(create_ast)
    result = CursorResult(dbapi_cursor, metadata)

    # Fetch all rows
    rows = result.fetchall()

    assert rows[0]['name'] == 'Isaac Newton'
    assert rows[0]['grade'] == 'B'
    assert rows[1]['name'] == 'Galileo Galilei'
    assert rows[1]['grade'] == 'A'

# Parametrize the test using the loaded data
@pytest.mark.parametrize("fixture_case", ["unordered_properties", "real_world_data"])
def test_cursor_result_from_fixture(json_fixtures, fixture_case):
    # Extract the named case
    case = next(f for f in json_fixtures if f["name"] == fixture_case)

    # Build CreateTable and ColumnDef objects
    columns = [ColumnDef(**col) for col in case["create_table"]["columns"]]
    table_def = CreateTable(
        table_name=case["create_table"]["table_name"],
        columns=columns
    )

    # Set up cursor with simulated result set
    cursor = Cursor()
    cursor._result_set = case["result_set"]

    metadata = _CursorMetaData(table_def)
    result = CursorResult(cursor, metadata)

    rows = result.fetchall()
    assert len(rows) == 1
    row = rows[0]

    # Check each expected column value
    for key, expected_val in case["expected_row"].items():
        assert row[key] == expected_val

@pytest.mark.skip(reason="execute() method not implemented in the CursorResult class yet")
def test_cursor_execute(dbapi_cursor):
    create_ast = CreateTable(
        table_name="students",
        columns=[
            ColumnDef("id", "int"),
            ColumnDef("name", "title_varchar(n)"),
            ColumnDef("grade", "varchar(n)")
        ]
    )

    # Add table to the metadata table catalog
    table_catalog: MetaData = MetaData()
    table_catalog.add(create_ast)

    # Connect metadata, and table catalog with cursor result
    # This is necessary because the visitor of the insert statement needs
    # the column types to properly generate the JSON object 
    metadata = _CursorMetaData(create_ast)
    result = CursorResult(dbapi_cursor, metadata)

    # Execute an insert statement
    row = result.execute("insert into students (id, name, grade) values (1, 'Isaac Newton', 'B')")