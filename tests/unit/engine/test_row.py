import pytest

from normlite.engine.base import Engine
from normlite.engine.resultmetadata import CursorResultMetaData
from normlite.notiondbapi.resultset import ResultSet
from normlite.engine.row import Row
from normlite.sql.schema import Table


from normlite.sql.type_api import DateTimeRange
from tests.utils.db_helpers import create_students_db, attach_table_oid, populate_students
from tests.utils.assertions import assert_is_today, assert_is_valid_uuid4

@pytest.fixture
def row_matadata(row_description: tuple[tuple, ...]) -> CursorResultMetaData:
    return CursorResultMetaData(
        row_description,
        is_ddl=False
    )

@pytest.fixture
def row(engine: Engine, students: Table, row_description: tuple[tuple, ...]) -> Row:
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=1)
    client = engine._client
    result = client.databases_query(
        path_params={"database_id": db_id}
    )

    assert len(result["results"]) == 1, "Expected one page only."

    page = result["results"][0]
    rs = ResultSet.from_json(row_description, page)
    return Row(
        CursorResultMetaData(rs.description, is_ddl=False),
        next(rs)
    )

@pytest.fixture
def table_rows(engine: Engine, students: Table) -> list[Row]:
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    client = engine._client
    database = client.databases_retrieve(
        path_params={"database_id": db_id}
    )

    assert database, f"Expected to find database with ID: {db_id}"

    rs = ResultSet.from_json(description=None, notion_obj=database)
    table_metadata = CursorResultMetaData(rs.description, is_ddl=True)
    rows = [Row(table_metadata, row_data) for row_data in rs]

    return rows    

# ---------------------------------------
# Constructions tests
# ---------------------------------------

def test_row_can_construct_from_page_metadata(row: Row):
    usr_colnames = {"name", "id", "is_active", "start_on", "grade"}
    sys_colnames = {"object_id", "is_archived", "is_deleted", "created_at"}
    all_colnames = sys_colnames.union(usr_colnames)
    colnames = set(row.keys())
    
    assert usr_colnames.issubset(colnames)
    assert sys_colnames.issubset(colnames)
    assert all_colnames == colnames

def test_row_can_construct_from_database_metadata(table_rows: list[Row]):
    colnames = set(table_rows[0].keys())
    syscol_rows = [row[0] for row in table_rows if row[4]]
    usrcol_rows = [row[0] for row in table_rows if not row[4]]

    assert len(table_rows) == 10
    assert len(syscol_rows) == 5
    assert len(usrcol_rows) == 5
    assert colnames == {"column_name", "column_type", "column_id", "metadata", "is_system"}

# ---------------------------------------
# Access to columns tests
# ---------------------------------------

def test_access_column_by_key_for_row(row: Row):
    all_colnames = {"object_id", "is_archived", "is_deleted", "created_at", "name", "id", "is_active", "start_on", "grade"}

    # test all columns
    for key in all_colnames:
        try:
            _ = row[key]
        except KeyError as exc:
            pytest.fail(f"row raised KeyError unexpectedly: {str(exc)}")

def test_access_to_col_val_by_key_for_row(row: Row):
    assert_is_valid_uuid4(row["object_id"])
    assert not row["is_archived"]
    assert not row["is_deleted"]
    assert_is_today(row["created_at"])
    assert row["name"] == "name_0"
    assert row["id"] == 0
    assert row["is_active"]
    assert row["grade"] == "A"
    assert isinstance(row["start_on"], DateTimeRange)
    assert row["start_on"] == DateTimeRange(start_datetime="1600-01-01")

def test_access_column_by_key_for_table(table_rows: list[Row]):
    all_colnames = {"column_name", "column_type", "column_id", "metadata", "is_system"}
    first = table_rows[0]    

    # test all columns
    for key in all_colnames:
        try:
            _ = first[key]
        except KeyError as exc:
            pytest.fail(f"row raised KeyError unexpectedly: {str(exc)}")

def test_column_by_index_for_row(row: Row):
    columns = len(list(row.keys()))
    
    for idx in range(columns):
        try:
            _ = row[idx]
        except IndexError as exc:
            pytest.fail(f"row raised IndexError unexpectedly: {str(exc)}")

def test_column_by_index_for_table(table_rows: list[Row]):
    columns = len(list(table_rows[0].keys()))

    for idx in range(columns):
        for table_row in table_rows:
            try:
                _ = table_row[idx]
            except IndexError as exc:
                pytest.fail(f"row raised IndexError unexpectedly: {str(exc)}")

def test_access_col_val_by_index_for_row(row: Row):
    assert_is_valid_uuid4(row[0])
    assert not row[1]
    assert not row[2]
    assert_is_today(row[3])
    assert row[4] == "name_0"
    assert row[5] == 0
    assert row[6]
    assert isinstance(row[7], DateTimeRange)
    assert row[7] == DateTimeRange(start_datetime="1600-01-01")
    assert row[8] == "A"

def test_access_col_val_by_index_for_table(table_rows: list[Row]):
    pass

def test_access_column_by_attribute_for_row():
    pass

def test_wrong_key_raises(row: Row):
    with pytest.raises(KeyError):
        _ = row["does_not_exists"]   

def test_wrong_index_raises(row: Row):
    with pytest.raises(IndexError):
        _ = row[100]

def test_wrong_attr_raises(row: Row):
    with pytest.raises(AttributeError):
        _ = row.does_not_exists

# ---------------------------------------
# Rows to mappings and tuples tests
# ---------------------------------------

def test_row_to_mapping(row: Row, table_rows: list[Row]):
    row_mapping = row.mapping()
    trow_mapping = table_rows[0].mapping()

    assert set(row_mapping.keys()) == {"object_id", "is_archived", "is_deleted", "created_at", "name", "id", "is_active", "start_on", "grade"}
    assert row_mapping["object_id"] == row["object_id"]

    assert set(trow_mapping.keys()) == {"column_name", "column_type", "column_id", "metadata", "is_system"}
    assert trow_mapping["column_name"] == table_rows[0]["column_name"]

def test_row_to_tuples(row: Row, table_rows: list[Row]):
    row_tuples = row.as_tuple()
    trow_tuples = table_rows[0].as_tuple()

    assert len(row_tuples) == 9
    assert row_tuples[8] == "A"         # grade property
    assert len(trow_tuples) == 5
    assert trow_tuples[4]               # is_system is True for the object_id column metadata
