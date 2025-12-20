import pdb
import pytest
from normlite.cursor import _NO_CURSOR_RESULT_METADATA, CursorResult, Row
from normlite.exceptions import MultipleResultsFound, NoResultFound, ResourceClosedError
from normlite.notiondbapi.dbapi2 import Cursor
from normlite._constants import SpecialColumns

def make_result_set(dbapi_cursor: Cursor) -> Cursor:
    dbapi_cursor._parse_result_set({
        "object": "list",
        "results": [
            {
                "object": "page",
                "id": '680dee41-b447-451d-9d36-c6eaff13fb45',
                "archived": False,
                "in_trash": False,
                "properties": {
                    "id": {"id": "%3AUPp","type": "number", "number": 12345},
                    "grade": {"id": "A%40Hk", "type": "rich_text", "rich_text": [{"text": {"content": "B"}}]},
                    "name": {"id": "BJXS", "type": "title", "title": [{"text": {"content": "Isaac Newton"}}]},
                },
            },
            {
                "object": "page",
                "id": '680dee41-b447-451d-9d36-c6eaff13fb46',
                "archived": True,
                "in_trash": True,
                "properties": {
                    "id": {"id": "Iowm", "type": "number", "number": 67890},
                    "grade": {"id": "Jsfb", "type": "rich_text", "rich_text": [{"text": {"content": "A"}}]},
                    "name": {"id": "WOd%3B", "type": "title", "title": [{"text": {"content": "Galileo Galilei"}}]},
                },
            },
        ]
    }) 

def test_compile_cursor_description(dbapi_cursor: Cursor):
    # Connect metadata and cursor result
    make_result_set(dbapi_cursor)
    result = CursorResult(dbapi_cursor)

    # Fetch all rows
    rows = result.fetchall()

    assert rows[0][SpecialColumns.NO_ID] == '680dee41-b447-451d-9d36-c6eaff13fb45'
    assert rows[0][SpecialColumns.NO_ARCHIVED] == False
    assert rows[0][SpecialColumns.NO_IN_TRASH] == False
    assert rows[0]['id'] == 12345
    assert rows[0]['name'] == 'Isaac Newton'
    assert rows[0]['grade'] == 'B'
    assert rows[1][SpecialColumns.NO_ID] == '680dee41-b447-451d-9d36-c6eaff13fb46'
    assert rows[1][SpecialColumns.NO_ARCHIVED] == True
    assert rows[1][SpecialColumns.NO_IN_TRASH] == True
    assert rows[1]['id'] == 67890
    assert rows[1]['name'] == 'Galileo Galilei'
    assert rows[1]['grade'] == 'A'

# Parametrize the test using the loaded data
@pytest.mark.parametrize("fixture_case", ["unordered_properties", "real_world_data"])
def test_cursor_result_from_fixture(dbapi_cursor: Cursor, json_fixtures, fixture_case):
    # Extract the named case
    case = next(f for f in json_fixtures if f["name"] == fixture_case)

    # Set up cursor with simulated result set
    dbapi_cursor._parse_result_set(case["result_set"])
    result = CursorResult(dbapi_cursor)
    rows = result.fetchall()

    assert len(rows) == 1
    row = rows[0]
    

    # Check each expected column value
    for key, expected_val in case["expected_row"].items():
        assert row[key] == expected_val

def test_row_getitem_key_or_index(dbapi_cursor: Cursor):
    make_result_set(dbapi_cursor)
    result = CursorResult(dbapi_cursor)
    rows = result.fetchall()

    assert rows[0][SpecialColumns.NO_ID] == rows[0][0]
    assert rows[0][SpecialColumns.NO_ARCHIVED] == rows[0][1]
    assert rows[0][SpecialColumns.NO_IN_TRASH] == rows[0][2]
    assert rows[0]['id'] == rows[0][3]

def test_row_provides_row_mapping(dbapi_cursor: Cursor):
    make_result_set(dbapi_cursor)
    expected = {
        SpecialColumns.NO_ID: '680dee41-b447-451d-9d36-c6eaff13fb46',
        SpecialColumns.NO_ARCHIVED: True,
        SpecialColumns.NO_IN_TRASH: True,
        "id": 67890,
        "grade": "A",
        "name": "Galileo Galilei"
    }    

    result = CursorResult(dbapi_cursor)
    rows = result.fetchall()

    assert rows[1].mapping() == expected

def test_cursor_first(dbapi_cursor: Cursor):
    # Given a CursorResult that returns rows
    make_result_set(dbapi_cursor)
    result = CursorResult(dbapi_cursor)
    assert result.returns_rows

    # When I ask for the first row
    row = result.first()

    # Then I get a Row object containing the column values of the first row
    assert isinstance(row, Row)
    assert row[SpecialColumns.NO_ID] == '680dee41-b447-451d-9d36-c6eaff13fb45'
    assert row['name'] == 'Isaac Newton'

    # And then the cursor is closed
    # Any subsequent call raises an error
    with pytest.raises(ResourceClosedError, match='closed state'):
        row = result.first()

    # and the attribute returns row is false
    assert not result.returns_rows

def test_cursor_as_iterable(dbapi_cursor: Cursor):
    make_result_set(dbapi_cursor)
    expected_names = ['Isaac Newton', 'Galileo Galilei']
    # Given a CursorResult that returns rows
    result = CursorResult(dbapi_cursor)
    assert result.returns_rows

    # When I iterate through the cursor result
    for idx, row in enumerate(result):
        # Then I get non None row objects
        assert isinstance(row, Row)
        assert row['name'] == expected_names[idx]

    # and then the cursor does not returns rows anymore
    assert not result.returns_rows

def test_cursor_one_for_rows(dbapi_cursor: Cursor):
    # Given a CursorResult that returns rows and only one row is in the result set
    make_result_set(dbapi_cursor)

    # IMPORTANT: comsume 1 result, len(result_set) == 2
    dbapi_cursor.fetchone()
    result = CursorResult(dbapi_cursor)
    assert result.returns_rows

    # When I ask for one row
    row = result.one()

    # then I get exactly the one expected row object
    assert isinstance(row, Row)
    assert row['name'] == 'Galileo Galilei'

    # then no no more rows are returned
    assert not result.returns_rows
    # And then the cursor is closed
    # Any subsequent call raises an error
    with pytest.raises(ResourceClosedError, match='closed state'):
        row = result.first()

def test_cursor_one_for_table(dbapi_cursor: Cursor):
    # Given a database result is expected, but none was found
    dbapi_cursor._parse_result_set({
        "object": "list",
        "results": [{
            "object": "database",
            "id": "680dee41-b447-451d-9d36-c6eaff13fb46",
                "archived": False,
                "in_trash": False,
            "parent": {
                "type": "page_id",
                "page_id": "ac1211ca-e3f1-9939-ae34-5260b16f628c"
            },
            "title": [
                {
                    "type": "text",
                    "text": {"content": "students"}
                }
            ],
            "properties": {
                "id": {"id": "evWq", "name": "id", "type": "number", "number": {}},
                "name": {"id": "title", "name": "name", "type": "title", "title": {}},
                "grade": {"id": "V}lX", "name": "grade", "type": "rich_text", "rich_text": {}},
            },           
        }]
    })

    result = CursorResult(dbapi_cursor)
    
    map = row.mapping()
    pdb.set_trace()



def test_result_was_required_but_none_found(dbapi_cursor: Cursor):
    # Given a database result is expected, but none was found
    dbapi_cursor._parse_result_set({
        "object": "list",
        "results": []
    })

    result = CursorResult(dbapi_cursor)
    
    # when I check for returned rows,
    # then I get False
    assert not result.returns_rows
    assert result._metadata is _NO_CURSOR_RESULT_METADATA

    # when I try to get the first result,
    # then I get a NoResultFound error
    with pytest.raises(NoResultFound, match='No row was found'):
        row = result.one()

def test_cursor_one_raises_multiple_results_found(dbapi_cursor: Cursor):
    # Given a CursorResult that returns multiple rows
    make_result_set(dbapi_cursor)
    result = CursorResult(dbapi_cursor)
    assert result.returns_rows
    assert dbapi_cursor.rowcount > 1

    # When I call one()
    with pytest.raises(
        MultipleResultsFound, 
        match='Multiple rows were found when exactly one was required.'):
        result.one()
        
def test_row_has_attrs_for_cols(dbapi_cursor: Cursor):
    make_result_set(dbapi_cursor)
    result = CursorResult(dbapi_cursor)
    rows = result.fetchall()

    assert rows[0][SpecialColumns.NO_ID] == getattr(rows[0], SpecialColumns.NO_ID.value)
    assert rows[0][SpecialColumns.NO_ARCHIVED] == getattr(rows[0], SpecialColumns.NO_ARCHIVED.value)
    assert rows[0][SpecialColumns.NO_IN_TRASH] == getattr(rows[0], SpecialColumns.NO_IN_TRASH.value)
    assert rows[0]['id'] == rows[0].id
    assert rows[0]['name'] == rows[0].name
    assert rows[0]['grade'] == rows[0].grade

    with pytest.raises(AttributeError, match="registered_on"):
        non_existing = rows[0].registered_on

def test_closed_cursor_result_raises_error(dbapi_cursor: Cursor):
    make_result_set(dbapi_cursor)
    result = CursorResult(dbapi_cursor)
    result.close()

@pytest.mark.skip('Future: Row with Frozen attributes')
def test_row_attributes_cannot_be_set_or_del(dbapi_cursor: Cursor):
    result = CursorResult(dbapi_cursor)
    rows = result.fetchall()

    with pytest.raises(AttributeError, match='Cannot modify read-only attribute'):
        setattr(rows[0], '__id__', 'invalid_val')

    with pytest.raises(AttributeError, match='Cannot delete read-only attribute'):
        delattr(rows[0], '__id__')

@pytest.mark.skip('Future: Row with Frozen attributes')
def test_row_attrerror_if_attr_non_existent(dbapi_cursor: Cursor):
    result = CursorResult(dbapi_cursor)
    rows = result.fetchall()

    with pytest.raises(AttributeError, match='object has no attribute:'):
        rows[0].invalid_name

