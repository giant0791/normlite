import pytest
from normlite.cursor import CursorResult, Row
from normlite.exceptions import MultipleResultsFound, NoResultFound
from normlite.notiondbapi.dbapi2 import Cursor


def test_compile_cursor_description(dbapi_cursor: Cursor):
    # Connect metadata and cursor result
    result = CursorResult(dbapi_cursor)

    # Fetch all rows
    rows = result.fetchall()

    assert rows[0]['__id__'] == '680dee41-b447-451d-9d36-c6eaff13fb45'
    assert rows[0]['__archived__'] == False
    assert rows[0]['__in_trash__'] == False
    assert rows[0]['id'] == 12345
    assert rows[0]['name'] == 'Isaac Newton'
    assert rows[0]['grade'] == 'B'
    assert rows[1]['__id__'] == '680dee41-b447-451d-9d36-c6eaff13fb46'
    assert rows[1]['__archived__'] == True
    assert rows[1]['__in_trash__'] == True
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
    result = CursorResult(dbapi_cursor)
    rows = result.fetchall()

    assert rows[0]['__id__'] == rows[0][0]
    assert rows[0]['__archived__'] == rows[0][1]
    assert rows[0]['__in_trash__'] == rows[0][2]
    assert rows[0]['id'] == rows[0][3]

def test_row_provides_row_mapping(dbapi_cursor: Cursor):
    expected = {
        "__id__": '680dee41-b447-451d-9d36-c6eaff13fb46',
        "__archived__": True,
        "__in_trash__": True,
        "id": 67890,
        "grade": "A",
        "name": "Galileo Galilei"
    }    

    result = CursorResult(dbapi_cursor)
    rows = result.fetchall()

    assert rows[1].mapping() == expected

def test_cursor_first(dbapi_cursor: Cursor):
    # Given a CursorResult that returns rows
    result = CursorResult(dbapi_cursor)
    assert result.returns_rows

    # When I ask for the first row
    row = result.first()

    # Then I get a Row object containing the column values of the first row
    assert isinstance(row, Row)
    assert row['__id__'] == '680dee41-b447-451d-9d36-c6eaff13fb45'
    assert row['name'] == 'Isaac Newton'

    # And then the cursor is closed
    # Any subsequent call returns None
    assert result.first() is None
    assert result.first() is None

    # and the attribute returns row is false
    assert not result.returns_rows

def test_cursor_as_iterable(dbapi_cursor: Cursor):
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

def test_cursor_one(dbapi_cursor: Cursor):
    # Given a CursorResult that returns rows and only one row is in the result set
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
    # then a NoResultFound is raised if I call one() again
    with pytest.raises(NoResultFound, match='No row was found when one was required.'):
        result.one()

def test_cursor_one_raises_multiple_results_found(dbapi_cursor: Cursor):
    # Given a CursorResult that returns multiple rows
    result = CursorResult(dbapi_cursor)
    assert result.returns_rows
    assert dbapi_cursor.rowcount > 1

    # When I call one()
    with pytest.raises(
        MultipleResultsFound, 
        match='Multiple rows were found when exactly one was required.'):
        result.one()
        
@pytest.mark.skip('Future: Row with Frozen attributes')
def test_row_has_attrs_for_cols(dbapi_cursor: Cursor):
    result = CursorResult(dbapi_cursor)
    rows = result.fetchall()

    assert rows[0]['__id__'] == rows[0].__id__
    assert rows[0]['__archived__'] == rows[0].__archived__
    assert rows[0]['__in_trash__'] == rows[0].__in_trash__
    assert rows[0]['id'] == rows[0].id

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

