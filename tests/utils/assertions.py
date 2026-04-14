from datetime import datetime
import uuid

def assert_no_rows(result) -> None:
    """
    Assert that a result returns no rows.
    """
    rows = result.all()
    assert rows == []
    assert not result.returns_rows


def assert_rowcount(result, expected: int) -> None:
    """
    Assert rowcount matches expected value.
    """
    assert result.rowcount == expected


def assert_columns(row, expected_columns) -> None:
    """
    Assert that a row contains exactly the expected columns.
    """
    actual = set(row.keys())
    expected = set(expected_columns)
    assert actual == expected, f"Expected columns {expected}, got {actual}"


def assert_single_row(result):
    """
    Assert result contains exactly one row and return it.
    """
    rows = result.all()
    assert len(rows) == 1
    return rows[0]


def assert_empty_select(engine, stmt):
    """
    Execute a select and assert it returns no rows.
    """
    with engine.connect() as conn:
        result = conn.execute(stmt)
        rows = result.all()
        assert rows == []

def is_today(date_as_iso: str):
    try:
        # Parse the ISO 8601 string into a datetime object
        parsed_dt = datetime.fromisoformat(date_as_iso)
        
        # Compare only the date parts (Year, Month, Day)
        return parsed_dt.date() == datetime.now().date()
    except ValueError:
        # Handle cases where the string isn't a valid ISO format
        return False
    
def assert_is_today(date_as_iso: str):
    assert is_today(date_as_iso)
    
def is_valid_uuid4(uuid_string):
    try:
        # If the string is not a valid UUIDv4, this will raise a ValueError
        val = uuid.UUID(uuid_string, version=4)
    except ValueError:
        return False
    
    # Optional: Ensure the string is in the standard canonical form
    return str(val) == uuid_string

def assert_is_valid_uuid4(uuuid_string):
    assert is_valid_uuid4(uuuid_string)