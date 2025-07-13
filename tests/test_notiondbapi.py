from normlite.notiondbapi.dbapi2 import Cursor


def test_dbapi_cursor_fetchall(dbapi_cursor):
    """Create a database if it does not exists"""

    rows = dbapi_cursor.fetchall()
    expected_rows = [
        [('id', 'number', '12345'),  ('grade', 'rich-text', 'B'), ('name', 'title', 'Isaac Newton')],
        [('id', 'number', '67890'),  ('grade', 'rich-text', 'A'), ('name', 'title', 'Galileo Galilei')]
    ]

    assert expected_rows == rows