from normlite.notiondbapi.dbapi2 import Cursor


def test_dbapi_cursor_fetchall():
    """Create a database if it does not exists"""

    cursor = Cursor()
    cursor._result_set = {
    "object": "list",
    "results": [
        {
            "object": "page",
            "properties": {
                "id": {"type": "number", "number": "12345"},
                "grade": {"type": "rich-text", "richt-text": [{"text": {"content": "B"}}]},
                "name": {"type": "title", "title": [{"text": {"content": "Isaac Newton"}}]},
            },
        },
        {
            "object": "page",
            "properties": {
                "id": {"type": "number", "number": "67890"},
                "grade": {"type": "rich-text", "richt-text": [{"text": {"content": "A"}}]},
                "name": {"type": "title", "title": [{"text": {"content": "Galileo Galilei"}}]},
            },
        },
    ]
    }

    rows = cursor.fetchall()
    expected_rows = [
        [('id', 'number', '12345'),  ('grade', 'rich-text', 'B'), ('name', 'title', 'Isaac Newton')],
        [('id', 'number', '67890'),  ('grade', 'rich-text', 'A'), ('name', 'title', 'Galileo Galilei')]
    ]

    assert expected_rows == rows