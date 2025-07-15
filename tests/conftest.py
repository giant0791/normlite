import json
from pathlib import Path
import pytest
from normlite.notion_sdk.client import AbstractNotionClient, FakeNotionClient
from normlite.notiondbapi.dbapi2 import Cursor

@pytest.fixture(scope="session")
def client() -> AbstractNotionClient:
    return FakeNotionClient(auth='ntn_abc123def456ghi789jkl012mno345pqr')

@pytest.fixture(scope="session")
def dbapi_cursor() -> Cursor:
    cursor = Cursor()
    
    # DBAPI 2.0: 
    # The attribute is -1 in case no .execute*() has been performed on the cursor 
    # or the rowcount of the last operation cannot be determined by the interface. 
    assert cursor.rowcount == -1

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

    return cursor

# Load the fixture file once
@pytest.fixture(scope="module")
def json_fixtures():
    fixture_path = Path(__file__).parent / "fixtures.json"
    with fixture_path.open() as f:
        return json.load(f)