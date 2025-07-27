import json
from pathlib import Path
import uuid
import pytest
from normlite.notion_sdk.client import AbstractNotionClient, InMemoryNotionClient
from normlite.notiondbapi.dbapi2 import Cursor

@pytest.fixture(scope="session")
def api_key() -> str:
    # This is a fake key, read the real one from env variable
    return 'ntn_abc123def456ghi789jkl012mno345pqr'

@pytest.fixture(scope="session")
def ischema_page_id() -> str:
    # This is a fake page id for the info schema page, read the real one from an env variable.
    # Remember that as of today, the Notion API only supports creating pages into 
    # **existing** pages.
    return '680dee41-b447-451d-9d36-c6eaff13fb46'

@pytest.fixture(scope="session")
def client(api_key: str, ischema_page_id: str) -> AbstractNotionClient:
    return InMemoryNotionClient()

# The following fixture must be scope=function otherwise the attribute
# _result_set gets overwritten when executing all tests together
@pytest.fixture(scope="function")
def dbapi_cursor(client: AbstractNotionClient) -> Cursor:
    cursor = Cursor(client)
    
    # DBAPI 2.0: 
    # The attribute is -1 in case no .execute*() has been performed on the cursor 
    # or the rowcount of the last operation cannot be determined by the interface. 
    assert cursor.rowcount == -1

    # New interface: ._parse_result_set() parses the returned object(s) and fills in the result set
    cursor._parse_result_set({
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

    return cursor

# Load the fixture file once
@pytest.fixture(scope="module")
def json_fixtures():
    fixture_path = Path(__file__).parent / "fixtures.json"
    with fixture_path.open() as f:
        return json.load(f)