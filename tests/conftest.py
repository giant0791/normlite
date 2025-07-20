import json
from pathlib import Path
import uuid
import pytest
from normlite.notion_sdk.client import AbstractNotionClient, FakeNotionClient
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
    return FakeNotionClient(auth=api_key, ischema_page_id=ischema_page_id)

# The following fixture must be scope=function otherwise the attribute
# _result_set gets overwritten when executing all tests together
@pytest.fixture(scope="function")
def dbapi_cursor(client: AbstractNotionClient) -> Cursor:
    cursor = Cursor(client)
    
    # DBAPI 2.0: 
    # The attribute is -1 in case no .execute*() has been performed on the cursor 
    # or the rowcount of the last operation cannot be determined by the interface. 
    assert cursor.rowcount == -1

    cursor._result_set = {
        "object": "list",
        "results": [
            {
                "object": "page",
                "id": '680dee41-b447-451d-9d36-c6eaff13fb45',
                "properties": {
                    "id": {"type": "number", "number": "12345"},
                    "grade": {"type": "rich_text", "richt_text": [{"text": {"content": "B"}}]},
                    "name": {"type": "title", "title": [{"text": {"content": "Isaac Newton"}}]},
                },
            },
            {
                "object": "page",
                "id": '680dee41-b447-451d-9d36-c6eaff13fb46',
                "properties": {
                    "id": {"type": "number", "number": "67890"},
                    "grade": {"type": "rich_text", "richt_text": [{"text": {"content": "A"}}]},
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