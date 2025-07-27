import pdb
import pytest
import json
from pathlib import Path
from uuid import UUID
from datetime import datetime
from unittest.mock import patch
import itertools

from normlite.notion_sdk.client import FileBasedNotionClient, InMemoryNotionClient, NotionError  

@pytest.fixture
def fresh_client():
    yield InMemoryNotionClient()

    # IMPORTANT:
    # tear down: reset shared class store between tests
    InMemoryNotionClient._store = {"store": []}

# Fixtures for mocking
@pytest.fixture
def fixed_datetime():
    return datetime(2024, 1, 1, 12, 0, 0)

@pytest.fixture
def fixed_uuid_generator():
    def _uuid_gen():
        for i in itertools.count():
            yield UUID(f"{i:032x}")
    return _uuid_gen()

@pytest.fixture
def fixed_uuids():
    # Provide a sequence of UUIDs that will be returned in order
    return [
        UUID("00000000-0000-0000-0000-000000000000"),  # parent.page_id for info schema
        UUID("11111111-1111-1111-1111-111111111111"),  # info schema page id
        UUID("22222222-2222-2222-2222-222222222222"),  # first page
        UUID("33333333-3333-3333-3333-333333333333"),  # second page
    ]


def make_page_payload(title: str, parent_id: str = "00000000-0000-0000-0000-000000000000") -> dict:
    return {
        'parent': {
            'type': 'page_id',
            'page_id': parent_id
        },
        'properties': {
            'Name': {'title': [{'text': {'content': title}}]}
        }
    }

# ============== Tests for InMemoryNotionClient =========================

@patch("normlite.notion_sdk.client.datetime")
@patch("normlite.notion_sdk.client.uuid.uuid4")
def test_pages_create_and_retrieve(mock_uuid4, mock_datetime, fresh_client):
    mock_uuid4.return_value = UUID("11111111-1111-1111-1111-111111111111")
    mock_datetime.now.return_value = datetime(2024, 1, 1, 12, 0, 0)

    payload = {
        'parent': {'type': 'page_id', 'page_id': '123'},
        'properties': {
            'Name': {'title': [{'text': {'content': 'My Test Page'}}]}
        }
    }

    page = fresh_client.pages_create(payload)
    assert page['id'] == "11111111-1111-1111-1111-111111111111"
    assert page['created_id'] == "2024-01-01T12:00:00"
    assert page['archived'] is False
    assert page['in_trash'] is False

    retrieved = fresh_client.pages_retrieve({'id': page['id']})
    assert retrieved['properties']['Name']['title'][0]['text']['content'] == "My Test Page"

def test_get_by_title(fresh_client):
    payload = {
        'parent': {'type': 'page_id', 'page_id': 'xyz'},
        'properties': {
            'Name': {'title': [{'text': {'content': 'Unique Title'}}]}
        }
    }

    page = fresh_client.pages_create(payload)
    found = fresh_client._get_by_title("Unique Title", "page")
    assert found['id'] == page['id']

    missing = fresh_client._get_by_title("Nonexistent", "page")
    assert missing == {}

def test_create_and_retrieve_database(fresh_client):
    db_payload = {
        'parent': {'type': 'page_id', 'page_id': 'parent123'},
        'properties': {
            'Name': {'title': [{'text': {'content': 'DB'}}]},
            'Field': {'rich_text': {}}
        }
    }

    db = fresh_client.databases_create(db_payload)
    assert db['object'] == 'database'
    assert db['properties']['Field']['type'] == 'rich_text'

    retrieved = fresh_client.databases_retrieve({'id': db['id']})
    assert retrieved['id'] == db['id']

def test_missing_parent_raises(fresh_client):
    with pytest.raises(NotionError, match='Missing "parent" object'):
        fresh_client.pages_create({'properties': {}})


def test_store_isolated_between_instances():
    InMemoryNotionClient._store = {"store": []}

    client1 = InMemoryNotionClient()
    client1._create_store([])
    client1.pages_create({
        'parent': {'type': 'page_id', 'page_id': 'abc'},
        'properties': {
            'Name': {'title': [{'text': {'content': 'Page 1'}}]}
        }
    })

    client2 = InMemoryNotionClient()
    # client2 sees client1's data due to shared class-level store
    assert client2._store_len() == 2  # 1 page + info_schema

    # To truly isolate, one should call `_create_store([])` in the second client
    client2._create_store([])
    assert client2._store_len() == 1  # Only info_schema after reset
    
# ============== Tests for FileBasedNotionClient =========================

@patch("normlite.notion_sdk.client.uuid.uuid4")
@patch("normlite.notion_sdk.client.datetime")
def test_pages_create_and_dump(mock_datetime, mock_uuid4, tmp_path, fixed_datetime, fixed_uuids):
    mock_datetime.now.return_value = fixed_datetime
    mock_uuid4.side_effect = fixed_uuids

    file_path = tmp_path / "notion-store.json"
    client = FileBasedNotionClient(str(file_path))

    with client as c:
        c.pages_create(make_page_payload("Test Page 1"))
        c.pages_create(make_page_payload("Test Page 2"))

    with open(file_path, "r") as f:
        content = json.load(f)

    assert len(content["store"]) == 3  # 1 info schema + 2 pages

    page_ids = [page["id"] for page in content["store"]]
    assert "11111111-1111-1111-1111-111111111111" in page_ids  # info schema
    assert "22222222-2222-2222-2222-222222222222" in page_ids  # page 1
    assert "33333333-3333-3333-3333-333333333333" in page_ids  # page 2

    for page in content["store"]:
        assert page["created_id"] == fixed_datetime.isoformat()
        assert page["archived"] is False
        assert page["in_trash"] is False


@patch("normlite.notion_sdk.client.uuid.uuid4")
@patch("normlite.notion_sdk.client.datetime")
def test_context_manager_creates_file_on_exit(mock_datetime, mock_uuid4, tmp_path, fixed_datetime, fixed_uuids):
    mock_datetime.now.return_value = fixed_datetime
    mock_uuid4.side_effect = fixed_uuids

    file_path = tmp_path / "new-store.json"

    with FileBasedNotionClient(str(file_path)) as client:
        client.pages_create(make_page_payload("First Page"))

    with open(file_path, "r") as f:
        saved = json.load(f)

    ids = [entry["id"] for entry in saved["store"]]
    assert "11111111-1111-1111-1111-111111111111" in ids  # info schema
    assert "22222222-2222-2222-2222-222222222222" in ids  # First Page

    for entry in saved["store"]:
        assert entry["created_id"] == fixed_datetime.isoformat()


def test_load_existing_file(tmp_path):
    # Manual dump of a fake page
    file_path = tmp_path / "existing-store.json"
    store = {
        "store": [
            {
                "object": "page",
                "id": "11111111-1111-1111-1111-111111111111",
                "parent": {
                    "type": "page_id",
                    "page_id": "00000000-0000-0000-0000-000000000000"
                },
                "properties": {
                    "Name": {"title": [{"text": {"content": "Loaded Page"}}]}
                },
                "created_id": "2024-01-01T00:00:00",
                "archived": False,
                "in_trash": False
            }
        ]
    }

    file_path.write_text(json.dumps(store))

    client = FileBasedNotionClient(str(file_path))
    with client as c:
        loaded = c._get("11111111-1111-1111-1111-111111111111")
        assert loaded["properties"]["Name"]["title"][0]["text"]["content"] == "Loaded Page"


@patch("normlite.notion_sdk.client.uuid.uuid4")
@patch("normlite.notion_sdk.client.datetime")
def test_dump_and_load_symmetry(mock_datetime, mock_uuid4, tmp_path, fixed_datetime, fixed_uuid_generator):
    """
    This test raises a StopIteration in an internal function of the mock.py module,
    if you pass the fixed_uiids fixture used in the previous tests.
    The issue is:

        - You're mocking uuid.uuid4() with a finite list of 4 UUIDs, passed as mock_uuid4.side_effect = fixed_uuids

        - But when re-entering the context manager (with FileBasedNotionClient(...)), it tries to 
          create the information_schema page again, which also calls uuid.uuid4() internally

        - That causes the UUID mock to run out of values → StopIteration    

    Adding an additional mocked id in the affected fixture resolves the issue.
    Here, you decided to avoid hardcoding limits and added the fixed_uuid_generator fixture, which 
    provides an infinite generator.
    This way uuid4() never runs out.
    """
    mock_datetime.now.return_value = fixed_datetime
    mock_uuid4.side_effect = fixed_uuid_generator

    file_path = tmp_path / "roundtrip-store.json"
    client = FileBasedNotionClient(str(file_path))

    with client as c:
        c.pages_create(make_page_payload("Symmetry Page"))

    # New client to load the same file
    with FileBasedNotionClient(str(file_path)) as nc:
        ids = [p["id"] for p in FileBasedNotionClient._store["store"]]
        assert "33333333-3333-3333-3333-333333333333" in ids or "22222222-2222-2222-2222-222222222222" in ids
