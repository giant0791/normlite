import json
import pytest
from pathlib import Path

from normlite.notion_sdk.client import FileBasedNotionClient, InMemoryNotionClient, NotionError

def bootstrap(client: InMemoryNotionClient):
    is_id = client.pages_create(
        payload = {
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "properties": {
                "Name": {"title": [{"text": {"content": "information_schema"}}]}
            },
        },
    )

    t_id = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": is_id},
            "title": [{"type": "text", "text": {"content": "tables"}}],
            "properties": {
                "table_name": {"title": {}},
                "table_schema": {"rich_text": {}},
                "table_catalog": {"rich_text": {}},
                "table_id": {"rich_text": {}},
            },
        }
    )

    client.pages_create(
        payload={
            "parent": {
                "type": "database_id",
                "database_id": t_id,
            },
            "properties": {
                "table_name": {
                    "title": [{"text": {"content": "tables"}}]
                },
                "table_schema": {
                    "rich_text": [{"text": {"content": "information_schema"}}]
                },
                "table_catalog": {
                    "rich_text": [{"text": {"content": "normlite"}}]
                },
                "table_id": {
                    "rich_text": [{"text": {"content": t_id}}]
                },
            },
        },
    )
    
# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def write_store(path: Path, *, version=1, objects=None):
    payload = {
        "version": version,
        "objects": objects if objects is not None else {},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


# ----------------------------------------------------------------------
# Construction & auto-load behavior
# ----------------------------------------------------------------------

def test_init_with_missing_file_creates_empty_store(tmp_path):
    path = tmp_path / "store.json"

    client = FileBasedNotionClient(path)

    assert client._store == {}
    assert not path.exists()


def test_init_with_existing_file_auto_loads(tmp_path):
    path = tmp_path / "store.json"
    write_store(path, objects={"abc": {"object": "page", "id": "abc"}})

    client = FileBasedNotionClient(path)

    assert "abc" in client._store
    assert client._store["abc"]["id"] == "abc"


def test_init_without_auto_load_does_not_touch_store(tmp_path):
    path = tmp_path / "store.json"
    write_store(path, objects={"abc": {"object": "page", "id": "abc"}})

    client = FileBasedNotionClient(path, auto_load=False)

    # invariant: _store is always empty after init if auto_load is False
    assert client._store == {}     
    assert path.exists()


# ----------------------------------------------------------------------
# Load()
# ----------------------------------------------------------------------

def test_load_missing_file_clears_store(tmp_path):
    path = tmp_path / "store.json"
    client = FileBasedNotionClient(path, auto_load=False)

    client._store["x"] = {"object": "page"}
    client.load()

    assert client._store == {}


def test_load_rejects_unsupported_version(tmp_path):
    path = tmp_path / "store.json"
    write_store(path, version=999)

    client = FileBasedNotionClient(path, auto_load=False)

    with pytest.raises(NotionError, match="Unsupported store version"):
        client.load()


def test_load_rejects_corrupted_store(tmp_path):
    path = tmp_path / "store.json"
    path.write_text(json.dumps({"version": 1}), encoding="utf-8")

    client = FileBasedNotionClient(path, auto_load=False)

    with pytest.raises(NotionError, match="Corrupted store"):
        client.load()


def test_load_deep_copies_objects(tmp_path):
    path = tmp_path / "store.json"
    original = {"abc": {"object": "page", "id": "abc"}}
    write_store(path, objects=original)

    client = FileBasedNotionClient(path, auto_load=False)
    client.load()

    client._store["abc"]["id"] = "mutated"
    assert original["abc"]["id"] == "abc"


# ----------------------------------------------------------------------
# Flush()
# ----------------------------------------------------------------------

def test_flush_writes_store_to_disk(tmp_path):
    path = tmp_path / "store.json"
    client = FileBasedNotionClient(path, auto_load=False)

    client._store["abc"] = {"object": "page", "id": "abc"}
    client.flush()

    data = json.loads(path.read_text())
    assert data["version"] == client.STORE_VERSION
    assert "abc" in data["objects"]


def test_flush_creates_parent_directories(tmp_path):
    path = tmp_path / "nested" / "store.json"
    client = FileBasedNotionClient(path, auto_load=False)

    client._store["abc"] = {"object": "page", "id": "abc"}
    client.flush()

    assert path.exists()


def test_flush_is_noop_when_read_only(tmp_path):
    path = tmp_path / "store.json"
    client = FileBasedNotionClient(path, read_only=True, auto_load=False)

    client._store["abc"] = {"object": "page", "id": "abc"}
    client.flush()

    assert not path.exists()


# ----------------------------------------------------------------------
# Clear()
# ----------------------------------------------------------------------

def test_clear_removes_store_and_file(tmp_path):
    path = tmp_path / "store.json"
    write_store(path, objects={"abc": {"object": "page", "id": "abc"}})

    client = FileBasedNotionClient(path)
    client.clear()

    assert client._store == {}
    assert not path.exists()


def test_clear_does_not_remove_file_when_read_only(tmp_path):
    path = tmp_path / "store.json"
    write_store(path)

    client = FileBasedNotionClient(path, read_only=True)
    client.clear()

    assert path.exists()


# ----------------------------------------------------------------------
# Context manager
# ----------------------------------------------------------------------

def test_context_manager_loads_and_flushes(tmp_path):
    path = tmp_path / "store.json"
    write_store(path)

    with FileBasedNotionClient(path) as client:
        client._store["abc"] = {"object": "page", "id": "abc"}

    data = json.loads(path.read_text())
    assert "abc" in data["objects"]


def test_context_manager_does_not_swallow_exceptions(tmp_path):
    path = tmp_path / "store.json"
    write_store(path)

    with pytest.raises(RuntimeError):
        with FileBasedNotionClient(path) as client:
            raise RuntimeError("boom")


def test_context_manager_respects_auto_flush_false(tmp_path):
    path = tmp_path / "store.json"
    write_store(path)

    with FileBasedNotionClient(path, auto_flush=False) as client:
        client._store["abc"] = {"object": "page", "id": "abc"}

    data = json.loads(path.read_text())
    assert "abc" not in data["objects"]
