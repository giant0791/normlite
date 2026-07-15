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

def write_store(path: Path, *, version=2, objects=None):
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


def test_load_rejects_old_shape_database_with_top_level_properties(tmp_path):
    # A genuine pre-2025-09-03 store: version 1 AND old shape. Under the upgrade a
    # database container carries NO top-level `properties` (its schema lives on a
    # separate `data_source` object); a top-level `properties` key is the tell-tale
    # of an old-format store. It must be rejected loudly, never silently mis-loaded.
    path = tmp_path / "store.json"
    write_store(
        path,
        version=1,
        objects={
            "db1": {
                "object": "database",
                "id": "db1",
                "properties": {"Name": {"title": {}}},
            },
        },
    )

    client = FileBasedNotionClient(path, auto_load=False)

    with pytest.raises(NotionError, match="predates the 2025-09-03 upgrade"):
        client.load()


def test_load_rejects_old_shape_page_parented_to_database_id(tmp_path):
    # The other tell-tale of a pre-2025-09-03 store: a page parented directly to a
    # `database_id`. Under the upgrade pages parent to `data_source_id`.
    path = tmp_path / "store.json"
    write_store(
        path,
        version=1,
        objects={
            "pg1": {
                "object": "page",
                "id": "pg1",
                "parent": {"type": "database_id", "database_id": "db1"},
                "properties": {},
            },
        },
    )

    client = FileBasedNotionClient(path, auto_load=False)

    with pytest.raises(NotionError, match="predates the 2025-09-03 upgrade"):
        client.load()


def test_load_accepts_new_shape_data_source_parented_to_database_id(tmp_path):
    # A `data_source` legitimately parents to `database_id` under the 2025-09-03
    # shape, so the old-shape "parented to database_id" guard MUST be scoped to
    # pages only — a data source must not trip it.
    path = tmp_path / "store.json"
    write_store(
        path,
        objects={
            "ds1": {
                "object": "data_source",
                "id": "ds1",
                "parent": {"type": "database_id", "database_id": "db1"},
                "properties": {"Name": {"id": "title", "type": "title", "title": {}}},
            },
        },
    )

    client = FileBasedNotionClient(path, auto_load=False)

    client.load()  # must not raise

    assert "ds1" in client._store


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
# Round-trip of the 2025-09-03 two-object store
# ----------------------------------------------------------------------

def test_flush_and_load_round_trips_new_two_object_store(tmp_path):
    # AC1: the new store shape survives a flush/reload cycle faithfully — a
    # `database` container with NO top-level `properties` advertising its
    # `data_sources`, a separate `data_source` object carrying the schema + its
    # title, and a page parented to the `data_source_id`.
    path = tmp_path / "store.json"

    writer = FileBasedNotionClient(path, auto_load=False)
    writer._ensure_root()

    is_id = writer.pages_create(
        payload={
            "parent": {"type": "page_id", "page_id": writer._ROOT_PAGE_ID_},
            "properties": {"Name": {"title": [{"text": {"content": "information_schema"}}]}},
        }
    )["id"]

    db = writer.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": is_id},
            "title": [{"type": "text", "text": {"content": "students"}}],
            "initial_data_source": {
                "properties": {
                    "Name": {"title": {}},
                    "Age": {"number": {}},
                },
            },
        }
    )
    db_id = db["id"]
    ds_id = db["data_sources"][0]["id"]

    writer.pages_create(
        payload={
            "parent": {"type": "data_source_id", "data_source_id": ds_id},
            "properties": {
                "Name": {"title": [{"text": {"content": "Alice"}}]},
                "Age": {"number": 30},
            },
        }
    )

    writer.flush()

    # Fresh client, same file: auto-loads the persisted store.
    reader = FileBasedNotionClient(path)

    # Database container: present, no top-level schema, advertises its data source.
    reloaded_db = reader._store[db_id]
    assert reloaded_db["object"] == "database"
    assert "properties" not in reloaded_db
    assert reloaded_db["data_sources"][0]["id"] == ds_id

    # Data source: a separate object carrying the schema and its title.
    reloaded_ds = reader._store[ds_id]
    assert reloaded_ds["object"] == "data_source"
    assert set(reloaded_ds["properties"]) == {"Name", "Age"}
    assert reloaded_ds["parent"] == {"type": "database_id", "database_id": db_id}
    assert reloaded_ds["title"] == reloaded_db["title"]

    # Page: parented to the data source, not the database.
    page = next(
        o for o in reader._store.values()
        if o["object"] == "page" and o["parent"].get("type") == "data_source_id"
    )
    assert page["parent"]["data_source_id"] == ds_id


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


# ----------------------------------------------------------------------
# close()
# ----------------------------------------------------------------------

def test_close_flushes_store_to_disk(tmp_path):
    path = tmp_path / "store.json"
    client = FileBasedNotionClient(path, auto_load=False)

    client._store["abc"] = {"object": "page", "id": "abc"}
    client.close()

    data = json.loads(path.read_text())
    assert "abc" in data["objects"]


def test_close_is_noop_when_read_only(tmp_path):
    path = tmp_path / "store.json"
    client = FileBasedNotionClient(path, read_only=True, auto_load=False)

    client._store["abc"] = {"object": "page", "id": "abc"}
    client.close()

    assert not path.exists()

def test_file_based_client_rejects_missing_path_in_read_only_mode(tmp_path):
    # Arrange: a file path inside tmp_path that we deliberately never create
    missing_path = tmp_path / "does-not-exist.json"
    assert not missing_path.exists()  # sanity check on the fixture

    # Act + Assert: construction must fail immediately — no broken-state client
    with pytest.raises(NotionError) as exc_info:
        FileBasedNotionClient(str(missing_path), read_only=True)

    # The exception must carry Notion's `invalid_request_url` vocabulary so callers
    # (and downstream tests that catch on .code) can react to it the same way they
    # would to a real Notion error.
    exc = exc_info.value
    assert exc.status_code == 400
    assert exc.code == "invalid_request_url"

    # The message must include the offending path so the user can act on it.
    assert str(missing_path) in exc.message
