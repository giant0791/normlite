import pdb
import uuid
import pytest

from normlite.notion_sdk.client import InMemoryNotionClient, NotionError
from normlite.notion_sdk.getters import (
    get_object_type,
    get_title,
    get_title_rich_text,
    rich_text_to_plain_text,
)

@pytest.fixture
def client() -> InMemoryNotionClient:
    """Fixture that mimics the initialized state in engine."""
    client = InMemoryNotionClient()
    client._ensure_root()
    return client


@pytest.fixture
def database():
    """Fixture for _update_database_properties() helper method."""
    return {
        "id": "db1",
        "object": "database",
        "title": [{"text": {"content": "Original Database Name"}}],
        "properties": {
            "Name": {
                "id": "title",
                "type": "title",
                "title": {},
            },
            "Score": {
                "id": "J@cT",
                "type": "number",
                "number": {"format": "number"},
            },
            "Status": {
                "id": "STAT",
                "type": "status",
                "status": {},
            },
        },
    }

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def make_title_page(parent_page_id, title="My Page"):
    return {
        "parent": {
            "type": "page_id",
            "page_id": parent_page_id,
        },
        "properties": {
            "Title": {
                "title": [{"text": {"content": title}}]
            }
        },
    }


def make_database(parent_page_id: str, name: str = "Students"):
    """A databases.create payload in the Notion 2025-09-03 shape:
    the schema lives under initial_data_source.properties, not top-level."""
    return {
        "parent": {
            "type": "page_id",
            "page_id": parent_page_id,
        },
        "title": [{"text": {"content": name}}],
        "initial_data_source": {
            "properties": {
                "Name": {"title": {}},
                "Age": {"number": {}},
            }
        },
    }


def data_source_of(client, db):
    """Temporary bridge: reach a container's single data source directly from
    the store. Replace with data_sources.retrieve once #349 lands."""
    return client._store[db["data_sources"][0]["id"]]


def make_db_page(database_id, name="Alice", age=20):
    return {
        "parent": {
            "type": "database_id",
            "database_id": database_id,
        },
        "properties": {
            "Name": {
                "title": [{"text": {"content": name}}]
            },
            "Age": {
                "number": age
            },
        },
    }

def make_ds_page(data_source_id, name="Alice", age=20):
    """A pages.create payload for a row under a data source (2025-09-03 shape)."""
    return {
        "parent": {
            "type": "data_source_id",
            "data_source_id": data_source_id,
        },
        "properties": {
            "Name": {
                "title": [{"text": {"content": name}}]
            },
            "Age": {
                "number": age
            },
        },
    }

def rt(*parts: str):
    return [{"text": {"content": p}} for p in parts]

def create_database(client):
    page = client.pages_create(
        payload=make_title_page(
            client._ROOT_PAGE_ID_,
            title="root"
        )
    )
    
    db = client.databases_create(
        payload=make_database(page['id'])
    )

    return db


def test_databases_create_returns_container_with_one_data_source(client):
    # Arrange
    payload = make_database(client._ROOT_PAGE_ID_, "Students")

    # Act
    container = client.databases_create(payload=payload)

    # Assert: the container advertises exactly one data source...
    data_sources = container["data_sources"]
    assert len(data_sources) == 1

    # ...whose id is a real, distinct identifier from the container itself
    data_source_id = data_sources[0]["id"]
    assert data_source_id
    assert data_source_id != container["id"]

def test_databases_retrieve_data_source_advertises_id_and_name(client):
    # Faithfulness to Notion 2025-09-03: a container's data_sources entries carry
    # both an id AND a name (the data source's display name, which defaults to the
    # database title). databases.retrieve must round-trip that shape.
    container = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_, "Students")
    )

    retrieved = client.databases_retrieve(
        path_params={"database_id": container["id"]}
    )

    data_sources = retrieved["data_sources"]
    assert len(data_sources) == 1
    assert data_sources[0]["id"] == container["data_sources"][0]["id"]
    assert data_sources[0]["name"] == "Students"

def test_data_sources_retrieve_returns_schema_with_ids_and_types(client):
    # 2-phase reflection foundation (#349): the column schema (property ids +
    # resolved types) lives on the DATA SOURCE, so reflection must fetch it via
    # data_sources.retrieve. databases.retrieve returns only the container plus
    # its data_sources list, not the properties. This is the second-phase call
    # that user-column reflection will build on.
    container = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_, "Students")
    )
    data_source_id = container["data_sources"][0]["id"]

    ds = client.data_sources_retrieve(
        path_params={"data_source_id": data_source_id}
    )

    assert ds["object"] == "data_source"
    assert ds["id"] == data_source_id

    # user-column schema is present, each property carrying a resolved type and id
    name_prop = ds["properties"]["Name"]
    assert name_prop["type"] == "title"
    assert name_prop["id"] == "title"

    age_prop = ds["properties"]["Age"]
    assert age_prop["type"] == "number"
    assert age_prop["id"]     # a generated, non-empty property id

# ---------------------------------------------------------
# Root page behavior
# ---------------------------------------------------------

def test_root_page_exists_by_default(client):

    root = client.pages_retrieve(
        path_params={"page_id": client._ROOT_PAGE_ID_}
    )

    assert root["object"] == "page"
    assert root["properties"]["Title"]["title"][0]["text"]["content"] == "ROOT_PAGE"


# ---------------------------------------------------------
# Page under page rules
# ---------------------------------------------------------

def test_create_page_under_page_with_valid_title(client):
    page = client.pages_create(
        payload=make_title_page(client._ROOT_PAGE_ID_, "Child")
    )

    assert page["parent"]["page_id"] == client._ROOT_PAGE_ID_
    assert "Title" in page["properties"]
    assert page["properties"]["Title"]["id"] == "title"     # only the property id is returned among the properties


def test_page_under_page_rejects_multiple_properties(client):
    payload = {
        "parent": {
            "type": "page_id",
            "page_id": client._ROOT_PAGE_ID_,
        },
        "properties": {
            "Title": {"title": [{"text": {"content": "OK"}}]},
            "Extra": {"rich_text": [{"text": {"content": "NO"}}]},
        },
    }

    with pytest.raises(NotionError):
        client.pages_create(payload=payload)


def test_page_under_page_rejects_non_title_property(client):
    payload = {
        "parent": {
            "type": "page_id",
            "page_id": client._ROOT_PAGE_ID_,
        },
        "properties": {
            "Foo": {"rich_text": [{"text": {"content": "NO"}}]},
        },
    }

    with pytest.raises(NotionError):
        client.pages_create(payload=payload)


# ---------------------------------------------------------
# Database creation rules
# ---------------------------------------------------------

def test_create_database_under_page(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    assert db["object"] == "database"

    ds = data_source_of(client, db)
    assert "Name" in ds["properties"]
    assert "Age" in ds["properties"]

    assert ds["properties"]["Name"]["type"] == "title"
    assert ds["properties"]["Name"]["id"] == "title"

    assert ds["properties"]["Age"]["type"] == "number"
    assert isinstance(ds["properties"]["Age"]["id"], str)


def test_database_cannot_be_created_under_database(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    payload = make_database(db["id"])

    payload["parent"] = {
        "type": "database_id",
        "database_id": db["id"],
    }

    with pytest.raises(NotionError):
        client.databases_create(payload=payload)


def test_page_cannot_be_parented_to_a_database_container(client):
    # Arrange: a real database (container) exists under root
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    # A page payload that tries to hang directly off the database container
    payload = make_db_page(db["id"], "Bob", 42)
    assert payload["parent"]["type"] == "database_id"  # guard: parent really is a container

    # Act / Assert: the container is not a valid parent for a page
    with pytest.raises(NotionError):
        client.pages_create(payload=payload)


def test_database_cannot_be_parented_to_a_data_source(client):
    # Arrange: an existing database gives us a real data_source id to point at
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )
    ds = data_source_of(client, db)

    # A database payload that tries to nest under a data source
    payload = make_database(client._ROOT_PAGE_ID_)
    payload["parent"] = {"type": "data_source_id", "data_source_id": ds["id"]}

    # Act / Assert: a data source is not a valid parent for a database container
    with pytest.raises(NotionError):
        client.databases_create(payload=payload)


def test_data_source_records_its_database_as_parent(client):
    # Arrange / Act: create a database container with its single data source
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )
    ds = data_source_of(client, db)

    # Assert: the data source hangs off the database container
    assert ds["parent"]["type"] == "database_id"
    assert ds["parent"]["database_id"] == db["id"]

# ---------------------------------------------------------
# Page under data source rules (step 3)
# ---------------------------------------------------------

def test_page_can_be_created_under_a_data_source(client):
    # Arrange: a database and its single data source
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )
    ds = data_source_of(client, db)

    # A row that hangs off the data source (the new queryable surface)
    payload = make_db_page(db["id"], "Bob", 42)
    payload["parent"] = {"type": "data_source_id", "data_source_id": ds["id"]}

    # Act
    page = client.pages_create(payload=payload)

    # Assert: the row is a page parented to the data source
    assert page["object"] == "page"
    assert page["parent"]["type"] == "data_source_id"
    assert page["parent"]["data_source_id"] == ds["id"]


def test_data_sources_query_returns_rows_for_data_source(client):
    # Arrange: a database with its data source, and two rows under the data source
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )
    ds = data_source_of(client, db)

    def row(name, age):
        payload = make_db_page(db["id"], name, age)
        payload["parent"] = {"type": "data_source_id", "data_source_id": ds["id"]}
        return payload

    p1 = client.pages_create(payload=row("Alice", 20))
    p2 = client.pages_create(payload=row("Bob", 30))

    # Act: query the data source (the new SELECT surface)
    result = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={},  # no filter
    )

    # Assert: both rows come back
    assert result["object"] == "list"
    assert result["has_more"] is False
    assert len(result["results"]) == 2
    assert {p["id"] for p in result["results"]} == {p1["id"], p2["id"]}


def test_data_sources_query_paginates_with_page_size_and_cursor(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )
    ds = data_source_of(client, db)

    p1 = client.pages_create(payload=make_ds_page(ds["id"], "Alice", 20))
    p2 = client.pages_create(payload=make_ds_page(ds["id"], "Bob", 30))
    p3 = client.pages_create(payload=make_ds_page(ds["id"], "Carol", 40))

    # First page: ask for 2 of the 3 matching rows.
    page_one = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={"page_size": 2},
    )

    assert len(page_one["results"]) == 2
    assert page_one["has_more"] is True
    assert page_one["next_cursor"] is not None

    # Second page: hand the cursor back to fetch the remainder.
    page_two = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={"page_size": 2, "start_cursor": page_one["next_cursor"]},
    )

    assert len(page_two["results"]) == 1
    assert page_two["has_more"] is False
    assert page_two["next_cursor"] is None

    # The two pages together are all 3 rows, in order, with no overlap.
    seen_ids = [p["id"] for p in page_one["results"]] + \
               [p["id"] for p in page_two["results"]]
    assert seen_ids == [p1["id"], p2["id"], p3["id"]]


def test_data_sources_query_without_payload_returns_all_rows(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )
    ds = data_source_of(client, db)

    p1 = client.pages_create(payload=make_ds_page(ds["id"], "Alice", 20))
    p2 = client.pages_create(payload=make_ds_page(ds["id"], "Bob", 30))

    # No payload at all — the parameter defaults to None.
    result = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
    )

    assert len(result["results"]) == 2
    assert {p["id"] for p in result["results"]} == {p1["id"], p2["id"]}


def test_data_sources_query_sorts_by_number_ascending(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )
    ds = data_source_of(client, db)

    # Insert out of order so a passing test can only be the sort's doing.
    client.pages_create(payload=make_ds_page(ds["id"], "A", 30))
    client.pages_create(payload=make_ds_page(ds["id"], "B", 10))
    client.pages_create(payload=make_ds_page(ds["id"], "C", 20))

    result = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={
            "sorts": [
                {"property": "Age", "direction": "ascending"}
            ]
        },
    )

    ages = [p["properties"]["Age"]["number"] for p in result["results"]]
    assert ages == [10, 20, 30]

def test_data_sources_query_with_filter_properties_projection(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )
    ds = data_source_of(client, db)

    client.pages_create(payload=make_ds_page(ds["id"], "Alice", 20))

    result = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        query_params={"filter_properties": ["Name"]},
        payload={},
    )

    props = result["results"][0]["properties"]
    assert "Name" in props
    assert "Age" not in props


def test_data_sources_query_paginates_and_projects_together(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )
    ds = data_source_of(client, db)

    client.pages_create(payload=make_ds_page(ds["id"], "A", 1))
    client.pages_create(payload=make_ds_page(ds["id"], "B", 2))
    client.pages_create(payload=make_ds_page(ds["id"], "C", 3))

    result = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        query_params={"filter_properties": ["Name"]},
        payload={"page_size": 2},
    )

    # Windowing still holds: projection must not re-expand the page.
    assert len(result["results"]) == 2
    assert result["has_more"] is True

    # ... and every row in the window is projected.
    for p in result["results"]:
        assert "Name" in p["properties"]
        assert "Age" not in p["properties"]

def test_data_sources_query_requires_data_source_id(client):
    with pytest.raises(NotionError):
        client.data_sources_query(
            path_params={},
            payload={},
        )

def test_data_sources_query_returns_empty_list_when_no_match(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )
    ds = data_source_of(client, db)
    
    client.pages_create(payload=make_ds_page(ds["id"], "Alice", 20))
    
    result = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={
            "filter": {
                "property": "Age",
                "number": {"equals": 99},
            }
        },      
    )           
    
    assert result["results"] == []
    assert result["has_more"] is False

def test_data_sources_query_does_not_return_deleted_rows_when_in_trash_false(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )
    ds = data_source_of(client, db)

    p1 = client.pages_create(payload=make_ds_page(ds["id"], "Alice", 20))
    p2 = client.pages_create(payload=make_ds_page(ds["id"], "Bob", 30))
    
    client.pages_update(
        path_params={"page_id": p1["id"]},
        payload={"in_trash": True},
    )

    result = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={"in_trash": False},
    )   
    
    assert len(result["results"]) == 1
    assert {p["id"] for p in result["results"]} == {p2["id"]}

def test_data_sources_query_skips_deleted_rows_by_default(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )
    ds = data_source_of(client, db)
    
    p1 = client.pages_create(payload=make_ds_page(ds["id"], "Alice", 20))
    p2 = client.pages_create(payload=make_ds_page(ds["id"], "Bob", 30))
    
    client.pages_update(
        path_params={"page_id": p1["id"]},
        payload={"in_trash": True},
    )

    # No in_trash key at all — the default must still hide the trashed row.
    result = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={},
    )
    
    assert len(result["results"]) == 1
    assert {p["id"] for p in result["results"]} == {p2["id"]}

def test_data_sources_query_ignores_in_trash_true_and_still_skips_deleted(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )
    ds = data_source_of(client, db)

    p1 = client.pages_create(payload=make_ds_page(ds["id"], "Alice", 20))
    p2 = client.pages_create(payload=make_ds_page(ds["id"], "Bob", 30))

    client.pages_update(
        path_params={"page_id": p1["id"]},
        payload={"in_trash": True},
    )

    # data_sources.query has no in_trash parameter — a truthy value must be
    # ignored, not honoured. The trashed row stays hidden.
    result = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={"in_trash": True},
    )

    assert len(result["results"]) == 1
    assert {p["id"] for p in result["results"]} == {p2["id"]}

def test_data_sources_query_with_simple_filter_number_equals(client):
      db = client.databases_create(payload=make_database(client._ROOT_PAGE_ID_))
      ds = data_source_of(client, db)
  
      client.pages_create(payload=make_ds_page(ds["id"], "Alice", 20))
      client.pages_create(payload=make_ds_page(ds["id"], "Bob", 30))
  
      result = client.data_sources_query(
          path_params={"data_source_id": ds["id"]},
          payload={
              "filter": {
                  "property": "Age",
                  "number": {"equals": 30},
              }
          },      
      )           
              
      assert len(result["results"]) == 1
      assert result["results"][0]["properties"]["Age"]["number"] == 30

def test_data_sources_query_with_simple_filter_number_equals(client):
    db = client.databases_create(payload=make_database(client._ROOT_PAGE_ID_))
    ds = data_source_of(client, db)

    client.pages_create(payload=make_ds_page(ds["id"], "Alice", 20))
    client.pages_create(payload=make_ds_page(ds["id"], "Bob", 30))

    result = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={
            "filter": {
                "property": "Age",
                "number": {"equals": 30},
            }
        },      
    )           
            
    assert len(result["results"]) == 1
    assert result["results"][0]["properties"]["Age"]["number"] == 30

def test_data_sources_query_with_title_contains_filter(client):
    db = client.databases_create(payload=make_database(client._ROOT_PAGE_ID_))
    ds = data_source_of(client, db)

    client.pages_create(payload=make_ds_page(ds["id"], "Alice", 20))
    client.pages_create(payload=make_ds_page(ds["id"], "Bob", 30))
    client.pages_create(payload=make_ds_page(ds["id"], "Alicia", 40))

    result = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={
            "filter": {
                "property": "Name",
                "title": {"contains": "Ali"},
            }
        },      
    )           
            
    names = [   
        p["properties"]["Name"]["title"][0]["text"]["content"]
        for p in result["results"]
    ]
    assert set(names) == {"Alice", "Alicia"}
      
def test_data_sources_query_with_and_filter(client):
    db = client.databases_create(payload=make_database(client._ROOT_PAGE_ID_))
    ds = data_source_of(client, db)

    client.pages_create(payload=make_ds_page(ds["id"], "Alice", 20))
    client.pages_create(payload=make_ds_page(ds["id"], "Alice", 30))
    client.pages_create(payload=make_ds_page(ds["id"], "Bob", 30))
    
    result = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={
            "filter": {
                "and": [
                    {"property": "Name", "title": {"equals": "Alice"}},
                    {"property": "Age", "number": {"equals": 30}},
                ]
            }
        },      
    )               
                    
    assert len(result["results"]) == 1
    page = result["results"][0]
    assert page["properties"]["Name"]["title"][0]["text"]["content"] == "Alice"
    assert page["properties"]["Age"]["number"] == 30

def test_data_sources_query_with_or_filter(client):
    db = client.databases_create(payload=make_database(client._ROOT_PAGE_ID_))
    ds = data_source_of(client, db)

    client.pages_create(payload=make_ds_page(ds["id"], "Alice", 20))
    client.pages_create(payload=make_ds_page(ds["id"], "Bob", 30))
    client.pages_create(payload=make_ds_page(ds["id"], "Charlie", 40))
    
    result = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={
            "filter": {
                "or": [
                    {"property": "Name", "title": {"equals": "Alice"}},
                    {"property": "Age", "number": {"equals": 40}},
                ]
            }
        },      
    )               
                    
    names = {
        p["properties"]["Name"]["title"][0]["text"]["content"]
        for p in result["results"]
    }   
    assert names == {"Alice", "Charlie"}

def test_data_sources_query_last_page_when_rows_are_exact_multiple_of_page_size(client):
    db = client.databases_create(payload=make_database(client._ROOT_PAGE_ID_))
    ds = data_source_of(client, db)

    # 4 rows with page_size=2 → the set drains exactly on page 2.
    client.pages_create(payload=make_ds_page(ds["id"], "Alice", 20))
    client.pages_create(payload=make_ds_page(ds["id"], "Bob", 30))
    client.pages_create(payload=make_ds_page(ds["id"], "Carol", 40))
    client.pages_create(payload=make_ds_page(ds["id"], "Dave", 50))
    
    page_one = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={"page_size": 2},
    )
    page_two = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={"page_size": 2, "start_cursor": page_one["next_cursor"]},
    )
        
    assert len(page_two["results"]) == 2
    assert page_two["has_more"] is False
    assert page_two["next_cursor"] is None

def test_data_sources_query_paginates_the_filtered_set_not_the_raw_store(client):
    db = client.databases_create(payload=make_database(client._ROOT_PAGE_ID_))
    ds = data_source_of(client, db)

    # 5 rows in the store, but only 3 match the filter.
    m1 = client.pages_create(payload=make_ds_page(ds["id"], "Alice", 30))
    client.pages_create(payload=make_ds_page(ds["id"], "Bob", 99))
    m2 = client.pages_create(payload=make_ds_page(ds["id"], "Carol", 30))
    client.pages_create(payload=make_ds_page(ds["id"], "Dave", 99))
    m3 = client.pages_create(payload=make_ds_page(ds["id"], "Eve", 30))
    
    age_is_30 = {"property": "Age", "number": {"equals": 30}}

    page_one = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={"filter": age_is_30, "page_size": 2},
    )
    page_two = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={"filter": age_is_30, "page_size": 2, "start_cursor": page_one["next_cursor"]},
    )
        
    # Page sizes follow the filtered set (3 matches → 2 then 1), not the raw store (5).
    assert len(page_one["results"]) == 2
    assert page_one["has_more"] is True
    assert len(page_two["results"]) == 1
    assert page_two["has_more"] is False
    assert page_two["next_cursor"] is None
    
    # Only the matching rows appear, in order; the non-matching rows are never paged in.
    seen_ids = [p["id"] for p in page_one["results"]] + \
                [p["id"] for p in page_two["results"]]
    assert seen_ids == [m1["id"], m2["id"], m3["id"]]


def test_data_sources_query_sorts_by_title_descending(client):
    db = client.databases_create(payload=make_database(client._ROOT_PAGE_ID_))
    ds = data_source_of(client, db)

    # Insert out of descending order so a pass can only be the sort's doing.
    client.pages_create(payload=make_ds_page(ds["id"], "Bob", 10))
    client.pages_create(payload=make_ds_page(ds["id"], "Alice", 10))
    client.pages_create(payload=make_ds_page(ds["id"], "Charlie", 10))

    result = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={
            "sorts": [
                {"property": "Name", "direction": "descending"}
            ]
        },
    )

    names = [
        p["properties"]["Name"]["title"][0]["text"]["content"]
        for p in result["results"]
    ]
    assert names == ["Charlie", "Bob", "Alice"]


def test_data_sources_query_multiple_sorts_are_applied_in_order(client):
    db = client.databases_create(payload=make_database(client._ROOT_PAGE_ID_))
    ds = data_source_of(client, db)

    # Two share Age=30; the secondary Name sort must break that tie.
    client.pages_create(payload=make_ds_page(ds["id"], "Bob", 30))
    client.pages_create(payload=make_ds_page(ds["id"], "Alice", 30))
    client.pages_create(payload=make_ds_page(ds["id"], "Charlie", 20))

    result = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={
            "sorts": [
                {"property": "Age", "direction": "ascending"},
                {"property": "Name", "direction": "ascending"},
            ]
        },
    )

    names = [
        p["properties"]["Name"]["title"][0]["text"]["content"]
        for p in result["results"]
    ]
    # Age asc puts Charlie (20) first; among the Age=30 pair, Name asc breaks the tie.
    assert names == ["Charlie", "Alice", "Bob"]


def test_data_sources_query_sorts_the_filtered_set(client):
    db = client.databases_create(payload=make_database(client._ROOT_PAGE_ID_))
    ds = data_source_of(client, db)

    client.pages_create(payload=make_ds_page(ds["id"], "Alice", 30))
    client.pages_create(payload=make_ds_page(ds["id"], "Bob", 20))
    client.pages_create(payload=make_ds_page(ds["id"], "Charlie", 30))

    result = client.data_sources_query(
        path_params={"data_source_id": ds["id"]},
        payload={
            "filter": {
                "property": "Age",
                "number": {"equals": 30},
            },
            "sorts": [
                {"property": "Name", "direction": "ascending"}
            ],
        },
    )

    names = [
        p["properties"]["Name"]["title"][0]["text"]["content"]
        for p in result["results"]
    ]
    # Bob (Age=20) is filtered out; the survivors come back Name-ascending.
    assert names == ["Alice", "Charlie"]

# ---------------------------------------------------------
# Page under database rules (schema enforcement)
# ---------------------------------------------------------

def test_create_page_under_database_with_exact_schema(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )
    ds = data_source_of(client, db)

    # 2025-09-03: rows parent to the data source, whose schema they must match.
    page = client.pages_create(
        payload=make_ds_page(ds["id"], "Bob", 42)
    )

    props = page["properties"]

    assert set(props.keys()) == {"Name", "Age"}
    assert props["Name"]["id"] == ds["properties"]["Name"]["id"]
    assert props["Age"]["id"] == ds["properties"]["Age"]["id"]


def test_page_under_database_rejects_missing_property(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    payload = {
        "parent": {
            "type": "database_id",
            "database_id": db["id"],
        },
        "properties": {
            "Name": {
                "title": [{"text": {"content": "Alice"}}]
            }
        },
    }

    with pytest.raises(NotionError):
        client.pages_create(payload=payload)


def test_page_under_database_rejects_extra_property(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    payload = {
        "parent": {
            "type": "database_id",
            "database_id": db["id"],
        },
        "properties": {
            "Name": {
                "title": [{"text": {"content": "Alice"}}]
            },
            "Age": {
                "number": 30
            },
            "Extra": {
                "number": 99
            },
        },
    }

    with pytest.raises(NotionError):
        client.pages_create(payload=payload)


def test_page_under_database_rejects_type_mismatch(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    payload = {
        "parent": {
            "type": "database_id",
            "database_id": db["id"],
        },
        "properties": {
            "Name": {
                "rich_text": [{"text": {"content": "Alice"}}]
            },
            "Age": {
                "number": 20
            },
        },
    }

    with pytest.raises(NotionError):
        client.pages_create(payload=payload)


# ---------------------------------------------------------
# Store invariants
# ---------------------------------------------------------

def test_store_and_returned_object_are_consistent(client):
    page = client.pages_create(
        payload=make_title_page(client._ROOT_PAGE_ID_, "Consistency")
    )

    retrieved_page = client.pages_retrieve(path_params={"page_id": page["id"]})

    stored = client._get_by_id(page["id"])

    assert stored is not page  # deep copy returned
    assert stored["id"] == retrieved_page["id"]
    assert stored["properties"] == retrieved_page["properties"]


# ---------------------------------------------------------
# Page update behavior
# ---------------------------------------------------------

def test_pages_update_archived_flag(client):
    page = client.pages_create(
        payload=make_title_page(client._ROOT_PAGE_ID_)
    )

    updated = client.pages_update(
        path_params={"page_id": page["id"]},
        payload={"archived": True},
    )

    assert updated["archived"] is True

    stored = client._get_by_id(page["id"])
    assert stored["archived"] is True


def test_pages_update_properties(client):
    page = client.pages_create(
        payload=make_title_page(client._ROOT_PAGE_ID_)
    )

    updated = client.pages_update(
        path_params={"page_id": page["id"]},
        payload={
            "properties": {
                "Title": {
                    "title": [{"text": {"content": "Updated"}}]
                }
            }
        },
    )

    assert (
        updated["properties"]["Title"]["title"][0]["text"]["content"]
        == "Updated"
    )

def test_pages_update_in_trash_flag(client):
    page = client.pages_create(
        payload=make_title_page(client._ROOT_PAGE_ID_)
    )

    updated = client.pages_update(
        path_params={"page_id": page["id"]},
        payload={"in_trash": True},
    )

    assert updated["in_trash"] is True

    stored = client._get_by_id(page["id"])
    assert stored["in_trash"] is True

# ----------------------------------------------------------
# Normalization tests
# Note: All tests deliberately avoid getters when verifying
# normalization invariants, in order to avoid circular trusts
# 
# IMPORTANT:
#     Do not use a consumer to validate a producer.
# 
# getters are allowed in behavioral tests, because they 
# answers:
#     “Does the public API behave correctly given canonical input?”
# ----------------------------------------------------------

def test_page_under_page_title_is_normalized(client):
    root = client.pages_create(
        payload= {
            'parent': {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            'properties': {
                "Name": {
                    "title": [{"text": {"content": "ROOT"}}]
                }
            },
        }
    )

    page = client.pages_create(
        payload= {
            'parent': {"type": "page_id", "page_id": root["id"]},
            'properties': {
                "Name": {
                    "title": [{"text": {"content": "child"}}]
                }
            },
        }
    )

    # always retrieve the page as pages_create() does not return the property values
    retrieved_page = client.pages_retrieve(path_params={"page_id": page["id"]})

    title_rt = get_title_rich_text(retrieved_page)

    assert isinstance(title_rt, list)
    assert title_rt[0]["text"]["content"] == "child"
    assert get_title(retrieved_page) == "child"

def test_page_under_page_rejects_extra_properties(client):
    root = client.pages_create(
        payload= {
            'parent': {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            'properties': {
                "Name": {
                    "title": [{"text": {"content": "ROOT"}}]
                }
            },
        }
    )

    with pytest.raises(NotionError):
        _ = client.pages_create(
            payload= {
                'parent': {"type": "page_id", "page_id": root["id"]},
                'properties': {
                    "Name": {"title": rt("bad")},
                    "Extra": {"rich_text": rt("nope")},                },
            }
        )

def test_database_schema_is_finalized(client):
    db = client.databases_create(payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": rt("tables"),
            "initial_data_source": {
                "properties": {
                    "table_name": {"title": {}},
                    "schema": {"rich_text": {}},
                },
            },
        }
    )

    props = data_source_of(client, db)["properties"]

    assert props["table_name"]["type"] == "title"
    assert props["table_name"]["id"] == "title"

    assert props["schema"]["type"] == "rich_text"
    assert "id" in props["schema"]

def test_database_title_is_normalized(client):
    db = client.databases_create(
        payload= {
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "tables"}}],
            "initial_data_source": {"properties": {"name": {"title": {}}}},
        }
    )

    assert get_object_type(db) == "database"
    assert get_title(db) == "tables"

def test_page_under_database_properties_are_normalized(client):
    db = client.databases_create(
        payload={ 
            'parent': {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            'title': rt("tables"),
            'initial_data_source': {
                'properties': {
                    'table_name': {"title": {}},
                    'schema': {"rich_text": {}},
                }
            }
        }
    )

    page = client.pages_create(
        payload={
            'parent': {"type": "data_source_id", "data_source_id": db["data_sources"][0]["id"]},
            'properties':{
                "table_name": {"title": rt("users")},
                "schema": {"rich_text": rt("public")},
            },
        }
    )

    retrieved_page = client.pages_retrieve(path_params={"page_id": page["id"]})

    props = retrieved_page["properties"]

    assert props["table_name"]["type"] == "title"
    assert props["table_name"]["title"][0]["text"]["content"] == "users"

    assert props["schema"]["type"] == "rich_text"
    assert props["schema"]["rich_text"][0]["text"]["content"] == "public"

def test_page_schema_mismatch_raises(client):
    db = client.databases_create(
        payload={            
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": rt("tables"),
            "initial_data_source": {"properties": {"name": {"title": {}}}},
        }
    )

    with pytest.raises(NotionError):
        client.pages_create(
            payload={
                "parent": {"type": "database_id", "database_id": db["id"]},
                "properties": {
                    "name": {"title": rt("ok")},
                    "extra": {"rich_text": rt("boom")},
                },
            }
        )

def test_get_title_raises_on_invalid_object(client):
    bad_obj = {"object": "page", "properties": {}}

    # normalization invariant broken → accessor must fail
    with pytest.raises(NotionError):
        norm_obj = client._normalize_property("fake", bad_obj)
        get_title(norm_obj)

def test_rich_text_normalization_uses_text_content(client):
    rich = [
        {"text": {"content": "foo"}, "plain_text": "ignored"},
        {"text": {"content": "bar"}},
    ]

    normalized_rich = client._normalize_rich_text(rich)
    assert rich_text_to_plain_text(normalized_rich) == "foobar"

def test_get_by_title_finds_page(client):
    root = client.pages_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "properties":{"Name": {"title": rt("ROOT")}},
        }
    )

    page = client.pages_create(
        payload={
            "parent": {"type": "page_id", "page_id": root["id"]},
            "properties": {"Name": {"title": rt("information_schema")}},
        }
    )

    result = client._get_by_title("information_schema", "page")

    assert result["object"] == "list"
    assert len(result["results"]) == 1
    assert result["results"][0]["id"] == page["id"]

def test_store_contains_only_normalized_objects(client):
    page = client.pages_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "properties": {"Name": {"title": rt("X")}},
        }
    )

    stored = client._store[page["id"]]

    title_prop = next(
        p for p in stored["properties"].values()
        if p["type"] == "title"
    )

    assert isinstance(title_prop["title"], list)
    assert "text" in title_prop["title"][0]
    assert "content" in title_prop["title"][0]["text"]

# ------------------------------------------------------------
# databases update tests
# ------------------------------------------------------------

# NOTE: the former `_update_database_properties` helper + its 8 direct tests were
# retired in #348 — under Notion 2025-09-03 a database container has no schema
# surface (user columns live on the data source), so databases.update no longer
# performs property edits. See test_database_update_rejects_schema_properties below.

def test_database_update_title(client, database):
    database_obj = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Original Database Name"}}],
            "initial_data_source": {"properties": database["properties"]}
        }
    )
    
    result = client.databases_update(
        path_params={"database_id": database_obj["id"]},
        payload={
            "title": [{"text": {"content": "Updated Database Name"}}]
        },
    )

    assert result["title"][0]["text"]["content"] == "Updated Database Name"

def test_database_update_title_invalid_type(client, database):
    database_obj = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Original Database Name"}}],
            "initial_data_source": {"properties": database["properties"]}
        }
    )
    
    with pytest.raises(NotionError, match="rich_text"):
        client.databases_update(
            path_params={"database_id": database_obj["id"]},
            payload={"title": "Invalid"},
        )

def test_database_update_rejects_schema_properties(client, database):
    # Notion 2025-09-03: the database container has no schema surface — user
    # columns live on the data source, edited via data_sources.update. So
    # databases.update is narrowed to container-level attrs (title / in_trash /
    # parent) and rejects a `properties` body param.
    database_obj = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Original Database Name"}}],
            "initial_data_source": {"properties": database["properties"]}
        }
    )

    with pytest.raises(NotionError, match="properties"):
        client.databases_update(
            path_params={"database_id": database_obj["id"]},
            payload={
                "properties": {
                    "Score": {"number": {"format": "percent"}}
                },
            },
        )

# ------------------------------------------------------------
# search tests
# ------------------------------------------------------------

def test_search_no_filter_returns_all(client):
    db1 = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_, name='students_v1')
    )

    db2 = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_, name='students_v1')
    )

    result = client.search()
    expected = [obj for obj in client._store.values()]

    assert result['results'] == expected

def test_search_for_databases_no_title_returns_all(client):
    db1 = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_, name='students_v1')
    )

    db2 = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_, name='students_v2')
    )

    result = client.search(
        payload={
            'filter': {
                'property': 'object',
                'value': 'database'
            }
        }
    )

    assert result['results'] == [db1, db2]
 
def test_search_for_page_or_database_query_returns_matching_only(client):
    db1 = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_, name='students_v1')
    )

    database1 = client.databases_retrieve(path_params={"database_id": db1["id"]})

    pg1 = client.pages_create(
        payload=make_title_page(client._ROOT_PAGE_ID_, title='students_v1')
    )

    page1 = client.pages_retrieve(path_params={"page_id": pg1["id"]})

    result = client.search(
        payload={
            'query': 'students_v1'
        }
    )

    assert result['results'] == [database1, page1]

def test_search_for_database_query_returns_matching_only(client):
    db1 = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_, name='students_v1')
    )

    pg1 = client.pages_create(
        payload=make_title_page(client._ROOT_PAGE_ID_, title='students_v1')
    )

    result = client.search(
        payload={
            'query': 'students_v1',
            'filter': {
                'property': 'object',
                'value': 'database'
            }
        }
    )

    assert result['results'] == [db1]

def test_search_for_page_query_returns_matching_only(client):
    db1 = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_, name='students_v1')
    )

    pg1 = client.pages_create(
        payload=make_title_page(client._ROOT_PAGE_ID_, title='students_v1')
    )

    page1 = client.pages_retrieve(path_params={"page_id": pg1["id"]})
    result = client.search(
        payload={
            'query': 'students_v1',
            'filter': {
                'property': 'object',
                'value': 'page'
            }
        }
    )

    assert result['results'] == [page1]

def test_search_returns_exact_match_only(client):
    db1 = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_, name='students')
    )

    db2 = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_, name='students_v1')
    )

    db3 = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_, name='students_v2')
    )

    result = client.search(
        payload={
            'query': 'students',
            'filter': {
                'property': 'object',
                'value': 'database'
            }
        }
    )

    assert result['results'] == [db1]
  
def test_search_filter_is_not_a_dict_raises(client):
    with pytest.raises(NotionError) as exc:
        result = client.search(
            payload={
                'filter': 'page'
            }
        )

    assert exc.value.code == 'invalid_json'
    assert exc.value.status_code == 400
    assert 'body.filter should be an object (not a string).' in str(exc.value)

def test_search_filter_property_missing_raises(client):
    with pytest.raises(NotionError) as exc:
        result = client.search(
            payload={
                'filter': {
                    'value': 'page'
                }
            }
        )

    assert exc.value.code == 'invalid_json'
    assert exc.value.status_code == 400
    assert 'body.property should be defined.' in str(exc.value)

def test_search_filter_value_missing_raises(client):
    with pytest.raises(NotionError) as exc:
        result = client.search(
            payload={
                'filter': {
                    'property': 'object'
                }
            }
        )

    assert exc.value.code == 'invalid_json'
    assert exc.value.status_code == 400
    assert 'body.value should be defined.' in str(exc.value)

def test_search_filter_value_not_a_page_or_database_raises(client):
    with pytest.raises(NotionError) as exc:
        result = client.search(
            payload={
                'filter': {
                    'property': 'object',
                    'value': 'data_source'

                }
            }
        )

    assert exc.value.code == 'invalid_json'
    assert exc.value.status_code == 400
    assert "body.value should be either 'page' or 'database'." in str(exc.value)

def test_deleted_pages_have_in_trash_true(client):
    pass

def test_close_is_noop(client):
    client._store["abc"] = {"object": "page", "id": "abc"}
    client.close()
    assert "abc" in client._store

# ---------------------------------------------------------
# "relation" object behavior
# ---------------------------------------------------------

def test_pages_create_rejects_relation_value_that_is_not_a_list(client):
    # Arrange — two databases linked by a relation column
    courses = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Courses"}}],
            "initial_data_source": {"properties": {"Title": {"title": {}}}},
        }
    )
    students = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Students"}}],
            "initial_data_source": {
                "properties": {
                    "Name": {"title": {}},
                    "enrolled_in": {
                        "relation": {
                            "database_id": courses["id"],
                            "single_property": {},
                        },
                    },
                },
            },
        }
    )


    malformed_page = {
        "parent": {"type": "data_source_id", "data_source_id": students["data_sources"][0]["id"]},
        "properties": {
            "Name": {"title": [{"text": {"content": "Alice"}}]},
            "enrolled_in": {"relation": "not-a-list"},
        },
    }

    # Act + Assert
    with pytest.raises(NotionError):
        client.pages_create(payload=malformed_page)

def test_pages_create_rejects_relation_item_that_is_not_a_dict(client):
    # Arrange — two databases linked by a relation column
    courses = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Courses"}}],
            "initial_data_source": {"properties": {"Title": {"title": {}}}},
        }
    )
    students = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Students"}}],
            "initial_data_source": {
                "properties": {
                    "Name": {"title": {}},
                    "enrolled_in": {
                        "relation": {
                            "database_id": courses["id"],
                            "single_property": {},
                        },
                    },
                },
            },
        }
    )

    malformed_page = {
        "parent": {"type": "data_source_id", "data_source_id": students["data_sources"][0]["id"]},
        "properties": {
            "Name": {"title": [{"text": {"content": "Alice"}}]},
            "enrolled_in": {"relation": ["not-a-dict-just-a-string"]},
        },
    }

    # Act + Assert
    with pytest.raises(NotionError):
        client.pages_create(payload=malformed_page)

def test_pages_create_rejects_relation_item_dict_without_id(client):
    # Arrange — two databases linked by a relation column
    courses = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Courses"}}],
            "initial_data_source": {"properties": {"Title": {"title": {}}}},
        }
    )
    students = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Students"}}],
            "initial_data_source": {
                "properties": {
                    "Name": {"title": {}},
                    "enrolled_in": {
                        "relation": {
                            "database_id": courses["id"],
                            "single_property": {},
                        },
                    },
                },
            },
        }
    )

    malformed_page = {
        "parent": {"type": "data_source_id", "data_source_id": students["data_sources"][0]["id"]},
        "properties": {
            "Name": {"title": [{"text": {"content": "Alice"}}]},
            "enrolled_in": {"relation": [{"name": "Math 101"}]},
        },
    }

    # Act + Assert
    with pytest.raises(NotionError):
        client.pages_create(payload=malformed_page)

def test_pages_create_rejects_relation_item_id_that_is_not_a_string(client):
    # Arrange — two databases linked by a relation column
    courses = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Courses"}}],
            "initial_data_source": {"properties": {"Title": {"title": {}}}},
        }
    )
    students = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Students"}}],
            "initial_data_source": {
                "properties": {
                    "Name": {"title": {}},
                    "enrolled_in": {
                        "relation": {
                            "database_id": courses["id"],
                            "single_property": {},
                        },
                    },
                },
            },
        }
    )

    malformed_page = {
        "parent": {"type": "data_source_id", "data_source_id": students["data_sources"][0]["id"]},
        "properties": {
            "Name": {"title": [{"text": {"content": "Alice"}}]},
            "enrolled_in": {"relation": [{"id": 12345}]},
        },
    }

    # Act + Assert
    with pytest.raises(NotionError):
        client.pages_create(payload=malformed_page)

def test_pages_create_preserves_relation_property_on_retrieve(client):
    # Arrange — two databases linked by a relation column
    courses = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Courses"}}],
            "initial_data_source": {"properties": {"Title": {"title": {}}}},
        }
    )
    students = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Students"}}],
            "initial_data_source": {
                "properties": {
                    "Name": {"title": {}},
                    "enrolled_in": {
                        "relation": {
                            "database_id": courses["id"],
                            "single_property": {},
                        },
                    },
                },
            },
        }
    )

    course = client.pages_create(
        payload={
            "parent": {"type": "data_source_id", "data_source_id": courses["data_sources"][0]["id"]},
            "properties": {
                "Title": {"title": [{"text": {"content": "Math 101"}}]},
            },
        }
    )

    student = client.pages_create(
        payload={
            "parent": {"type": "data_source_id", "data_source_id": students["data_sources"][0]["id"]},
            "properties": {
                "Name": {"title": [{"text": {"content": "Alice"}}]},
                "enrolled_in": {"relation": [{"id": course["id"]}]},
            },
        }
    )

    # Act
    retrieved = client.pages_retrieve(path_params={"page_id": student["id"]})

    # Assert
    enrolled_in = retrieved["properties"]["enrolled_in"]
    assert enrolled_in["type"] == "relation"
    assert enrolled_in["relation"] == [{"id": course["id"]}]

def test_pages_update_replaces_relation_property(client):
    # Arrange — two databases linked by a relation column
    courses = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Courses"}}],
            "initial_data_source": {"properties": {"Title": {"title": {}}}},
        }
    )
    students = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Students"}}],
            "initial_data_source": {
                "properties": {
                    "Name": {"title": {}},
                    "enrolled_in": {
                        "relation": {
                            "database_id": courses["id"],
                            "single_property": {},
                        },
                    },
                },
            },
        }
    )

    math = client.pages_create(
        payload={
            "parent": {"type": "data_source_id", "data_source_id": courses["data_sources"][0]["id"]},
            "properties": {"Title": {"title": [{"text": {"content": "Math 101"}}]}},
        }
    )
    history = client.pages_create(
        payload={
            "parent": {"type": "data_source_id", "data_source_id": courses["data_sources"][0]["id"]},
            "properties": {"Title": {"title": [{"text": {"content": "History 101"}}]}},
        }
    )

    student = client.pages_create(
        payload={
            "parent": {"type": "data_source_id", "data_source_id": students["data_sources"][0]["id"]},
            "properties": {
                "Name": {"title": [{"text": {"content": "Alice"}}]},
                "enrolled_in": {"relation": [{"id": math["id"]}]},
            },
        }
    )

    # Act — replace [Math] with [History]
    client.pages_update(
        path_params={"page_id": student["id"]},
        payload={
            "properties": {
                "enrolled_in": {"relation": [{"id": history["id"]}]},
            }
        },
    )

    # Assert — only History remains, Math is gone
    retrieved = client.pages_retrieve(path_params={"page_id": student["id"]})
    assert retrieved["properties"]["enrolled_in"]["relation"] == [{"id": history["id"]}]

def test_pages_update_clears_relation_property_with_empty_list(client):
    # Arrange — two databases linked by a relation column
    courses = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Courses"}}],
            "initial_data_source": {"properties": {"Title": {"title": {}}}},
        }
    )
    students = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Students"}}],
            "initial_data_source": {
                "properties": {
                    "Name": {"title": {}},
                    "enrolled_in": {
                        "relation": {
                            "database_id": courses["id"],
                            "single_property": {},
                        },
                    },
                },
            },
        }
    )

    math = client.pages_create(
        payload={
            "parent": {"type": "data_source_id", "data_source_id": courses["data_sources"][0]["id"]},
            "properties": {"Title": {"title": [{"text": {"content": "Math 101"}}]}},
        }
    )

    student = client.pages_create(
        payload={
            "parent": {"type": "data_source_id", "data_source_id": students["data_sources"][0]["id"]},
            "properties": {
                "Name": {"title": [{"text": {"content": "Alice"}}]},
                "enrolled_in": {"relation": [{"id": math["id"]}]},
            },
        }
    )

    # Act — clear the relation
    client.pages_update(
        path_params={"page_id": student["id"]},
        payload={
            "properties": {
                "enrolled_in": {"relation": []},
            }
        },
    )

    # Assert — relation is empty
    retrieved = client.pages_retrieve(path_params={"page_id": student["id"]})
    assert retrieved["properties"]["enrolled_in"]["relation"] == []

def test_pages_update_rejects_relation_value_that_is_not_a_list(client):
    # Arrange — two databases linked by a relation column, and a valid student
    courses = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Courses"}}],
            "initial_data_source": {"properties": {"Title": {"title": {}}}},
        }
    )
    students = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "Students"}}],
            "initial_data_source": {
                "properties": {
                    "Name": {"title": {}},
                    "enrolled_in": {
                        "relation": {
                            "database_id": courses["id"],
                            "single_property": {},
                        },
                    },
                },
            },
        }
    )

    math = client.pages_create(
        payload={
            "parent": {"type": "data_source_id", "data_source_id": courses["data_sources"][0]["id"]},
            "properties": {"Title": {"title": [{"text": {"content": "Math 101"}}]}},
        }
    )

    student = client.pages_create(
        payload={
            "parent": {"type": "data_source_id", "data_source_id": students["data_sources"][0]["id"]},
            "properties": {
                "Name": {"title": [{"text": {"content": "Alice"}}]},
                "enrolled_in": {"relation": [{"id": math["id"]}]},
            },
        }
    )

    # Act + Assert — pages_update with malformed relation value must reject
    with pytest.raises(NotionError):
        client.pages_update(
            path_params={"page_id": student["id"]},
            payload={
                "properties": {
                    "enrolled_in": {"relation": "not-a-list"},
                }
            },
        )


def test_databases_query_endpoint_is_retired(client: InMemoryNotionClient):
    """The 2025-09-03 migration retires ``databases.query`` for ``data_sources.query``.

    Page queries now route to the data source, so the old endpoint must be
    un-routable: ``databases_query`` is no longer an allowed client operation and
    dispatching to it raises rather than silently returning an (empty) result set.
    """
    with pytest.raises(NotionError, match="Unknown or unsupported operation"):
        client("databases", "query", path_params={"database_id": "some-db-id"})