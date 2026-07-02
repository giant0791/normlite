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

# ---------------------------------------------------------
# Page under database rules (schema enforcement)
# ---------------------------------------------------------

def test_create_page_under_database_with_exact_schema(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    page = client.pages_create(
        payload=make_db_page(db["id"], "Bob", 42)
    )

    props = page["properties"]

    # only property ids are returned
    assert set(props.keys()) == {"Name", "Age"}
    ds = data_source_of(client, db)
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

# ---------------------------------------------------------
# Database query API (databases_query)
# ---------------------------------------------------------

def test_databases_query_returns_pages_for_database(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    p1 = client.pages_create(payload=make_db_page(db["id"], "Alice", 20))
    p2 = client.pages_create(payload=make_db_page(db["id"], "Bob", 30))

    result = client.databases_query(
        path_params={"database_id": db["id"]},
        payload={},   # no filter
    )

    assert result["object"] == "list"
    assert result["has_more"] is False
    assert len(result["results"]) == 2

    ids = {p["id"] for p in result["results"]}
    assert ids == {p1["id"], p2["id"]}

def test_databases_query_does_not_return_deleted_pages_if_in_trash_false(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    p1 = client.pages_create(payload=make_db_page(db["id"], "Alice", 20))
    p2 = client.pages_create(payload=make_db_page(db["id"], "Bob", 30))

    result = client.databases_query(
        path_params={"database_id": db["id"]},
        payload={},   # no filter
    )

    assert len(result["results"]) == 2

    u1 = client.pages_update(
        path_params={"page_id": p1["id"]},
        payload={"in_trash": True},
    )

    result = client.databases_query(
        path_params={"database_id": db["id"]},
        payload={"in_trash": False},   # retrieve all non-deleted pages
    )

    assert len(result["results"]) == 1
    ids = {p["id"] for p in result["results"]}
    assert ids == {p2["id"]}

def test_databases_query_paginates_with_page_size_and_cursor(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    p1 = client.pages_create(payload=make_db_page(db["id"], "Alice", 20))
    p2 = client.pages_create(payload=make_db_page(db["id"], "Bob", 30))
    p3 = client.pages_create(payload=make_db_page(db["id"], "Carol", 40))

    # First page: ask for 2 of the 3 matching rows.
    page_one = client.databases_query(
        path_params={"database_id": db["id"]},
        payload={"page_size": 2},
    )

    assert len(page_one["results"]) == 2
    assert page_one["has_more"] is True
    assert page_one["next_cursor"] is not None

    # Second page: hand the cursor back to fetch the remainder.
    page_two = client.databases_query(
        path_params={"database_id": db["id"]},
        payload={"page_size": 2, "start_cursor": page_one["next_cursor"]},
    )

    assert len(page_two["results"]) == 1
    assert page_two["has_more"] is False
    assert page_two["next_cursor"] is None

    # The two pages together are all 3 rows, in order, with no overlap.
    seen_ids = [p["id"] for p in page_one["results"]] + \
               [p["id"] for p in page_two["results"]]
    assert seen_ids == [p1["id"], p2["id"], p3["id"]]


def test_databases_query_last_page_when_rows_are_exact_multiple_of_page_size(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    # 4 rows with page_size=2 → the set drains exactly on page 2.
    client.pages_create(payload=make_db_page(db["id"], "Alice", 20))
    client.pages_create(payload=make_db_page(db["id"], "Bob", 30))
    client.pages_create(payload=make_db_page(db["id"], "Carol", 40))
    client.pages_create(payload=make_db_page(db["id"], "Dave", 50))

    page_one = client.databases_query(
        path_params={"database_id": db["id"]},
        payload={"page_size": 2},
    )
    page_two = client.databases_query(
        path_params={"database_id": db["id"]},
        payload={"page_size": 2, "start_cursor": page_one["next_cursor"]},
    )

    # The second page exactly drains the set: no phantom page beyond it.
    assert len(page_two["results"]) == 2
    assert page_two["has_more"] is False
    assert page_two["next_cursor"] is None


def test_databases_query_paginates_the_filtered_set_not_the_raw_store(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    # 5 rows in the store, but only 3 match the filter.
    m1 = client.pages_create(payload=make_db_page(db["id"], "Alice", 30))
    client.pages_create(payload=make_db_page(db["id"], "Bob", 99))
    m2 = client.pages_create(payload=make_db_page(db["id"], "Carol", 30))
    client.pages_create(payload=make_db_page(db["id"], "Dave", 99))
    m3 = client.pages_create(payload=make_db_page(db["id"], "Eve", 30))

    age_is_30 = {"property": "Age", "number": {"equals": 30}}

    page_one = client.databases_query(
        path_params={"database_id": db["id"]},
        payload={"filter": age_is_30, "page_size": 2},
    )
    page_two = client.databases_query(
        path_params={"database_id": db["id"]},
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


def test_databases_query_returns_deleted_pages_if_in_trash_true(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    p1 = client.pages_create(payload=make_db_page(db["id"], "Alice", 20))
    p2 = client.pages_create(payload=make_db_page(db["id"], "Bob", 30))

    result = client.databases_query(
        path_params={"database_id": db["id"]},
        payload={},   # no filter
    )

    assert len(result["results"]) == 2

    u1 = client.pages_update(
        path_params={"page_id": p1["id"]},
        payload={"in_trash": True},
    )

    result = client.databases_query(
        path_params={"database_id": db["id"]},
        payload={"in_trash": True},   # retrieve all pages
    )

    assert len(result["results"]) == 2
    ids = {p["id"] for p in result["results"]}
    assert ids == {p1["id"], p2["id"]}

def test_databases_query_does_not_return_pages_from_other_databases(client):
    db1 = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )
    db2 = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    client.pages_create(payload=make_db_page(db1["id"], "Alice", 20))
    client.pages_create(payload=make_db_page(db2["id"], "Bob", 30))

    result = client.databases_query(
        path_params={"database_id": db1["id"]},
        payload={},
    )

    assert len(result["results"]) == 1
    assert result["results"][0]["parent"]["database_id"] == db1["id"]


def test_databases_query_with_simple_filter_number_equals(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    client.pages_create(payload=make_db_page(db["id"], "Alice", 20))
    client.pages_create(payload=make_db_page(db["id"], "Bob", 30))

    result = client.databases_query(
        path_params={"database_id": db["id"]},
        payload={
            "filter": {
                "property": "Age",
                "number": {
                    "equals": 30
                },
            }
        },
    )

    assert len(result["results"]) == 1
    assert (
        result["results"][0]["properties"]["Age"]["number"]
        == 30
    )


def test_databases_query_with_title_contains_filter(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    client.pages_create(payload=make_db_page(db["id"], "Alice", 20))
    client.pages_create(payload=make_db_page(db["id"], "Bob", 30))
    client.pages_create(payload=make_db_page(db["id"], "Alicia", 40))

    result = client.databases_query(
        path_params={"database_id": db["id"]},
        payload={
            "filter": {
                "property": "Name",
                "title": {
                    "contains": "Ali"
                },
            }
        },
    )

    names = [
        p["properties"]["Name"]["title"][0]["text"]["content"]
        for p in result["results"]
    ]

    assert set(names) == {"Alice", "Alicia"}


def test_databases_query_with_and_filter(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    client.pages_create(payload=make_db_page(db["id"], "Alice", 20))
    client.pages_create(payload=make_db_page(db["id"], "Alice", 30))
    client.pages_create(payload=make_db_page(db["id"], "Bob", 30))

    result = client.databases_query(
        path_params={"database_id": db["id"]},
        payload={
            "filter": {
                "and": [
                    {
                        "property": "Name",
                        "title": {"equals": "Alice"},
                    },
                    {
                        "property": "Age",
                        "number": {"equals": 30},
                    },
                ]
            }
        },
    )

    assert len(result["results"]) == 1
    page = result["results"][0]
    assert page["properties"]["Name"]["title"][0]["text"]["content"] == "Alice"
    assert page["properties"]["Age"]["number"] == 30


def test_databases_query_with_or_filter(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    client.pages_create(payload=make_db_page(db["id"], "Alice", 20))
    client.pages_create(payload=make_db_page(db["id"], "Bob", 30))
    client.pages_create(payload=make_db_page(db["id"], "Charlie", 40))

    result = client.databases_query(
        path_params={"database_id": db["id"]},
        payload={
            "filter": {
                "or": [
                    {
                        "property": "Name",
                        "title": {"equals": "Alice"},
                    },
                    {
                        "property": "Age",
                        "number": {"equals": 40},
                    },
                ]
            }
        },
    )

    names = {
        p["properties"]["Name"]["title"][0]["text"]["content"]
        for p in result["results"]
    }

    assert names == {"Alice", "Charlie"}


def test_databases_query_with_filter_properties_projection(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    client.pages_create(payload=make_db_page(db["id"], "Alice", 20))

    result = client.databases_query(
        path_params={"database_id": db["id"]},
        query_params={"filter_properties": ["Name"]},
        payload={},
    )

    props = result["results"][0]["properties"]

    assert "Name" in props
    assert "Age" not in props


def test_databases_query_returns_empty_list_when_no_match(client):
    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    client.pages_create(payload=make_db_page(db["id"], "Alice", 20))

    result = client.databases_query(
        path_params={"database_id": db["id"]},
        payload={
            "filter": {
                "property": "Age",
                "number": {"equals": 99},
            }
        },
    )

    assert result["results"] == []
    assert result["has_more"] is False


def test_databases_query_requires_database_id(client):
    with pytest.raises(NotionError):
        client.databases_query(
            path_params={},
            payload={},
        )

# ----------------------------------------------------------
# Sort by number (ascending / descending)
# ----------------------------------------------------------

def test_database_query_sort_by_number_ascending(client):
    db = client.databases_create(payload=make_database(client._ROOT_PAGE_ID_))

    client.pages_create(payload=make_db_page(db["id"], "A", 30))
    client.pages_create(payload=make_db_page(db["id"], "B", 10))
    client.pages_create(payload=make_db_page(db["id"], "C", 20))

    result = client.databases_query(
        path_params={"database_id": db["id"]},
        payload={
            "sorts": [
                {"property": "Age", "direction": "ascending"}
            ]
        },
    )

    ages = [
        p["properties"]["Age"]["number"]
        for p in result["results"]
    ]

    assert ages == [10, 20, 30]

# ----------------------------------------------------------
# Sort by title (descending)
# ----------------------------------------------------------

def test_database_query_sort_by_title_descending(client):
    db = client.databases_create(payload=make_database(client._ROOT_PAGE_ID_))

    client.pages_create(payload=make_db_page(db["id"], "Alice", 10))
    client.pages_create(payload=make_db_page(db["id"], "Bob", 10))
    client.pages_create(payload=make_db_page(db["id"], "Charlie", 10))

    result = client.databases_query(
        path_params={"database_id": db["id"]},
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

# ----------------------------------------------------------
# Multiple sorts (stable ordering)
# ----------------------------------------------------------

def test_database_query_multiple_sorts(client):
    db = client.databases_create(payload=make_database(client._ROOT_PAGE_ID_))

    client.pages_create(payload=make_db_page(db["id"], "Alice", 30))
    client.pages_create(payload=make_db_page(db["id"], "Bob", 30))
    client.pages_create(payload=make_db_page(db["id"], "Charlie", 20))

    result = client.databases_query(
        path_params={"database_id": db["id"]},
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

    assert names == ["Charlie", "Alice", "Bob"]

# ----------------------------------------------------------
# Sorting after filtering
# ----------------------------------------------------------

def test_database_query_filter_and_sort(client):
    db = client.databases_create(payload=make_database(client._ROOT_PAGE_ID_))

    client.pages_create(payload=make_db_page(db["id"], "Alice", 30))
    client.pages_create(payload=make_db_page(db["id"], "Bob", 20))
    client.pages_create(payload=make_db_page(db["id"], "Charlie", 30))

    result = client.databases_query(
        path_params={"database_id": db["id"]},
        payload={
            "filter": {
                "property": "Age",
                "number": {"equals": 30}
            },
            "sorts": [
                {"property": "Name", "direction": "ascending"}
            ]
        },
    )

    names = [
        p["properties"]["Name"]["title"][0]["text"]["content"]
        for p in result["results"]
    ]

    assert names == ["Alice", "Charlie"]

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
            'parent': {"type": "database_id", "database_id": db["id"]},
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

def test_delete_property_by_name(client, database):
    client._update_database_properties(
        database,
        {"Score": None},
    )
    assert "Score" not in database["properties"]

def test_delete_title_property_fails(client, database):
    with pytest.raises(NotionError, match="Cannot delete title"):
        client._update_database_properties(
            database,
            {"Name": None},
        )

def test_rename_property_by_id(client, database):
    client._update_database_properties(
        database,
        {"J@cT": {"name": "Points"}},
    )
    assert "Points" in database["properties"]
    assert database["properties"]["Points"]["id"] == "J@cT"

def test_rename_status_property_fails(client, database):
    with pytest.raises(NotionError, match="status"):
        client._update_database_properties(
            database,
            {"Status": {"name": "New Status"}},
        )

def test_update_property_configuration(client, database):
    client._update_database_properties(
        database,
        {"Score": {"number": {"format": "percent"}}},
    )
    assert database["properties"]["Score"]["number"]["format"] == "percent"

def test_update_property_type(client, database):
    client._update_database_properties(
        database,
        {"Score": {"rich_text": {}}},
    )
    assert database["properties"]["Score"]["rich_text"] == {}

def test_change_title_property_fails(client, database):
    with pytest.raises(NotionError, match="Cannot change type of title"):
        client._update_database_properties(
            database,
            {"Name": {"rich_text": {}}},
        )

def test_update_status_property_fails(client, database):
    with pytest.raises(NotionError, match="status"):
        client._update_database_properties(
            database,
            {"Status": {"status": {"options": []}}},
        )

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

@pytest.mark.xfail(
    reason="Schema-write via databases.update relocates to data_sources.update; "
    "databases.update narrows to container-level attrs in #348. Out of scope for #347.",
    strict=True,
)
def test_database_update_title_and_schema(client, database):
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
            "title": [{"text": {"content": "New Name"}}],
            "properties": {
                "Score": {"number": {"format": "percent"}}
            },
        },
    )

    assert result["title"][0]["text"]["content"] == "New Name"
    assert result["properties"]["Score"]["number"]["format"] == "percent"

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
        "parent": {"type": "database_id", "database_id": students["id"]},
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
        "parent": {"type": "database_id", "database_id": students["id"]},
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
        "parent": {"type": "database_id", "database_id": students["id"]},
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
        "parent": {"type": "database_id", "database_id": students["id"]},
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
            "parent": {"type": "database_id", "database_id": courses["id"]},
            "properties": {
                "Title": {"title": [{"text": {"content": "Math 101"}}]},
            },
        }
    )

    student = client.pages_create(
        payload={
            "parent": {"type": "database_id", "database_id": students["id"]},
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
            "parent": {"type": "database_id", "database_id": courses["id"]},
            "properties": {"Title": {"title": [{"text": {"content": "Math 101"}}]}},
        }
    )
    history = client.pages_create(
        payload={
            "parent": {"type": "database_id", "database_id": courses["id"]},
            "properties": {"Title": {"title": [{"text": {"content": "History 101"}}]}},
        }
    )

    student = client.pages_create(
        payload={
            "parent": {"type": "database_id", "database_id": students["id"]},
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
            "parent": {"type": "database_id", "database_id": courses["id"]},
            "properties": {"Title": {"title": [{"text": {"content": "Math 101"}}]}},
        }
    )

    student = client.pages_create(
        payload={
            "parent": {"type": "database_id", "database_id": students["id"]},
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
            "parent": {"type": "database_id", "database_id": courses["id"]},
            "properties": {"Title": {"title": [{"text": {"content": "Math 101"}}]}},
        }
    )

    student = client.pages_create(
        payload={
            "parent": {"type": "database_id", "database_id": students["id"]},
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