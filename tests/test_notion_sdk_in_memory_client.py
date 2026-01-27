import pytest

from normlite.notion_sdk.client import InMemoryNotionClient, NotionError


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


def make_database(parent_page_id):
    return {
        "parent": {
            "type": "page_id",
            "page_id": parent_page_id,
        },
        "title": [{"text": {"content": "Students"}}],
        "properties": {
            "Name": {
                "title": {}
            },
            "Age": {
                "number": {}
            },
        },
    }


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


# ---------------------------------------------------------
# Root page behavior
# ---------------------------------------------------------

def test_root_page_exists_by_default():
    client = InMemoryNotionClient()

    root = client.pages_retrieve(
        path_params={"page_id": client._ROOT_PAGE_ID_}
    )

    assert root["object"] == "page"
    assert root["properties"]["Title"]["title"][0]["text"]["content"] == "ROOT_PAGE"


# ---------------------------------------------------------
# Page under page rules
# ---------------------------------------------------------

def test_create_page_under_page_with_valid_title():
    client = InMemoryNotionClient()

    page = client.pages_create(
        payload=make_title_page(client._ROOT_PAGE_ID_, "Child")
    )

    assert page["parent"]["page_id"] == client._ROOT_PAGE_ID_
    assert "Title" in page["properties"]
    assert page["properties"]["Title"]["type"] == "title"
    assert page["properties"]["Title"]["id"] == "title"


def test_page_under_page_rejects_multiple_properties():
    client = InMemoryNotionClient()

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


def test_page_under_page_rejects_non_title_property():
    client = InMemoryNotionClient()

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

def test_create_database_under_page():
    client = InMemoryNotionClient()

    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    assert db["object"] == "database"
    assert "Name" in db["properties"]
    assert "Age" in db["properties"]

    assert db["properties"]["Name"]["type"] == "title"
    assert db["properties"]["Name"]["id"] == "title"

    assert db["properties"]["Age"]["type"] == "number"
    assert isinstance(db["properties"]["Age"]["id"], str)


def test_database_cannot_be_created_under_database():
    client = InMemoryNotionClient()

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

def test_create_page_under_database_with_exact_schema():
    client = InMemoryNotionClient()

    db = client.databases_create(
        payload=make_database(client._ROOT_PAGE_ID_)
    )

    page = client.pages_create(
        payload=make_db_page(db["id"], "Bob", 42)
    )

    props = page["properties"]

    assert set(props.keys()) == {"Name", "Age"}
    assert props["Name"]["type"] == "title"
    assert props["Age"]["type"] == "number"

    assert props["Name"]["id"] == db["properties"]["Name"]["id"]
    assert props["Age"]["id"] == db["properties"]["Age"]["id"]


def test_page_under_database_rejects_missing_property():
    client = InMemoryNotionClient()

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


def test_page_under_database_rejects_extra_property():
    client = InMemoryNotionClient()

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


def test_page_under_database_rejects_type_mismatch():
    client = InMemoryNotionClient()

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

def test_store_and_returned_object_are_consistent():
    client = InMemoryNotionClient()

    page = client.pages_create(
        payload=make_title_page(client._ROOT_PAGE_ID_, "Consistency")
    )

    stored = client._get_by_id(page["id"])

    assert stored is not page  # deep copy returned
    assert stored["id"] == page["id"]
    assert stored["properties"] == page["properties"]


# ---------------------------------------------------------
# Page update behavior
# ---------------------------------------------------------

def test_pages_update_archived_flag():
    client = InMemoryNotionClient()

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


def test_pages_update_properties():
    client = InMemoryNotionClient()

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

# ---------------------------------------------------------
# Database query API (databases_query)
# ---------------------------------------------------------

def test_databases_query_returns_pages_for_database():
    client = InMemoryNotionClient()

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


def test_databases_query_does_not_return_pages_from_other_databases():
    client = InMemoryNotionClient()

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


def test_databases_query_with_simple_filter_number_equals():
    client = InMemoryNotionClient()

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


def test_databases_query_with_title_contains_filter():
    client = InMemoryNotionClient()

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


def test_databases_query_with_and_filter():
    client = InMemoryNotionClient()

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


def test_databases_query_with_or_filter():
    client = InMemoryNotionClient()

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


def test_databases_query_with_filter_properties_projection():
    client = InMemoryNotionClient()

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


def test_databases_query_returns_empty_list_when_no_match():
    client = InMemoryNotionClient()

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


def test_databases_query_requires_database_id():
    client = InMemoryNotionClient()

    with pytest.raises(NotionError):
        client.databases_query(
            path_params={},
            payload={},
        )

# ----------------------------------------------------------
# Sort by number (ascending / descending)
# ----------------------------------------------------------

def test_database_query_sort_by_number_ascending():
    client = InMemoryNotionClient()
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

def test_database_query_sort_by_title_descending():
    client = InMemoryNotionClient()
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

def test_database_query_multiple_sorts():
    client = InMemoryNotionClient()
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

def test_database_query_filter_and_sort():
    client = InMemoryNotionClient()
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
