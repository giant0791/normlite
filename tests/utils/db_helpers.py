import pdb
from typing import Optional

from normlite.engine.base import Engine
from normlite.notion_sdk.client import AbstractNotionClient
from normlite.sql.schema import Table


def bootstrap_client(
    client: AbstractNotionClient, 
) -> str:
    """
    Bootstrap a client in the mocked Notion backend.
    It creates the information schema page and creates the tables database.
    """

    assert hasattr(client, "_ROOT_PAGE_ID_"), "Please use a concrete Notion client."

    # create information schema page
    parent_id = client.__ROOT_PAGE_ID_
    name = "information_schema"
    page = client._add(
        "page",
        {
            "parent": {"type": "page_id", "page_id": parent_id},
            "properties": {
                "Name": {"title": [{"text": {"content": name}}]}
            },
        },
    )

    # create tables
    parent_id = page["id"]
    name="tables"
    properties = {
        "table_name": {"title": {}},
        "table_schema": {"rich_text": {}},
        "table_catalog": {"rich_text": {}},
        "table_id": {"rich_text": {}},
    }

    db = client._add(
        "database",
        {
            "parent": {"type": "page_id", "page_id": parent_id},
            "title": [{"type": "text", "text": {"content": name}}],
            "properties": properties,
        },
    )

    return db['id']

def create_students_db_from_client(
    client: AbstractNotionClient,
    user_tables_page_id: str,
    tables_dsid: str,
) -> dict:
    """
    Create a student database in the mocked Notion backend from a client
    and register it in the ``tables`` catalog.

    As of Notion 2025-09-03 (ADR-0014) the column schema lives on the
    database's data source (``initial_data_source.properties``), the catalog
    page parents to the ``tables`` data source id, and the row persists both
    the database id (``table_id``) and its data source id (``table_dsid``).

    Returns:
        The ``databases.create`` response dict (carries ``id`` and
        ``data_sources[0].id``).
    """

    db = client._add(
        "database",
        {
            "parent": {"type": "page_id", "page_id": user_tables_page_id},
            "title": [{"text": {"content": "students"}}],
            "initial_data_source": {
                "properties": {
                    "name": {"title": {}},
                    "id": {"number": {}},
                    "is_active": {"checkbox": {}},
                    "start_on": {"date": {}},
                    "grade": {"rich_text": {}},
                },
            },
        },
    )

    client._add(
        "page",
        {
            "parent": {"type": "data_source_id", "data_source_id": tables_dsid},
            "properties": {
                "table_name": {"title": [{"text": {"content": "students"}}]},
                "table_schema": {"rich_text": [{"text": {"content": ""}}]},
                "table_catalog": {"rich_text": [{"text": {"content": "memory"}}]},
                "table_id": {"rich_text": [{"text": {"content": db["id"]}}]},
                "table_dsid": {"rich_text": [{"text": {"content": db["data_sources"][0]["id"]}}]},
                "is_dropped": {"checkbox": False},
            },
        },
    )

    return db


def create_students_db(engine: Engine) -> dict:
    """
    Creates a students database in the mocked Notion backend
    and registers it in the system catalog.

    Returns:
        The ``databases.create`` response dict (carries the database id and
        its data source id).
    """

    return create_students_db_from_client(
        engine._client,
        engine._user_tables_page_id,
        engine._catalog._tables_dsid,
    )

def attach_table_oid(table: Table, db, ds_id: Optional[str] = None) -> None:
    """
    Attach a database id (and its data source id) to a Table to simulate reflection.

    As of Notion 2025-09-03 a Table carries a two-ID identity: the database id
    (``object_id``) and its data source id (``data_source_id``).

    ``db`` may be the full ``databases.create`` response dict (carrying the real
    ``data_sources[0].id``) or a bare database-id string. For a string, ``ds_id``
    supplies the data source id, defaulting to a deterministic, distinct synthetic
    value derived from the id — enough for pure-compilation tests that never
    execute against the fake.
    """
    if isinstance(db, dict):
        db_id = db["id"]
        ds_id = db["data_sources"][0]["id"]
    else:
        db_id = db
        ds_id = ds_id or f"{db_id}-ds"
    table._sys_columns["object_id"]._value = db_id
    table._sys_columns["data_source_id"]._value = ds_id


def populate_students(
    engine: Engine,
    students: Table,
    n: int = 2,
    *,
    is_active: bool = True,
) -> None:
    """
    Populate the students table with N rows.
    """
    ds_id = students.get_data_source_id()
    assert ds_id is not None, "Table must be attached to a data source first."

    for i in range(n):
        engine._client.pages_create(
            payload={
                "parent": {"type": "data_source_id", "data_source_id": ds_id},
                "properties": {
                    "name": {"title": [{"text": {"content": f"name_{i}"}}]},
                    "id": {"number": i},
                    "is_active": {"checkbox": is_active},
                    "start_on": {"date": {"start": "1600-01-01"}},
                    "grade": {"rich_text": [{"text": {"content": "A"}}]},
                },
            }
        )

def populate_database(
    client: AbstractNotionClient, 
    database_id: str,
    n: int = 10,
    *,
    is_active: bool = True,
    grade: str = "A",
) -> None:
    for i in range(n):
        client.pages_create(
            payload={
                "parent": {"type": "database_id", "database_id": database_id},
                "properties": {
                    "name": {"title": [{"text": {"content": f"name_{i}"}}]},
                    "id": {"number": i},
                    "is_active": {"checkbox": is_active},
                    "start_on": {"date": {"start": "1600-01-01"}},
                    "grade": {"rich_text": [{"text": {"content": grade}}]},
                },
            }
        )
    