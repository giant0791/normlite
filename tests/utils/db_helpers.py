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
    tables_id: str,
) -> str:
    """
    Create a student database in the mocked Notion backend
    from a client.
    """

    db = client._add(
        "database",
        {
            "parent": {"type": "page_id", "page_id": user_tables_page_id},
            "title": [{"text": {"content": "students"}}],
            "properties": {
                "name": {"title": {}},
                "id": {"number": {}},
                "is_active": {"checkbox": {}},
                "start_on": {"date": {}},
                "grade": {"rich_text": {}},
            },
        },
    )

    client._add(
        "page",
        {
            "parent": {"type": "database_id", "database_id": tables_id},
            "properties": {
                "table_name": {"title": [{"text": {"content": "students"}}]},
                "table_schema": {"rich_text": [{"text": {"content": ""}}]},
                "table_catalog": {"rich_text": [{"text": {"content": "memory"}}]},
                "table_id": {"rich_text": [{"text": {"content": db.get("id")}}]},
            },
        },
    )

    return db.get("id")


def create_students_db(engine: Engine) -> str:
    """
    Creates a students database in the mocked Notion backend
    and registers it in the system catalog.

    Returns:
        database_id (str)
    """

    return create_students_db_from_client(
        engine._client,
        engine._user_tables_page_id,
        engine._tables_id
    )

def attach_table_oid(table: Table, db_id: str) -> None:
    """
    Attach a database id to a Table to simulate reflection.
    """
    table._sys_columns["object_id"]._value = db_id


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
    db_id = students.get_oid()
    assert db_id is not None, "Table must be attached to a database first."

    for i in range(n):
        engine._client.pages_create(
            payload={
                "parent": {"type": "database_id", "database_id": db_id},
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
    