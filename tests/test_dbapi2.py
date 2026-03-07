from operator import itemgetter

import pytest

from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.notion_sdk.getters import rich_text_to_plain_text
from normlite.notiondbapi.dbapi2 import Cursor, ProgrammingError
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode

@pytest.fixture
def row_description() -> tuple[tuple, ...]:
    return (
        ("id", DBAPITypeCode.ID, None, None, None, None, None,),
        ("archived", DBAPITypeCode.ARCHIVAL_FLAG, None, None, None, None, None,),
        ("in_trash", DBAPITypeCode.ARCHIVAL_FLAG, None, None, None, None, None,),
        ("created_time", DBAPITypeCode.TIMESTAMP, None, None, None, None, None,),
        ("name", DBAPITypeCode.TITLE, None, None, None, None, None,),
        ("id", DBAPITypeCode.NUMBER, None, None, None, None, None,),
        ("is_active", DBAPITypeCode.CHECKBOX, None, None, None, None, None,),
        ("start_on", DBAPITypeCode.DATE, None, None, None, None, None,),
        ("grade", DBAPITypeCode.RICH_TEXT, None, None, None, None, None,),
    )
@pytest.fixture
def client() -> InMemoryNotionClient:
    new_client = InMemoryNotionClient()
    new_client._ensure_root()   
    return new_client

@pytest.fixture
def prefilled_client(client: InMemoryNotionClient) -> InMemoryNotionClient:
    _, _ = add_pages(
        client, [
            {
                "name": "Galileo Galilei", 
                "id": 123456, 
                "is_active": False, 
                "start_on": "1581-01-01", 
                "grade": "A"
            }, 
            {
                "name": "Isaac Newton", 
                "id": 123457, 
                "is_active": False, 
                "start_on": "1681-01-01", 
                "grade": "B"
            }, 
            {
                "name": "Ada Lovelace", 
                "id": 123458, 
                "is_active": False, 
                "start_on": "1781-01-01", 
                "grade": "C"
            }, 
        ]
    )

    return client

@pytest.fixture
def database_id(prefilled_client: InMemoryNotionClient) -> str:
    found = prefilled_client.search(
        payload={
            "query": "students",
            "filter": {
                "property": "object",
                "value": "database"
            }
        }
    )

    results = found["results"]
    assert len(results) == 1

    return results[0]["id"]

@pytest.fixture
def cursor(prefilled_client: InMemoryNotionClient) -> Cursor:
    return Cursor(prefilled_client)

def add_pages(client: InMemoryNotionClient, page_data: list[dict]) -> tuple[str, list[str]]:

    students_db = client._add('database', {
        'parent': {
            'type': 'page_id',
            'page_id': client._ROOT_PAGE_ID_
        },
        "title": [
            {
                "type": "text",
                "text": {
                    "content": "students",
                    "link": None
                },
                "plain_text": "students",
                "href": None
            }
        ],
        'properties': {
            'name': {'title': {}},
            'id': {'number': {}},
            'is_active': {'checkbox': {}},
            'start_on': {'date': {}},
            'grade': {'rich_text': {}},
        }
    })

    inserted_pages = []
    for data in page_data:
        page = client.pages_create(
            payload={
                'parent': {
                    'type': 'database_id',
                    'database_id': students_db['id']
                },
                'properties': {
                    'name': {'title': [{'text': {'content': data['name']}}]},
                    'id': {'number': data['id']},
                    'is_active': {'checkbox': data['is_active']},
                    'start_on': {'date': {'start': data['start_on']}},
                    'grade': {'rich_text': [{'text': {'content': data['grade']}}]},
                }
            }
        )
        inserted_pages.append(page)

    return students_db['id'], inserted_pages


get_name = itemgetter(4)

#----------------------------------------------------------------
# Description tests
#----------------------------------------------------------------

def test_description_none_if_no_execute(cursor: Cursor):
    assert cursor.description is None

def test_description_none_if_no_rows(
        cursor: Cursor, 
        row_description: tuple[tuple, ...],
        database_id: str
):
    cursor._inject_description(row_description)
    cursor.execute(
        operation={
            "endpoint": "databases",
            "request": "query"
        },
        parameters={
            "path_params": {"database_id": database_id},
            "payload":{
                "filter": {
                    "property": "name",
                    "title": {
                        "equals": "Albert Einstein"
                    }
                }
            }
        }
    )

    assert cursor.description is None

#----------------------------------------------------------------
# fetchone tests
#----------------------------------------------------------------

def test_fetchone_raises_if_no_execute(cursor: Cursor):
    with pytest.raises(ProgrammingError):
        _ = cursor.fetchone()

def test_fetchone_returns_none_if_no_rows_found(
        cursor: Cursor, 
        row_description: tuple[tuple, ...],
        database_id: str
):
    cursor._inject_description(row_description)
    cursor.execute(
        operation={
            "endpoint": "databases",
            "request": "query"
        },
        parameters={
            "path_params": {"database_id": database_id},
            "payload":{
                "filter": {
                    "property": "name",
                    "title": {
                        "equals": "Albert Einstein"
                    }
                }
            }
        }
    )

    row = cursor.fetchone()

    assert row is None

def test_fetchone_returns_first_row_found(
        cursor: Cursor, 
        row_description: tuple[tuple, ...],
        database_id: str
):
    cursor._inject_description(row_description)
    cursor.execute(
        operation={
            "endpoint": "databases",
            "request": "query"
        },
        parameters={
            "path_params": {"database_id": database_id},
            "payload":{
                "filter": {
                    "property": "is_active",
                    "checkbox": {
                        "does_not_equal": True
                    }
                }
            }
        }
    )

    first = cursor.fetchone()

    assert rich_text_to_plain_text(get_name(first)) == "Galileo Galilei"
    assert not cursor.closed

#----------------------------------------------------------------
# fetchone tests
#----------------------------------------------------------------

def test_fetchall_returns_all_rows_found(
        cursor: Cursor, 
        row_description: tuple[tuple, ...],
        database_id: str
):
    cursor._inject_description(row_description)
    cursor.execute(
        operation={
            "endpoint": "databases",
            "request": "query"
        },
        parameters={
            "path_params": {"database_id": database_id},
            "payload":{
                "filter": {
                    "property": "is_active",
                    "checkbox": {
                        "does_not_equal": True
                    }
                }
            }
        }
    )

    rows = cursor.fetchall()

    assert len(rows) == 3
    assert rich_text_to_plain_text(get_name(rows[0])) == "Galileo Galilei"
    assert rich_text_to_plain_text(get_name(rows[2])) == "Ada Lovelace"
    assert not cursor.closed

def test_fetchall_raises_if_no_execute(cursor: Cursor):
    with pytest.raises(ProgrammingError):
        _ = cursor.fetchall()

