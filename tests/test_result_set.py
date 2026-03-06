from __future__ import annotations

from operator import itemgetter
import pdb

import pytest

from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.notion_sdk.getters import get_title_property_value, rich_text_to_plain_text
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode
from normlite.notiondbapi.resultset import ResultSet

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

def test_client_conforms_API_version_2022_06_28(client: InMemoryNotionClient):
    _, pages = add_pages(
        client, [
            {
                "name": "Galileo Galilei", 
                "id": 123456, 
                "is_active": False, 
                "start_on": "1581-01-01", 
                "grade": "A"
            }
        ]
    )

    properties = pages[0].get("properties")
    assert list(properties['name'].keys()) == ['id']
    assert list(properties['id'].keys()) == ['id']
    assert list(properties['is_active'].keys()) == ['id']
    assert list(properties['start_on'].keys()) == ['id']
    assert list(properties['grade'].keys()) == ['id']


def test_resultset_pages_from_json(prefilled_client: InMemoryNotionClient, database_id: str):
    results = prefilled_client.databases_query({"database_id": database_id})
    resultset = ResultSet.from_json(results)

    assert len(results["results"]) == len(resultset)

def test_resultset_fetch_first(prefilled_client: InMemoryNotionClient, database_id: str):
    results = prefilled_client.databases_query({"database_id": database_id})
    resultset = ResultSet.from_json(results)
    first = next(resultset)

    assert get_title_property_value(first["name"]) == "Galileo Galilei"

def test_resultset_fetchall(prefilled_client: InMemoryNotionClient, database_id: str):
    results = prefilled_client.databases_query({"database_id": database_id})
    resultset = ResultSet.from_json(results)
    all = [get_title_property_value(row["name"]) for row in resultset]

    assert ["Galileo Galilei", "Isaac Newton", "Ada Lovelace"] == all

def test_resultset_database_from_json(prefilled_client: InMemoryNotionClient, database_id: str):
    database = prefilled_client.databases_retrieve(
        path_params={"database_id": database_id}
    )

    assert database["id"] == database_id
    resultset = ResultSet.from_json(database)
    #pdb.set_trace()
    rows = [row for row in resultset]
    get_col_name = itemgetter(0)

    # expected 9, 1 row for each col spec
    assert len(resultset) == 9 

    # sys cols spec 
    assert get_col_name(rows[0]) == "object_id"
    assert get_col_name(rows[1]) == "is_archived"
    assert get_col_name(rows[2]) == "is_deleted"
    assert get_col_name(rows[3]) == "created_at"

    # user cols spec
    assert get_col_name(rows[4]) == "name"
    assert get_col_name(rows[5]) == "id"
    assert get_col_name(rows[6]) == "is_active"
    assert get_col_name(rows[7]) == "start_on"
    assert get_col_name(rows[8]) == "grade"

def test_resultset_database_cols_from_json(prefilled_client: InMemoryNotionClient, database_id: str):
    database = prefilled_client.databases_retrieve(
        path_params={"database_id": database_id}
    )

    assert database["id"] == database_id
    resultset = ResultSet.from_json(database)
    description = resultset.make_description()
    get_names = itemgetter(0)
    col_names = list(map(get_names, description))

    assert len(description) == 5
    assert ["column_name", "column_type", "column_id", "metadata", "is_system"] == col_names

