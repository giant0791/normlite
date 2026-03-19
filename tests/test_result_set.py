from __future__ import annotations

from operator import itemgetter
import pdb

import pytest

from normlite._constants import SpecialColumns
from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.notion_sdk.getters import rich_text_to_plain_text
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode
from normlite.notiondbapi.resultset import ResultSet

@pytest.fixture
def row_description() -> tuple[tuple, ...]:
    return (
        (SpecialColumns.NO_ID, DBAPITypeCode.ID, None, None, None, None, None,),
        (SpecialColumns.NO_ARCHIVED, DBAPITypeCode.ARCHIVAL_FLAG, None, None, None, None, None,),
        (SpecialColumns.NO_IN_TRASH, DBAPITypeCode.ARCHIVAL_FLAG, None, None, None, None, None,),
        (SpecialColumns.NO_CREATED_TIME, DBAPITypeCode.TIMESTAMP, None, None, None, None, None,),
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

def test_resultset_created_pages_from_json(client: InMemoryNotionClient, row_description: tuple[tuple, ...]):
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

    resultset = ResultSet(pages[0], row_description)
    get_name = itemgetter(4)
    get_id = itemgetter(5)
    get_is_active = itemgetter(6)
    get_start_on = itemgetter(7)
    get_grade = itemgetter(8)
    first = next(resultset)

    # All property values from a page returned by pages.create are None
    assert get_name(first) is None
    assert get_id(first) is None
    assert get_is_active(first) is None
    assert get_start_on(first) is None
    assert get_grade(first) is None

def test_resultset_pages_from_json(prefilled_client: InMemoryNotionClient, database_id: str, row_description: tuple[tuple, ...]):
    results = prefilled_client.databases_query({"database_id": database_id})
    resultset = ResultSet(results, row_description)

    assert len(results["results"]) == len(resultset)

def test_resultset_fetch_first(prefilled_client: InMemoryNotionClient, database_id: str, row_description: tuple[tuple, ...]):
    results = prefilled_client.databases_query({"database_id": database_id})
    resultset = ResultSet(results, row_description)
    first = next(resultset)
    get_in_trash = itemgetter(2)
    get_col_name = itemgetter(4)

    assert not get_in_trash(first)
    assert rich_text_to_plain_text(get_col_name(first)) == "Galileo Galilei"

def test_resultset_fetchall(prefilled_client: InMemoryNotionClient, database_id: str, row_description: tuple[tuple, ...]):
    results = prefilled_client.databases_query({"database_id": database_id})
    resultset = ResultSet(results, row_description)
    get_col_name = itemgetter(4)    
    all = [rich_text_to_plain_text(get_col_name(row)) for row in resultset]

    assert ["Galileo Galilei", "Isaac Newton", "Ada Lovelace"] == all

def test_resultset_database_from_json(prefilled_client: InMemoryNotionClient, database_id: str):
    database = prefilled_client.databases_retrieve(
        path_params={"database_id": database_id}
    )

    assert database["id"] == database_id
    resultset = ResultSet(database, description=None)
    #pdb.set_trace()
    rows = [row for row in resultset]
    get_col_name = itemgetter(0)

    # expected 9, 1 row for each col spec
    assert len(resultset) == 10 

    # sys cols spec 
    assert get_col_name(rows[0]) == SpecialColumns.NO_ID.value
    assert get_col_name(rows[1]) == SpecialColumns.NO_ARCHIVED.value
    assert get_col_name(rows[2]) == SpecialColumns.NO_IN_TRASH.value
    assert get_col_name(rows[3]) == SpecialColumns.NO_CREATED_TIME.value
    assert get_col_name(rows[4]) == SpecialColumns.NO_TITLE.value

    # user cols spec
    assert get_col_name(rows[5]) == "name"
    assert get_col_name(rows[6]) == "id"
    assert get_col_name(rows[7]) == "is_active"
    assert get_col_name(rows[8]) == "start_on"
    assert get_col_name(rows[9]) == "grade"

def test_resultset_database_cols_from_json(prefilled_client: InMemoryNotionClient, database_id: str):
    database = prefilled_client.databases_retrieve(
        path_params={"database_id": database_id}
    )

    assert database["id"] == database_id
    resultset = ResultSet(database, description=None)
    description = resultset.description
    get_names = itemgetter(0)
    col_names = list(map(get_names, description))

    assert len(description) == 5
    assert ["column_name", "column_type", "column_id", "metadata", "is_system"] == col_names

def test_resultset_database_cols_from_json_extract_table_name(prefilled_client: InMemoryNotionClient, database_id: str):
    database = prefilled_client.databases_retrieve(
        path_params={"database_id": database_id}
    )
    resultset = ResultSet(database, description=None)
    get_table_name = itemgetter(3)
    get_table_name_metadata = itemgetter(4)
    table_name_metadata_row = get_table_name_metadata(resultset._rows)
    table_name = get_table_name(table_name_metadata_row)

    assert rich_text_to_plain_text(table_name) == "students"

def test_resultset_last_inserted_rowids_from_pages(
    prefilled_client: InMemoryNotionClient, #
    database_id: str, 
    row_description: tuple[tuple, ...]        
):
    results = prefilled_client.databases_query({"database_id": database_id})
    resultset = ResultSet(results, row_description)

    assert len(resultset.last_inserted_rowids) == len(resultset)    

def test_resultset_last_inserted_rowids_from_database(prefilled_client: InMemoryNotionClient, database_id: str):
    database = prefilled_client.databases_retrieve(
        path_params={"database_id": database_id}
    )

    assert database["id"] == database_id
    resultset = ResultSet(database, description=None)

    assert resultset.last_inserted_rowids is None
