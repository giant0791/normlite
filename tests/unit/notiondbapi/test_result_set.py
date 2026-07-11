from __future__ import annotations

from operator import itemgetter
import json
import pdb
from typing import Any, Callable, Sequence

import pytest

from normlite._constants import SpecialColumns
from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.notion_sdk.getters import rich_text_to_plain_text
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode
from normlite.notiondbapi.resultset import ResultSet

class _RowGetter:
    _METADATA = ("column_name", "column_type", "column_id", "metadata", "is_system")

    def __init__(self, desc: Sequence[tuple]):
        self.index_map = {
            col[0]: idx
            for idx, col in enumerate(desc)
        }

        self.value_map = {
            col: idx
            for idx, col in enumerate(self._METADATA)
        }
    
    def getter(self, column_name: str) -> Callable[[Sequence[Any]], Any]: 
        idx = self.index_map[column_name]

        def _getter(row: Sequence[Any]) -> Any:
            return row[idx]
        
        return _getter
    
    def value_getter(self, value_name: str) -> Callable[[Sequence[Any]], Any]:
        idx = self.value_map[value_name]

        def _value_getter(row: Sequence) -> Any:
            return row[idx]
        
        return _value_getter

@pytest.fixture
def database_retrieved() -> dict:
    json_obj = """
    {
        "object": "database",
        "id": "bc1211ca-e3f1-4939-ae34-5260b16f627c",
        "data_sources": [
            {
                "id": "9f8e7d6c-1234-4939-ae34-5260b16f627c",
                "name": "Grocery List"
            }
        ],
        "created_time": "2021-07-08T23:50:00.000Z",
        "last_edited_time": "2021-07-08T23:50:00.000Z",
        "cover": {
            "type": "external",
            "external": {
                "url": "https://website.domain/images/image.png"
            }
        },
        "url": "https://www.notion.so/bc1211cae3f14939ae34260b16f627c",
        "title": [
            {
                "type": "text",
                "text": {
                    "content": "Grocery List",
                    "link": null
                },
                "annotations": {
                    "bold": false,
                    "italic": false,
                    "strikethrough": false,
                    "underline": false,
                    "code": false,
                    "color": "default"
                },
                "plain_text": "Grocery List",
                "href": null
            }
        ],
        "description": [
            {
                "type": "text",
                "text": {
                    "content": "Grocery list for just kale",
                    "link": null
                },
                "annotations": {
                    "bold": false,
                    "italic": false,
                    "strikethrough": false,
                    "underline": false,
                    "code": false,
                    "color": "default"
                },
                "plain_text": "Grocery list for just kale",
                "href": null
            }
        ],
        "properties": {
            "Price": {
                "id": "evWq",
                "name": "Price",
                "type": "number",
                "number": {
                    "format": "dollar"
                }
            },
            "Description": {
                "id": "V}lX",
                "name": "Description",
                "type": "rich_text",
                "rich_text": {}
            },
            "Name": {
                "id": "title",
                "name": "Name",
                "type": "title",
                "title": {}
            }
        },
        "parent": {
            "type": "page_id",
            "page_id": "98ad959b-2b6a-4774-80ee-00246fb0ea9b"
        },
        "archived": false,
        "in_trash": false,
        "is_inline": false,
        "public_url": null
    }
    """
    return json.loads(json_obj)

@pytest.fixture
def retrieved_page() -> dict:
    return {
        "object": "page",
        "id": "59833787-2cf9-4fdf-8782-e53db20768a5",
        "created_time": "2022-03-01T19:05:00.000Z",
        "last_edited_time": "2022-07-06T20:25:00.000Z",
        "created_by": {
            "object": "user",
            "id": "ee5f0f84-409a-440f-983a-a5315961c6e4"
        },
        "last_edited_by": {
            "object": "user",
            "id": "0c3e9826-b8f7-4f73-927d-2caaf86f1103"
        },
        "cover": {
            "type": "external",
            "external": {
            "url": "https://upload.wikimedia.org/wikipedia/commons/6/62/Tuscankale.jpg"
            }
        },
        "parent": {
            "type": "database_id",
            "database_id": "d9824bdc-8445-4327-be8b-5b47500af6ce"
        },
        "archived": False,
        "properties": {
            "Price": {
                "id": "BJXS",
                "type": "number",
                "number": 2.5
            },
            "Description": {
                "id": "_Tc_",
                "type": "rich_text",
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "A dark ",
                            "link": None
                        },
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                            "color": "default"
                        },
                        "plain_text": "A dark ",
                        "href": None
                    },
                    {
                        "type": "text",
                        "text": {
                            "content": "green",
                            "link": None
                        },
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                            "color": "green"
                        },
                        "plain_text": "green",
                        "href": None
                    },
                    {
                        "type": "text",
                            "text": {
                                "content": " leafy vegetable",
                                "link": None
                            },
                            "annotations": {
                                "bold": False,
                                "italic": False,
                                "strikethrough": False,
                                "underline": False,
                                "code": False,
                                "color": "default"
                            },
                            "plain_text": " leafy vegetable",
                            "href": None
                        }
                ]
            },
            "Name": {
                "id": "title",
                "type": "title",
                "title": [
                    {
                        "type": "text",
                        "text": {
                            "content": "Tuscan kale",
                            "link": None
                        },
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                            "color": "default"
                        },
                        "plain_text": "Tuscan kale",
                        "href": None
                    }
                ]
            },
        },
        "url": r"https://www.notion.so/Tuscan-kale-598337872cf94fdf8782e53db20768a5",
        "public_url": None
    }

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

@pytest.fixture
def data_source_id(prefilled_client: InMemoryNotionClient, database_id: str) -> str:
    database = prefilled_client.databases_retrieve(
        path_params={"database_id": database_id}
    )
    return database["data_sources"][0]["id"]

def add_pages(client: InMemoryNotionClient, page_data: list[dict]) -> tuple[str, list[str]]:
    # As of Notion 2025-09-03 (ADR-0014) the column schema lives on the
    # database's data source (`initial_data_source.properties`) and pages parent
    # to the data source id, not the database id.
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
        'initial_data_source': {
            'properties': {
                'name': {'title': {}},
                'id': {'number': {}},
                'is_active': {'checkbox': {}},
                'start_on': {'date': {}},
                'grade': {'rich_text': {}},
            }
        }
    })

    data_source_id = students_db['data_sources'][0]['id']

    inserted_pages = []
    for data in page_data:
        page = client.pages_create(
            payload={
                'parent': {
                    'type': 'data_source_id',
                    'data_source_id': data_source_id
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

def test_client_conforms_API_version_2025_09_03(client: InMemoryNotionClient):
    # As of Notion 2025-09-03, pages.create returns fully-typed properties
    # (id + type + value), NOT the old 2022-06-28 id-only shape.
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
    assert properties['name']['type'] == 'title'
    assert properties['id']['type'] == 'number'
    assert properties['id']['number'] == 123456
    assert properties['is_active']['type'] == 'checkbox'
    assert properties['is_active']['checkbox'] is False
    assert properties['start_on']['type'] == 'date'
    assert properties['grade']['type'] == 'rich_text'

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

    resultset = ResultSet.from_json(row_description, pages[0])
    row_getters = _RowGetter(row_description)
    get_name = row_getters.getter("name")
    get_id = row_getters.getter("id")
    get_is_active = row_getters.getter("is_active")
    get_grade = row_getters.getter("grade")
    first = next(resultset)

    # As of Notion 2025-09-03, pages.create returns fully-typed properties, so
    # the result set carries populated values (not the old all-None shape).
    assert rich_text_to_plain_text(get_name(first)["title"]) == "Galileo Galilei"
    assert get_id(first) == {"number": 123456}
    assert get_is_active(first) == {"checkbox": False}
    assert rich_text_to_plain_text(get_grade(first)["rich_text"]) == "A"

def test_resultset_pages_from_json(prefilled_client: InMemoryNotionClient, data_source_id: str, row_description: tuple[tuple, ...]):
    results = prefilled_client.data_sources_query(path_params={"data_source_id": data_source_id})
    resultset = ResultSet.from_json(row_description, results)

    assert len(results["results"]) == len(resultset)

def test_resultset_fetch_first(prefilled_client: InMemoryNotionClient, data_source_id: str, row_description: tuple[tuple, ...]):
    results = prefilled_client.data_sources_query(path_params={"data_source_id": data_source_id})
    resultset = ResultSet.from_json(row_description, results)
    row_getters = _RowGetter(row_description)
    first = next(resultset)
    get_in_trash = row_getters.getter(SpecialColumns.NO_IN_TRASH)
    get_col_name = row_getters.getter("name")

    assert not get_in_trash(first)
    assert rich_text_to_plain_text(get_col_name(first)["title"]) == "Galileo Galilei"

def test_resultset_fetchall(prefilled_client: InMemoryNotionClient, data_source_id: str, row_description: tuple[tuple, ...]):
    results = prefilled_client.data_sources_query(path_params={"data_source_id": data_source_id})
    resultset = ResultSet.from_json(row_description, results)
    row_getters = _RowGetter(row_description)
    get_col_name = row_getters.getter("name")   
    all = [rich_text_to_plain_text(get_col_name(row)["title"]) for row in resultset]

    assert ["Galileo Galilei", "Isaac Newton", "Ada Lovelace"] == all

def test_resultset_database_from_json(
    prefilled_client: InMemoryNotionClient, 
    row_description: tuple[tuple, ...],
    database_id: str,
):
    database = prefilled_client.databases_retrieve(
        path_params={"database_id": database_id}
    )

    assert database["id"] == database_id
    resultset = ResultSet.from_json(row_description, database)
    get_col_name = itemgetter(0)
    rows = [row for row in resultset]

    # As of Notion 2025-09-03 (ADR-0014) a database object carries system columns
    # only; user-defined columns live on its data source and are reflected
    # separately via data_sources.retrieve. So reflecting the database yields
    # exactly the 6 system column specs, no user columns.
    assert len(resultset) == 6

    # sys cols spec
    assert get_col_name(rows[0]) == SpecialColumns.NO_ID.value
    assert get_col_name(rows[1]) == SpecialColumns.NO_DSID.value
    assert get_col_name(rows[2]) == SpecialColumns.NO_ARCHIVED.value
    assert get_col_name(rows[3]) == SpecialColumns.NO_IN_TRASH.value
    assert get_col_name(rows[4]) == SpecialColumns.NO_CREATED_TIME.value
    assert get_col_name(rows[5]) == SpecialColumns.NO_TITLE.value

def test_resultset_database_cols_from_json(prefilled_client: InMemoryNotionClient, database_id: str):
    database = prefilled_client.databases_retrieve(
        path_params={"database_id": database_id}
    )

    assert database["id"] == database_id
    resultset = ResultSet.from_json(description=None, notion_obj=database)
    description = resultset.description
    get_names = itemgetter(0)
    col_names = list(map(get_names, description))

    assert len(description) == 5
    assert ["column_name", "column_type", "column_id", "metadata", "is_system"] == col_names

def test_resultset_database_cols_from_json_extract_table_name(
    prefilled_client: InMemoryNotionClient, 
    row_description: tuple[tuple, ...],
    database_id: str
):
    database = prefilled_client.databases_retrieve(
        path_params={"database_id": database_id}
    )
    resultset = ResultSet.from_json(row_description, database)
    # value of the table_name
    get_table_name = itemgetter(3)
    # row containing the table name as metadata: NO_TITLE is now the 6th system
    # row (index 5) since NO_DSID was inserted after NO_ID (2025-09-03).
    get_table_name_metadata = itemgetter(5)
    table_name_metadata_row = get_table_name_metadata(resultset._rows)
    table_name = get_table_name(table_name_metadata_row)

    assert rich_text_to_plain_text(table_name["title"]) == "students"

def test_process_data_source_emits_user_columns(client: InMemoryNotionClient):
    # 2-phase reflection STEP 2 (#347): the 2025-09-03 column schema (property
    # ids + resolved types) lives on the DATA SOURCE, not the container.
    # _process_database emits system columns only; a new
    # ResultSet._process_data_source turns a retrieved data-source object's
    # `properties` into user-column reflection tuples in the SAME contract
    # _process_database uses for system columns:
    #   (column_name, column_type, column_id, {typ: prop[typ]}, is_system=False)
    # Scoped to plain scalar user columns (relation-value extraction is a later red).
    container = client.databases_create(
        payload={
            "parent": {"type": "page_id", "page_id": client._ROOT_PAGE_ID_},
            "title": [{"text": {"content": "students"}}],
            "initial_data_source": {
                "properties": {
                    "name": {"title": {}},
                    "age": {"number": {}},
                }
            },
        }
    )
    data_source_id = container["data_sources"][0]["id"]
    data_source = client.data_sources_retrieve(
        path_params={"data_source_id": data_source_id}
    )

    rows = ResultSet._process_data_source(data_source)

    # one reflection tuple per user-defined property, in the shared 5-field contract
    by_name = {row[0]: row for row in rows}
    assert set(by_name) == {"name", "age"}

    age = by_name["age"]
    assert age[1] == "number"          # column_type = resolved Notion type string
    assert age[2]                      # column_id = generated, non-empty prop id
    assert age[3] == {"number": {}}    # metadata = {typ: prop[typ]}
    assert age[4] is False             # user-defined, NOT a system column

def test_resultset_last_inserted_rowids_from_pages(
    prefilled_client: InMemoryNotionClient, #
    data_source_id: str,
    row_description: tuple[tuple, ...]
):
    results = prefilled_client.data_sources_query(path_params={"data_source_id": data_source_id})
    resultset = ResultSet.from_json(row_description, results)

    assert len(resultset.last_inserted_rowids) == len(resultset)    

def test_resultset_last_inserted_rowids_from_database(prefilled_client: InMemoryNotionClient, database_id: str):
    database = prefilled_client.databases_retrieve(
        path_params={"database_id": database_id}
    )

    assert database["id"] == database_id
    resultset = ResultSet.from_json(description=None, notion_obj=database)

    assert resultset.last_inserted_rowids is None

def test_rs_real_life_db_notion_object_creates_desc(database_retrieved):
    rs = ResultSet.from_json(description=None, notion_obj=database_retrieved)
    expected_cols = {"column_name", "column_type", "column_id", "metadata", "is_system"}
    desc = {colname[0] for colname in rs.description}

    assert desc == expected_cols

@pytest.mark.xfail(
    reason="2025-09-03: a database object no longer carries user columns; "
    "they now come from data_sources.retrieve. To be reworked into the "
    "_process_data_source red."
)
def test_rs_real_life_db_notion_object_creates_rows_as_metadata(database_retrieved):
    rs = ResultSet.from_json(description=None, notion_obj=database_retrieved)
    rg = _RowGetter(desc=[
        (SpecialColumns.NO_ID,),
        (SpecialColumns.NO_ARCHIVED,),
        (SpecialColumns.NO_IN_TRASH,),
        (SpecialColumns.NO_CREATED_TIME,),
        (SpecialColumns.NO_TITLE,),
        ("Price",),
        ("Description",),
        ("Name",),
    ])
    
    get_metadata = rg.value_getter("metadata")

    assert len(rs) == 8
    assert get_metadata(next(rs)) == "bc1211ca-e3f1-4939-ae34-5260b16f627c"     # object id
    assert not get_metadata(next(rs))                                           # archived
    assert not get_metadata(next(rs))                                           # in_trash
    assert get_metadata(next(rs)) == "2021-07-08T23:50:00.000Z"                 # created_time
