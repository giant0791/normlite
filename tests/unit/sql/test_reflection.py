import json
import pdb
import pytest

from normlite._constants import SpecialColumns
from normlite.engine.base import Engine, Inspector, create_engine
from normlite.engine.cursor import CursorResult
from normlite.sql.reflection import ReflectedTableInfo
from normlite.notiondbapi.dbapi2 import Cursor
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode
from normlite.sql.base import DDLCompiled
from normlite.sql.schema import MetaData, Table

@pytest.fixture
def database_retrieved() -> dict:
    json_obj = """
    {
        "object": "database",
        "id": "bc1211ca-e3f1-4939-ae34-5260b16f627c",
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
def db_as_tuples(database_retrieved: dict) -> list[tuple]:
    tuple_seq = [
        (SpecialColumns.NO_ID, DBAPITypeCode.ID, None, database_retrieved["id"], True),
        (SpecialColumns.NO_ARCHIVED, DBAPITypeCode.ARCHIVAL_FLAG, None, database_retrieved["archived"], True),
        (SpecialColumns.NO_IN_TRASH, DBAPITypeCode.ARCHIVAL_FLAG, None, database_retrieved["in_trash"], True),
        (SpecialColumns.NO_CREATED_TIME, DBAPITypeCode.TIMESTAMP, None, database_retrieved["created_time"], True),
        (SpecialColumns.NO_TITLE, DBAPITypeCode.TITLE, None, database_retrieved["title"], True), 
    ]
    for key, value in database_retrieved["properties"].items():
        typ = value["type"]
        tuple_seq.append(
            (key, typ, value["id"], value[typ], False, )
        )

    return tuple_seq

@pytest.fixture
def inspector(engine: Engine) -> Inspector:
    return engine.inspect()

def test_reflect_table_info_from_dict(database_retrieved: dict):
    reflected_table_info = ReflectedTableInfo.from_dict(database_retrieved)
    usr_column_names = [c.name for c in reflected_table_info.get_user_columns()]
    
    assert reflected_table_info.id == 'bc1211ca-e3f1-4939-ae34-5260b16f627c'
    assert reflected_table_info.name == 'Grocery List'
    assert len(reflected_table_info.get_columns()) == 8
    assert usr_column_names == ['Price', 'Description', 'Name']

def test_reflect_table_info_from_tuple(db_as_tuples):

    reflected_table_info = ReflectedTableInfo.from_tuples(db_as_tuples)
    usr_column_names = [c.name for c in reflected_table_info.get_user_columns()]

    assert reflected_table_info.id == 'bc1211ca-e3f1-4939-ae34-5260b16f627c'
    assert reflected_table_info.name == 'Grocery List'
    assert len(reflected_table_info.get_columns()) == 8
    assert usr_column_names == ['Price', 'Description', 'Name']


def test_reflect_from_dict_tuples_is_invariant(db_as_tuples, database_retrieved):
    rti1 = ReflectedTableInfo.from_tuples(db_as_tuples)
    rti2 = ReflectedTableInfo.from_dict(database_retrieved)

    assert rti1.id == rti2.id
    assert rti1.name == rti2.name
    assert len(rti1.get_columns()) == len(rti2.get_columns())
