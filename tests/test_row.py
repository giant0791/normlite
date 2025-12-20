from decimal import Decimal
import json
import pdb
import pytest

from normlite._constants import SpecialColumns
from normlite.future.engine.resultmetadata import CursorResultMetaData
from normlite.notiondbapi._parser import parse_database, parse_page
from normlite.notiondbapi.compiler import DescriptionCompiler, RowCompiler
from normlite.future.engine.reflection import ReflectedColumnInfo
from normlite.future.engine.row import Row
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode
from normlite.sql.type_api import Boolean, Money, ObjectId, String


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

def test_parse_database_into_row(database_retrieved: dict):
    # description returned by DBAPI Cursor.description
    # The DBAPI description provides a sequence of column names and DBAPI types
    # 
    # ex_description = [
    #   ('name', DBAPITypeCode.META, None, None, None, None, None,),
    #   ('type', DBAPITypeCode.META, None, None, None, None, None,),
    #   ('id', DBAPITypeCode.META, None, None, None, None, None,),
    #   ('value', DBAPITypeCode.META, None, None, None, None, None,),
    # ]

    notion_database = parse_database(database_retrieved)
    rd_compiler = DescriptionCompiler()
    description = notion_database.compile(rd_compiler)
    assert description == description
    

    # row_data object returned by DBAPI Cursor.fetchall()
    # row_data = Sequence[tuple[
    #   str,                    # column name
    #   Optional[str],          # optional column id
    #   Optional[dict],         # optional type engine argument
    #   Any                     # Python value
    # ]]
    # The DBAPI cursor fetchers provide a sequence of column names and their associated Notion ids and values
    # ex_row_data = [
    #   (SpecialColumns.NO_ID, None, 'bc1211ca-e3f1-4939-ae34-5260b16f627c',),
    #   (SpecialColumns.NO_ARCHIVED, None, False, ),
    #   (SpecialColumns.NO_TITLE, None, 'Grocery List',), 
    #   ('Price', 'evWq', None, ),        
    #   ('Description', 'V}lX', None,),
    # ]

    row_compiler = RowCompiler()
    row_data = notion_database.compile(row_compiler)
    #assert ex_row_data == row_data

    # Row object procured by the cursor result contains the following columns
    # SchemaRowType: tuple[
    #   str,                # column name
    #                       # no type needed here because it is encoded in the description
    #   Optional[str],      # optional Notion property id
    #   Optional[Any]       # optional Python value
    # ]
    # The row object delivered by the CursorResult delivers column names and 
    # their associated Python type engines, column ids and Python values
    ex_row = [
        ReflectedColumnInfo(SpecialColumns.NO_ID, ObjectId(), None, 'bc1211ca-e3f1-4939-ae34-5260b16f627c'),
        ReflectedColumnInfo(SpecialColumns.NO_ARCHIVED, Boolean(), None, False),
    	ReflectedColumnInfo(SpecialColumns.NO_TITLE, String(is_title=True), None, 'Grocery List'), 
        ReflectedColumnInfo('Price', Money(currency='dollar'), 'evWq', None),        
        ReflectedColumnInfo('Description', String(is_title=False), 'V}lX', None),
        ReflectedColumnInfo('Name', String(is_title=True), 'title', None),
    ]

    metadata = CursorResultMetaData(True, description)
    rows = [Row(metadata, dbapi_row) for dbapi_row in row_data]
    

def test_parse_page_into_row(retrieved_page: dict):
    row_description = [
        (SpecialColumns.NO_ID, DBAPITypeCode.ID, None, None, None, None, None, ),
        (SpecialColumns.NO_ARCHIVED, DBAPITypeCode.CHECKBOX, None, None, None, None, None, ),
        (SpecialColumns.NO_IN_TRASH, DBAPITypeCode.CHECKBOX, None, None, None, None, None, ),
        ('Price', DBAPITypeCode.NUMBER_WITH_COMMAS, None, None, None, None, None, ),
        ('Description', DBAPITypeCode.RICH_TEXT, None, None, None, None, None, ),
        ('Name', DBAPITypeCode.TITLE, None, None, None, None, None, ),
    ]

    notion_page = parse_page(retrieved_page)
    rd_compiler = DescriptionCompiler()
    description = notion_page.compile(rd_compiler)
    assert description == row_description

    row_visitor = RowCompiler()
    metadata = CursorResultMetaData(False, row_description)
    row_data = notion_page.compile(row_visitor)
    #pdb.set_trace()
    row = Row(metadata, row_data)
    row_as_tuple = (
            '59833787-2cf9-4fdf-8782-e53db20768a5', 
            False, 
            None,
            Decimal('2.5'), 
            'A dark green leafy vegetable', 
            'Tuscan kale'
    )
    
    assert row.as_tuple() == row_as_tuple
    assert str(row) == str(row_as_tuple)
    assert row._no_id == '59833787-2cf9-4fdf-8782-e53db20768a5'
    assert not row._no_archived
    assert 'Name' in row.keys()
    assert row.Name == 'Tuscan kale'
    assert row.Price == Decimal(2.5)
    assert isinstance(row.Price, Decimal)
    assert row.Description == 'A dark green leafy vegetable'
