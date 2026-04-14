import json
from typing import Any, Dict
import pytest

from normlite._constants import SpecialColumns
from normlite.exceptions import CompileError
#from normlite.notiondbapi._model import NotionDatabase, NotionPage, NotionProperty
#from normlite.notiondbapi._parser import parse_database, parse_page, parse_property
#from normlite.notiondbapi._visitor_impl import ToDescVisitor, ToRowVisitor
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode
from normlite.notiondbapi._parser import parse_page, parse_database, parse_page_property, parse_database_property
from normlite.notiondbapi.compiler import DescriptionCompiler, RowCompiler
from normlite.notiondbapi._model import NotionDatabase, NotionPage, NotionProperty


@pytest.fixture
def created_database() -> Dict[str, Any]:
    return {
        "object": "database",
        "id": "bc1211ca-e3f1-4939-ae34-5260b16f627c",
        "parent": {
            "type": "page_id",
            "page_id": "ac1211ca-e3f1-9939-ae34-5260b16f628c"
        },
        "title": [
            {
                "type": "text",
                "text": {"content": "students"}
            }
        ],
        "properties": {
            "id": {"id": "evWq", "name": "id", "type": "number", "number": {}},
            "name": {"id": "title", "name": "name", "type": "title", "title": {}},
            "grade": {"id": "V}lX", "name": "grade", "type": "rich_text", "rich_text": {}},
        },
        "archived": False,
    }

@pytest.fixture
def page_retrieved() -> dict:
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
def page_created_or_updated() -> dict:
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
            },
            "Description": {
                "id": "_Tc_",
            },
            "Name": {
                "id": "title",
            },
        },
        "url": r"https://www.notion.so/Tuscan-kale-598337872cf94fdf8782e53db20768a5",
        "public_url": None
    }

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
def unsupported_prop_type() -> Dict[str, Any]:
    return {
        "Store availability": {
            "id": "%3AUPp",
            "type": "multi_select",
            "multi_select": [
                {
                    "id": "t|O@",
                    "name": "Gus's Community Market",
                    "color": "yellow"
                },
                {
                    "id": "{Ml\\",
                    "name": "Rainbow Grocery",
                    "color": "gray"
                }
            ]
        }
    }

@pytest.fixture
def dollar_number_def() -> Dict[str, Any]:
    return  {
        "Price": {
            "id": "evWq",
            "name": "Price",
            "type": "number",
            "number": {
                "format": "dollar"
            }
        }
    }           

@pytest.fixture
def number_def() -> Dict[str, Any]:
    return {"id": {"id": "_e:Wq", "name": "id", "type": "number", "number": {}}}

@pytest.fixture
def int_or_float_number_def() -> dict:
    return  {
        "Price": {
            "id": "%7B%5D_P",
            "name": "Price",
            "type": "number",
            "number": {
                "format": "number"
            }
        }
    }           

@pytest.fixture
def int_number_val() -> Dict[str, Any]:
    return  {
        "Price": {
            "id": "?vWq",
            "name": "Price",
            "type": "number",
            "number": 1870
        }
    }           

@pytest.fixture
def float_number_val() -> Dict[str, Any]:
    return  {
        "Price": {
            "id": "?vWq",
            "name": "Price",
            "type": "number",
            "number": 18.70
        }
    }           

def test_parse_dollar_number_def_property(dollar_number_def: dict):
    """Test that the description visitor correctly parses the number definition"""
    expected =  (
        'Price', DBAPITypeCode.NUMBER_DOLLAR, None, None, None, None, None,
    )

    property: NotionProperty = parse_database_property('Price', dollar_number_def['Price'])
    assert expected == property.compile(DescriptionCompiler())

def test_parse_number_def_property(number_def: dict):
    expected = (
        'id', DBAPITypeCode.NUMBER, None, None, None, None, None,
    )

    property: NotionProperty = parse_database_property('id', number_def['id'])
    assert expected == property.compile(DescriptionCompiler())

def test_parse_int_or_float_number_def_property(int_or_float_number_def: dict):
    expected = (
        'Price', DBAPITypeCode.NUMBER, None, None, None, None, None, 
    )

    property: NotionProperty = parse_database_property('Price', int_or_float_number_def['Price'])
    assert expected == property.compile(DescriptionCompiler())

def test_parse_int_number_val_property(int_number_val: dict):
    expected = ('Price', {'number': 1870},)
    property: NotionProperty = parse_page_property('Price', int_number_val['Price'])
    pvalue = property.compile(RowCompiler())
    assert expected == pvalue

def test_parse_float_number_val_property(float_number_val: Dict[str, Any]):
    expected = ('Price', {'number': 18.70})
    property: NotionProperty = parse_page_property('Price', float_number_val['Price'])
    pvalue = property.compile(RowCompiler())
    assert expected == pvalue

def test_parse_multi_array_rich_text(page_retrieved: dict):
    expected = ('Description', {'rich_text': [{'text': {'content': 'A dark ', 'link': None}}, {'text': {'content': 'green', 'link': None}}, {'text': {'content': ' leafy vegetable', 'link': None}}]})
    properties = page_retrieved['properties']
    property: NotionProperty = parse_page_property('Description', properties['Description'])
    pvalue = property.compile(RowCompiler())

    assert expected == pvalue

def test_parse_single_array_title(page_retrieved: dict):
    expected = {'title': [{'text': {'content': 'Tuscan kale', 'link': None}}]}
    properties = page_retrieved['properties']
    property: NotionProperty = parse_page_property('Name', properties['Name'])

    assert expected == property.value

def test_unsupported_property_type(unsupported_prop_type: Dict[str, Any]):
    
    with pytest.raises(TypeError, match="Unexpected or unsupported property type"):
        property: NotionProperty = parse_page_property(
            'Store availability', 
            unsupported_prop_type['Store availability']
        )
        desc = property.compile(DescriptionCompiler())

def test_compile_database_created(created_database: dict):
    COL_NAME = 0
    COL_ID   = 1
    COL_VAL  = 2
    ex_description = [
        (SpecialColumns.NO_ID, DBAPITypeCode.ID, None, None, None, None, None, ),
        (SpecialColumns.NO_ARCHIVED, DBAPITypeCode.CHECKBOX, None, None, None, None, None, ),
        (SpecialColumns.NO_TITLE, DBAPITypeCode.TITLE, None, None, None, None, None, ),
        ('id', DBAPITypeCode.NUMBER, None, None, None, None, None, ),
        ('name', DBAPITypeCode.TITLE, None, None, None, None, None,),
        ('grade', DBAPITypeCode.RICH_TEXT, None, None, None, None, None, ),
    ]
    database_object: NotionDatabase = parse_database(created_database)
    description = database_object.compile(DescriptionCompiler())
    row_data = database_object.compile(RowCompiler())
    assert ex_description == description
    assert len(row_data) == 6
    first_col = row_data[0]
    last_col = row_data[-1]

    assert first_col[COL_NAME] == SpecialColumns.NO_ID
    assert first_col[COL_ID] is None
    assert first_col[COL_VAL] == 'bc1211ca-e3f1-4939-ae34-5260b16f627c'
    assert last_col[COL_NAME] == 'grade'
    assert last_col[COL_ID] == 'V}lX'
    assert last_col[COL_VAL] is None

def test_compile_database_retrieved(database_retrieved: dict):
    # description returned by DBAPI Cursor.description
    # The DBAPI description provides a sequence of column names and DBAPI types
    ex_description = [
        (SpecialColumns.NO_ID, DBAPITypeCode.ID, None, None, None, None, None, ),
        (SpecialColumns.NO_ARCHIVED, DBAPITypeCode.CHECKBOX, None, None, None, None, None, ),
        (SpecialColumns.NO_TITLE, DBAPITypeCode.TITLE, None, None, None, None, None, ),
        ('Price', DBAPITypeCode.NUMBER_DOLLAR, None, None, None, None, None, ),
        ('Description', DBAPITypeCode.RICH_TEXT, None, None, None, None, None, ),
        ('Name', DBAPITypeCode.TITLE, None, None, None, None, None,),
    ]

    notion_database = parse_database(database_retrieved)
    description = notion_database.compile(DescriptionCompiler())
    assert ex_description == description

def test_compile_page_created_or_updated(page_created_or_updated: dict):
    # created pages return the object and property ids as result
    expected = [
        (SpecialColumns.NO_ID, '59833787-2cf9-4fdf-8782-e53db20768a5',), 
        (SpecialColumns.NO_ARCHIVED, False,), 
        (SpecialColumns.NO_IN_TRASH, None,), 
        ('Price', 'BJXS',), 
        ('Description', '_Tc_',), 
        ('Name', 'title',)
    ]
    page_created: NotionPage = parse_page(page_created_or_updated)
    row = page_created.compile(RowCompiler())
    assert all([p.is_page_created_or_updated for p in page_created.properties])
    assert expected == row

def test_compile_page_retrieved(page_retrieved: Dict[str, Any]):
    expected = [
        (SpecialColumns.NO_ID, '59833787-2cf9-4fdf-8782-e53db20768a5',),
        (SpecialColumns.NO_ARCHIVED, False,),
        (SpecialColumns.NO_IN_TRASH, None,),
        ('Price', {'number': 2.5},), 
        ('Description', {'rich_text': [{'text': {'content': 'A dark ', 'link': None}}, {'text': {'content': 'green', 'link': None}}, {'text': {'content': ' leafy vegetable', 'link': None}}]},), 
        ('Name', {'title': [{'text': {'content': 'Tuscan kale', 'link': None}}]},),
    ]
    
    page_object: NotionPage = parse_page(page_retrieved)
    row = page_object.compile(RowCompiler())
    assert expected == row

def test_desc_visitor_for_page_created_or_updated(page_created_or_updated: dict):
    expected = [
        (SpecialColumns.NO_ID, DBAPITypeCode.ID, None, None, None, None, None,),
        (SpecialColumns.NO_ARCHIVED, DBAPITypeCode.CHECKBOX, None, None, None, None, None,),
        (SpecialColumns.NO_IN_TRASH, DBAPITypeCode.CHECKBOX, None, None, None, None, None),
        ('Price', DBAPITypeCode.PROPERTY_ID, None, None, None, None, None,),
        ('Description', DBAPITypeCode.PROPERTY_ID, None, None, None, None, None,),
        ('Name', DBAPITypeCode.PROPERTY_ID, None, None, None, None, None,),

    ]
    page: NotionPage = parse_page(page_created_or_updated)
    description = page.compile(DescriptionCompiler())
    assert expected == description

def test_desc_visitor_for_page_retrieved(page_retrieved: dict):
    expected = [
        (SpecialColumns.NO_ID, DBAPITypeCode.ID, None, None, None, None, None,),
        (SpecialColumns.NO_ARCHIVED, DBAPITypeCode.CHECKBOX, None, None, None, None, None,),
        (SpecialColumns.NO_IN_TRASH, DBAPITypeCode.CHECKBOX, None, None, None, None, None),
        ('Price', DBAPITypeCode.NUMBER_WITH_COMMAS, None, None, None, None, None,),
        ('Description', DBAPITypeCode.RICH_TEXT, None, None, None, None, None,),
        ('Name', DBAPITypeCode.TITLE, None, None, None, None, None,),
    ]

    page = parse_page(page_retrieved)
    description = page.compile(DescriptionCompiler())
    assert expected == description