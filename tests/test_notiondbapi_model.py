import json
import pdb
from typing import Any, Dict
import pytest

from normlite._constants import SpecialColumns
from normlite.notiondbapi._model import NotionDatabase, NotionPage, NotionProperty
from normlite.notiondbapi._parser import parse_database, parse_page, parse_property
from normlite.notiondbapi._visitor_impl import ToDescVisitor, ToRowVisitor

@pytest.fixture
def created_database() -> Dict[str, Any]:
    return {
        "object": "database",
        "id": "bc1211ca-e3f1-4939-ae34-5260b16f627c",
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
def created_page() -> dict:
    return {
        "object": "page",
        "id": "59833787-2cf9-4fdf-8782-e53db20768a5",
        "archived": False,
        "properties": {
            "id": {
                "id": "%3AUPp",
            },
            "name": {
                "id": "A%40Hk",
           },
            "grade": {
                "id": "BJXS",
            }
        }
    }

@pytest.fixture
def updated_page() -> dict:
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
        'Price', 'number.dollar', 'evWq', None, None, None, None,
    )

    property: NotionProperty = parse_property('Price', dollar_number_def['Price'])
    assert expected == property.accept(ToDescVisitor())

def test_parse_number_def_property(number_def: dict):
    expected = (
        'id', 'number', '_e:Wq', None, None, None, None,
    )

    property: NotionProperty = parse_property('id', number_def['id'])
    assert expected == property.accept(ToDescVisitor())

def test_parse_int_or_float_number_def_property(int_or_float_number_def: dict):
    expected = (
        'Price', 'number.number', '%7B%5D_P', None, None, None, None, 
    )

    property: NotionProperty = parse_property('Price', int_or_float_number_def['Price'])
    assert expected == property.accept(ToDescVisitor())

def test_parse_int_number_val_property(int_number_val: dict):
    expected = 1870
    property: NotionProperty = parse_property('Price', int_number_val['Price'])
    pvalue = property.accept(ToRowVisitor())
    assert expected == pvalue

def test_parse_float_number_val_property(float_number_val: Dict[str, Any]):
    expected = 18.70

    property: NotionProperty = parse_property('Price', float_number_val['Price'])
    assert expected == property.accept(ToRowVisitor())

def test_parse_multi_array_rich_text(retrieved_page: Dict[str, Any]):
    expected = 'A dark green leafy vegetable'
    properties = retrieved_page['properties']
    property: NotionProperty = parse_property('Description', properties['Description'])

    assert expected == property.value

def test_parse_single_array_title(retrieved_page: Dict[str, Any]):
    expected = 'Tuscan kale'
    properties = retrieved_page['properties']
    property: NotionProperty = parse_property('Name', properties['Name'])

    assert expected == property.value

def test_unsupported_property_type(unsupported_prop_type: Dict[str, Any]):
    
    with pytest.raises(TypeError, match="Unexpected or unsupported property type"):
        property: NotionProperty = parse_property(
            'Store availability', 
            unsupported_prop_type['Store availability']
        )

def test_compile_page_created(created_page: dict):
    # created pages return the object and property ids as result
    expected = (
        "59833787-2cf9-4fdf-8782-e53db20768a5",         
        False, 
        None,                                    
        '%3AUPp', 
        'A%40Hk', 
        'BJXS',                    
    )
    page_object: NotionPage = parse_page(created_page)
    visitor = ToRowVisitor()
    row = page_object.accept(visitor)
    assert expected == row

def test_compile_database_retrieved(database_retrieved: dict):
    expected = (
        'bc1211ca-e3f1-4939-ae34-5260b16f627c',
        'Grocery List',
        False,
        None,                                   
        'evWq',                    
        'V}lX', 
        'title',
     )
    database_object: NotionDatabase = parse_database(database_retrieved)
    visitor = ToRowVisitor()
    row = database_object.accept(visitor)
    assert expected == row

def test_compile_page_retrieved(retrieved_page: Dict[str, Any]):
    expected = (
        '59833787-2cf9-4fdf-8782-e53db20768a5',
        False, None,
        2.5,
        'A dark green leafy vegetable',
        'Tuscan kale',
    )
    
    page_object: NotionPage = parse_page(retrieved_page)
    visitor = ToRowVisitor()
    row = page_object.accept(visitor)
    assert expected == row

def test_compile_page_updated(updated_page: Dict[str, Any]):
    expected = (
        '59833787-2cf9-4fdf-8782-e53db20768a5',
        False, None,
        'BJXS', '_Tc_', 'title',                    # no values available, ids are returned
    )
    
    page_object: NotionPage = parse_page(updated_page)
    visitor = ToRowVisitor()
    row = page_object.accept(visitor)
    assert expected == row

def test_to_desc_visitor_for_database_created(created_database):
    """
    Given I have created a table (Notion database)
    When I cross-compile the parsed JSON object returned by Notion
    Then I get a database descriptor and the tuples describing the columns have column names and types
    
    """
    expected = (
        (SpecialColumns.NO_ID, 'string', None, None, None, None, None,),
        (SpecialColumns.NO_TITLE, 'string', None, None, None, None, None,),
        (SpecialColumns.NO_ARCHIVED, 'boolean', None, None, None, None, None,),
        (SpecialColumns.NO_IN_TRASH, 'boolean', None, None, None, None, None),
        ('id', 'number', 'evWq', None, None, None, None,),
        ('name', 'title', 'title', None, None, None, None,),
        ('grade', 'rich_text', 'V}lX', None, None, None, None,),
    )
    
    database: NotionDatabase = parse_database(created_database)
    visitor = ToDescVisitor()
    description = database.accept(visitor)

    assert expected == description

def test_desc_visitor_for_page_created(created_page: dict):
    expected = (
        (SpecialColumns.NO_ID, 'string',  None, None, None, None, None,),
        (SpecialColumns.NO_ARCHIVED, 'boolean', None, None, None, None, None,),
        (SpecialColumns.NO_IN_TRASH, 'boolean', None, None, None, None, None),
        ('id', None, '%3AUPp', None, None, None, None),
        ('name', None, 'A%40Hk', None, None, None, None),
        ('grade', None, 'BJXS', None, None, None, None),
    )

    page: NotionPage = parse_page(created_page)
    visitor = ToDescVisitor()
    description = page.accept(visitor)

    assert expected == description

def test_desc_visitor_for_page_retrieved(retrieved_page: dict):
    expected = (
        (SpecialColumns.NO_ID, 'string', None, None, None, None, None,),
        (SpecialColumns.NO_ARCHIVED, 'boolean', None, None, None, None, None,),
        (SpecialColumns.NO_IN_TRASH, 'boolean', None, None, None, None, None),
        ('Price', 'number', 'BJXS', None, None, None, None,),
        ('Description', 'rich_text', '_Tc_', None, None, None, None,),
        ('Name', 'title', 'title', None, None, None, None,),
    )

    page: NotionPage = parse_page(retrieved_page)
    visitor = ToDescVisitor()
    description = page.accept(visitor)

    assert expected == description

def test_desc_visitor_for_page_updated(updated_page: dict):
    expected = (
        (SpecialColumns.NO_ID, 'string', None, None, None, None, None,),
        (SpecialColumns.NO_ARCHIVED, 'boolean', None, None, None, None, None,),
        (SpecialColumns.NO_IN_TRASH, 'boolean', None, None, None, None, None),
        ('Price', None, 'BJXS', None, None, None, None,),
        ('Description', None, '_Tc_', None, None, None, None,),
        ('Name', None, 'title', None, None, None, None,),
    )

    page: NotionPage = parse_page(updated_page)
    visitor = ToDescVisitor()
    description = page.accept(visitor)

    assert expected == description

def test_desc_visitor_for_retrieved_database(database_retrieved: dict):
    expected = (
        (SpecialColumns.NO_ID, 'string', None, None, None, None, None,),
        (SpecialColumns.NO_TITLE, 'string', None, None, None, None, None,),
        (SpecialColumns.NO_ARCHIVED, 'boolean', None, None, None, None, None,),
        (SpecialColumns.NO_IN_TRASH, 'boolean', None, None, None, None, None),
        ('Price', 'number.dollar', 'evWq', None, None, None, None,),
        ('Description', 'rich_text', 'V}lX', None, None, None, None,),
        ('Name', 'title', 'title', None, None, None, None,),
    )

    database: NotionDatabase = parse_database(database_retrieved)
    visitor = ToDescVisitor()
    description = database.accept(visitor)

    assert expected == description
    


