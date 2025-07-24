import json
import pdb
from typing import Any, Dict
import pytest

from normlite.notiondbapi._model import NotionDatabase, NotionPage, NotionProperty
from normlite.notiondbapi._parser import parse_database, parse_page, parse_property
from normlite.notiondbapi._visitor_impl import ToRowVisitor

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
    }

@pytest.fixture
def retrieved_page() -> Dict[str, Any]:
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
def created_page() -> Dict[str, Any]:
    return {
        "object": "page",
        "id": "59833787-2cf9-4fdf-8782-e53db20768a5",
        "archived": False,
        "properties": {
            "id": {
                "id": "%3AUPp",
                "type": "number",
                "number": 1
            },
            "name": {
                "id": "A%40Hk",
                "type": "title",
                "title": [
                    {
                        "type": "text",
                        "text": {
                            "content": "Isaac Newton"
                        }
                    }
                ]
            },
            "grade": {
                "id": "BJXS",
                "type": "rich_text",
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "B"
                        }
                    }
                ]
            }
        }
    }

@pytest.fixture
def updated_page() -> Dict[str, Any]:
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
def database_retrieved() -> Dict[str, Any]:
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
def int_or_float_number_def() -> Dict[str, Any]:
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

def test_parse_dollar_number_def_property(dollar_number_def: Dict[str, Any]):
    expected =  (
        'Price', 'evWq', 'number', 'dollar',
    )

    property: NotionProperty = parse_property('Price', dollar_number_def['Price'])
    assert expected == property.accept(ToRowVisitor())

def test_parse_number_def_property(number_def: Dict[str, Any]):
    expected = (
        'id', '_e:Wq', 'number', None,
    )

    property: NotionProperty = parse_property('id', number_def['id'])
    assert expected == property.accept(ToRowVisitor())

def test_parse_int_or_floar_number_def_property(int_or_float_number_def: Dict[str, Any]):
    expected = (
        'Price', '%7B%5D_P', 'number', 'number',
    )

    property: NotionProperty = parse_property('Price', int_or_float_number_def['Price'])
    assert expected == property.accept(ToRowVisitor())

def test_parse_int_number_val_property(int_number_val: Dict[str, Any]):
    expected = (
        'Price', '?vWq', 'number', 1870,
    )

    property: NotionProperty = parse_property('Price', int_number_val['Price'])
    assert expected == property.accept(ToRowVisitor())

def test_parse_float_number_val_property(float_number_val: Dict[str, Any]):
    expected = (
        'Price', '?vWq', 'number', 18.70,
    )

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


def test_compile_page_created(created_page: Dict[str, Any]):
    expected = (
        'page',                                         # metadata: object type
        "59833787-2cf9-4fdf-8782-e53db20768a5",         # metadata object id
        False, None,                                    # archive is false, in_trash is missing in the object
        'id', '%3AUPp', 'number', 1,                    # first property: property_name, property_id, property_type, property_value
        'name', 'A%40Hk', 'title', 'Isaac Newton',
        'grade', 'BJXS', 'rich_text', 'B',
    )
    page_object: NotionPage = parse_page(created_page)
    visitor = ToRowVisitor()
    row = page_object.accept(visitor)
    assert expected == row

def test_compile_database_created(created_database: Dict[str, Any]):
    expected = (
        'database',
        'bc1211ca-e3f1-4939-ae34-5260b16f627c',
        'students',
        None, None,                                     # both archived and in_trash are missing
        'id', 'evWq', 'number', None,                   # number property is defined, value is None
        'name', 'title', 'title', None,
        'grade', 'V}lX', 'rich_text', None,
    )
    database_object: NotionDatabase = parse_database(created_database)
    visitor = ToRowVisitor()
    row = database_object.accept(visitor)
    assert expected == row

def test_compile_page_retrieved(retrieved_page: Dict[str, Any]):
    expected = (
        'page',
        '59833787-2cf9-4fdf-8782-e53db20768a5',
        False, None,
        'Price', 'BJXS', 'number', 2.5,
        'Description', '_Tc_', 'rich_text', 'A dark green leafy vegetable',
        'Name', 'title', 'title', 'Tuscan kale'
    )
    
    page_object: NotionPage = parse_page(retrieved_page)
    visitor = ToRowVisitor()
    row = page_object.accept(visitor)
    assert expected == row

def test_compile_page_updated(updated_page: Dict[str, Any]):
    expected = (
        'page',
        '59833787-2cf9-4fdf-8782-e53db20768a5',
        False, None,
        'Price', 'BJXS', None, None,
        'Description', '_Tc_', None, None,
        'Name', 'title', None, None
    )
    
    page_object: NotionPage = parse_page(updated_page)
    visitor = ToRowVisitor()
    row = page_object.accept(visitor)
    assert expected == row
