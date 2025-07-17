import pdb
from typing import Any, Dict
from datetime import datetime
import uuid
from normlite.notion_sdk.client import AbstractNotionClient

def is_valid_isodt(dt_str: str) -> bool:
    try:
        datetime.fromisoformat(dt_str)
    except:
        return False
    
    return True

def is_valid_uuid(uuid_str: str) -> bool:
    try:
        uuid_obj = uuid.UUID(uuid_str, version=4)
    except:
        return False
    
    return True

def test_add_private_method(client: AbstractNotionClient):
    payload = dict()
    parent = dict(type='page_id', page_id=client.ischema_page_id)
    properties = {
        'table_name': {'title': {}},
        'table_schema': {'rich_text': {}},
        'table_catalog': {'rich_text': {}},
        'table_id': {'rich_text': {}}
    }
    payload['parent'] = parent
    payload['properties']= properties
    payload['title'] = [{'type': 'text', 'text': {'content': 'tables'}}]

    database_obj = client._add('database', payload)
    #pdb.set_trace()
    properties = database_obj['properties']
    assert properties['table_name']['type'] == 'title'
    assert properties['table_schema']['type'] == 'rich_text'
    assert properties['table_catalog']['type'] == 'rich_text'
    assert properties['table_id']['type'] == 'rich_text'


def test_pages_create_old_interface(client: AbstractNotionClient):
    page_object = {
        'parent': {
            'type': 'database_id',
            'database_id': 'd9824bdc-8445-4327-be8b-5b47500af6ce'
        },
        'properties': {
            'Name': {
                'title': [
                    {
                        'text': {'content': 'Tuscan kale'}
                    }
                ]
            },
            'Description': {
                'rich_text': [
                    {
                        'text': {'content': 'A dark green leafy vegetable'}
                    }
                ]
            }
        }
    }
    new_page: Dict[str, Any] = client.pages_create(page_object)
    retrieved_page: Dict[str, Any] = client.pages_retrieve(new_page)

    assert retrieved_page is not {}
    assert retrieved_page == new_page

def test_pages_create_new_interface(client: AbstractNotionClient):
    page_object = {
        'parent': {
            'type': 'database_id',
            'database_id': 'd9824bdc-8445-4327-be8b-5b47500af6ce'
        },
        'properties': {
            'Name': {
                'title': [
                    {
                        'text': {'content': 'Tuscan kale'}
                    }
                ]
            },
            'Description': {
                'rich_text': [
                    {
                        'text': {'content': 'A dark green leafy vegetable'}
                    }
                ]
            }
        }
    }
    new_page: Dict[str, Any] = client('pages', 'create', page_object)
    retrieved_page: Dict[str, Any] = client('pages', 'retrieve', new_page)

    assert retrieved_page is not {}
    assert retrieved_page == new_page
