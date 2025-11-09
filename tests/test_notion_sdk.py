import pdb
from typing import Any, Dict
from datetime import datetime
import uuid

import pytest
from normlite.notion_sdk.client import FileBasedNotionClient, InMemoryNotionClient

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

@pytest.fixture
def inmem_client() -> InMemoryNotionClient:
    return InMemoryNotionClient()

@pytest.fixture
def file_path() -> str:
    return './notion-store.db'

@pytest.fixture
def filebased_client(file_path: str):
    with FileBasedNotionClient(file_path) as client:
        yield client 

def test_add_private_method(inmem_client: InMemoryNotionClient):
    payload = dict()
    parent = dict(type='page_id', page_id=inmem_client.ischema_page_id)
    properties = {
        'table_name': {'title': {}},
        'table_schema': {'rich_text': {}},
        'table_catalog': {'rich_text': {}},
        'table_id': {'rich_text': {}}
    }
    payload['parent'] = parent
    payload['properties']= properties
    payload['title'] = [{'type': 'text', 'text': {'content': 'tables'}}]

    database_obj = inmem_client._add('database', payload)
    #pdb.set_trace()
    properties = database_obj['properties']
    assert properties['table_name']['type'] == 'title'
    assert properties['table_schema']['type'] == 'rich_text'
    assert properties['table_catalog']['type'] == 'rich_text'
    assert properties['table_id']['type'] == 'rich_text'

def _compare_property_ids(obj: dict, other: dict) -> bool:
    """Helper function to compare the property ids of two objects"""
    props = obj.get('properties')
    otherprops = other.get('properties')
    pkeys = list(props.keys())
    otherkeys = list(otherprops.keys())
    if pkeys != otherkeys:
        return False
    
    pids = {'id': props[key]['id'] for key in pkeys}
    otherids = {'id': otherprops[key]['id'] for key in otherkeys}

    return pids == otherids

def test_add_page_to_page(inmem_client: InMemoryNotionClient):
    pg_payload = {
        'parent': {
            'type': "page_id",
            'page_id': "98ad959b-2b6a-4774-80ee-00246fb0ea9b",
        },
        'properties': {
            'Name': {'title': {}},
            'Description': {'rich_text': {}}
        }
    }
    new_pg_obj = inmem_client.pages_create(pg_payload)
    retrieved_pg_obj = inmem_client.pages_retrieve({'id': new_pg_obj['id']})
    assert _compare_property_ids(new_pg_obj, retrieved_pg_obj)


def test_add_page_to_database(inmem_client: InMemoryNotionClient):
    # 1. Create a new database
    db_payload = {
        'parent': {
            'type': "page_id",
            'page_id': "98ad959b-2b6a-4774-80ee-00246fb0ea9b",
        },
        'title': [{
            'type': 'text',
            'text': {'content': 'Grocery List', 'link': None}
        }],
        'properties': {
            'Name': {'title': {}},
            'Description': {'rich_text': {}}
        }
    }
    new_db_obj = inmem_client.databases_create(db_payload)
    assert new_db_obj['parent']['page_id'] == "98ad959b-2b6a-4774-80ee-00246fb0ea9b"
    assert not new_db_obj['archived'] 
    assert not new_db_obj['in_trash']
    property_keys = [k for k in new_db_obj['properties'].keys()]
    assert property_keys == ['Name', 'Description']

    # 2. Add a new page to the newly created database
    dbid = new_db_obj['id']
    pg_payload = {
        'parent': {
            'type': 'database_id',
            'database_id': dbid
        },
        'properties': {
            'Name': {
                'title': [{'text': {'content': 'Tuscan kale'}}]
            },
            'Description': {
                'rich_text': [{'text': {'content': 'A dark green leafy vegetable'}}]
            }
        }
    }

    new_pg_obj = inmem_client.pages_create(pg_payload)
    assert _compare_property_ids(new_pg_obj, new_db_obj)

def test_add_database(inmem_client: InMemoryNotionClient):
    # 1. Create a new database
    db_payload = {
        'parent': {
            'type': "page_id",
            'page_id': "98ad959b-2b6a-4774-80ee-00246fb0ea9b",
        },
        'title': [{
            'type': 'text',
            'text': {'content': 'Grocery List', 'link': None}
        }],
        'properties': {
            'Name': {'title': {}},
            'Description': {'rich_text': {}}
        }
    }
    new_db_obj = inmem_client.databases_create(db_payload)
    retrieved_db_obj = inmem_client.databases_retrieve({'id': new_db_obj['id']})
    assert new_db_obj == retrieved_db_obj
