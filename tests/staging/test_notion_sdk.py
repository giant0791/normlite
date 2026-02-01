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
    client = InMemoryNotionClient()
    ischema_page = {
        'parent' : {
            'type': 'page_id',
            'page_id': str(uuid.uuid4())
        },
        'properties': {
            'Name': {
                'title': [{
                    'type': 'text',
                    'text': {'content': 'INFORMATION_SCHEMA', 'link': None}
                }]
            }
        }
    }
    ischema_page_obj = client._add('page', ischema_page)
    client._ischema_page_id = ischema_page_obj['id']
    return client

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
            'page_id': inmem_client.ischema_page_id,
        },
        'properties': {
            'Name': {'title': {}},
            'Description': {'rich_text': {}}
        }
    }
    new_pg_obj = inmem_client.pages_create(payload=pg_payload)
    retrieved_pg_obj = inmem_client.pages_retrieve(
        path_params={'page_id': new_pg_obj['id']},
    )
    assert _compare_property_ids(new_pg_obj, retrieved_pg_obj)

def test_add_page_to_database(inmem_client: InMemoryNotionClient):
    # 1. Create a new database
    db_payload = {
        'parent': {
            'type': "page_id",
            'page_id': inmem_client.ischema_page_id,
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
    new_db_obj = inmem_client.databases_create(payload=db_payload)
    assert new_db_obj['parent']['page_id'] == inmem_client.ischema_page_id
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

    new_pg_obj = inmem_client.pages_create(payload=pg_payload)
    assert _compare_property_ids(new_pg_obj, new_db_obj)

def test_add_database(inmem_client: InMemoryNotionClient):
    # 1. Create a new database
    db_payload = {
        'parent': {
            'type': "page_id",
            'page_id': inmem_client.ischema_page_id,
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
    new_db_obj = inmem_client.databases_create(payload=db_payload)
    retrieved_db_obj = inmem_client.databases_retrieve(path_params={'database_id': new_db_obj['id']})
    assert new_db_obj == retrieved_db_obj

def _add_page_to_database(
        inmem_client: InMemoryNotionClient, 
        database_id: str,
        *,
        name: str,
        description: str,
        price: float
    ):
    payload = {
        'parent': {
            'type': 'database_id',
            'database_id': database_id
        },
        'properties': {
            'name': {
                'title': [{'text': {'content': name}}]
            },
            'description': {
                'rich_text': [{'text': {'content': description}}]
            },
            'price': {
                'number': price
            }
            
        }
    }
    
    inmem_client.pages_create(payload=payload)

def _setup_test_for_database_query(inmem_client: InMemoryNotionClient):
    # 1. Create a new database
    db_payload = {
        'parent': {
            'type': "page_id",
            'page_id': inmem_client.ischema_page_id,
        },
        'title': [{
            'type': 'text',
            'text': {'content': 'Grocery List', 'link': None}
        }],
        'properties': {
            'name': {'title': {}},
            'description': {'rich_text': {}},
            'price': {'number': {'format': 'number_with_commas'}}
        }
    }
    database = inmem_client.databases_create(payload=db_payload)

    # 2. add some pages
    _add_page_to_database(
        inmem_client, 
        database['id'],
        name='Spinach',
        description='A vegetable',
        price=0.5
    )

    _add_page_to_database(
        inmem_client, 
        database['id'],
        name='Banana',
        description='A fruit',
        price=2.5
    )

    _add_page_to_database(
        inmem_client, 
        database['id'],
        name='Potato',
        description='Another vegetable',
        price=0.75
    )

    return database['id']


def test_query_database(inmem_client: InMemoryNotionClient):
    database_id = _setup_test_for_database_query(inmem_client)
    filter_obj = {
            'and': [
                {
                    'property': 'description',
                    'rich_text': {'contains': 'vegetable'}
                },
                {
                    'property': 'price',
                    'number': {'less_than': 2.5}
                }
            ]
    }

    result_pages = inmem_client.databases_query(
        path_params={'database_id': database_id},
        payload={'filter': filter_obj}
    ) 

    assert len(result_pages['results']) == 2

def test_query_database_with_filter_properties(inmem_client: InMemoryNotionClient):
    database_id = _setup_test_for_database_query(inmem_client)

    filter_obj = {
            'and': [
                {
                    'property': 'description',
                    'rich_text': {'contains': 'vegetable'}
                },
                {
                    'property': 'price',
                    'number': {'less_than': 2.5}
                }
            ]
    }

    result_pages = inmem_client.databases_query(
        path_params={'database_id': database_id},
        query_params={'filter_properties': ['name', 'price']},
        payload={'filter': filter_obj}
    ) 

    assert len(result_pages['results']) == 2




