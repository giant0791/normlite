import copy
import pdb
from typing import Any, Dict
import pytest
from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.notiondbapi.dbapi2 import Cursor, InterfaceError

@pytest.fixture
def new_pg_payload() -> Dict[str,Any]:
    """Proivde a well-formed parameters fixture for the `Cursor.execute()` method"""
    # The DBAPI implementation for Notion requires to specify the parent object as part of
    # the operation specification
    parent = dict(type = 'database_id', database_id = 'd9824bdc-8445-4327-be8b-5b47500af6ce')
    payload = {
        'properties': {
            'id': {'number': 98765},
            'name': {'title': [{'text': {'content': 'Ada Lovelace'}}]},
            'grade': {'rich_text': [{'text': {'content': 'A'}}]}
        },
        'parent': parent
    }
    
    return payload

@pytest.fixture
def new_db_payload() -> dict:
    # IMPORTANT
    # use the client's root page id otherwise every database creat operation will fail with:
    # Could not find page with ID: ...
    parent = dict(type = 'page_id', page_id = InMemoryNotionClient._ROOT_PAGE_ID_)
    payload = {
        'title': [
        {
            'type': 'text',
            'text': {
                'content': 'students',
                "link": None
            }
        }],
        'properties': {
            'id': {'number': {}},
            'name': {'title': {}},
            'grade': {'rich_text': {}}
        },
        'parent': parent
    }
    return payload

def update_payload(payload: dict, key: str, value: dict) -> dict:
    new_payload = copy.deepcopy(payload)
    new_payload[key] = value
    return new_payload

def test_dbapi_cursor_execute_create_databases(dbapi_cursor: Cursor, new_db_payload: dict):
    operation = dict(endpoint = 'databases', request = 'create', payload=new_db_payload)
    dbapi_cursor.execute(operation)
    assert dbapi_cursor.rowcount == 1
    row = dbapi_cursor.fetchone()      
    
    # IMPORTANT: unwrap because the row elements are 3-value tuples
    _, _, database_id = row[0]
    _, _, database_name = row[1]

    operation = dict(endpoint='databases', request='retrieve', payload={'database_id': database_id})
    dbapi_cursor.execute(operation)
    assert dbapi_cursor.rowcount == 1
    row = dbapi_cursor.fetchone()
    _, _, ret_database_id = row[0]
    _, _, ret_database_name = row[1]
    assert ret_database_id == database_id
    assert ret_database_name == database_name 
    

def test_dbapi_cursor_execute_create_pages(dbapi_cursor: Cursor, new_db_payload: dict, new_pg_payload: dict):
    operation = dict(endpoint = 'databases', request = 'create', payload=new_db_payload)
    dbapi_cursor.execute(operation)
    assert dbapi_cursor.rowcount == 1
    row = dbapi_cursor.fetchone()      
    _, _, database_id = row[0]

    parent = dict(type='database_id', database_id=database_id)
    updated_payload = update_payload(new_pg_payload, 'parent', parent)
    operation = dict(endpoint = 'pages', request = 'create', payload=updated_payload)
    dbapi_cursor.execute(operation)
    rows = dbapi_cursor.fetchall()
    assert len(rows) == 1

def test_dbapi_cursor_execute_update_pages(dbapi_cursor: Cursor, new_db_payload: dict, new_pg_payload: dict):
    # 1. create the database
    operation = dict(endpoint = 'databases', request = 'create', payload=new_db_payload)
    dbapi_cursor.execute(operation)
    assert dbapi_cursor.rowcount == 1
    row = dbapi_cursor.fetchone()      
    _, _, database_id = row[0]

    # 2. add a page
    parent = dict(type='database_id', database_id=database_id)
    updated_payload = update_payload(new_pg_payload, 'parent', parent)
    operation = dict(endpoint = 'pages', request = 'create', payload=updated_payload)
    row = dbapi_cursor.execute(operation).fetchone()

    # IMPORTANT: unwrap because the row elements are 2-value tuples for pages
    _, page_id = row[0]
    _, archived = row[1]

    # 3. modify page attribute archived to True
    payload = dict(page_id=page_id, archived=True)
    operation = dict(endpoint='pages', request='update', payload=payload)
    dbapi_cursor.execute(operation)
    row = dbapi_cursor.fetchone()
    _, new_archived = row[1]
    assert archived != new_archived

def test_dbapi_cursor_no_properties(dbapi_cursor: Cursor, new_db_payload: dict):
    payload_wo_properties = copy.deepcopy(new_db_payload)
    payload_wo_properties.pop('properties')
    operation = dict(endpoint = 'databases', request = 'create', payload=payload_wo_properties)
    with pytest.raises(
        InterfaceError, 
        match='Body failed validation: body.properties should be defined,'):
        dbapi_cursor.execute(operation)



