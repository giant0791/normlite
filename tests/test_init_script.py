import pdb
from typing import Any, List, Tuple
import uuid
import pytest
from normlite.notion_sdk.client import AbstractNotionClient
from normlite.notiondbapi.dbapi2 import Cursor

@pytest.fixture
def tables_payload(client: AbstractNotionClient) -> dict:
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

    return payload



@pytest.fixture
def tables_parameters(client: AbstractNotionClient)-> dict:

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

    return {
        'payload': payload,
        'params': {}                # no param bindings required for databases.create
    }

def test_init_create_tables_w_notion_sdk(client: AbstractNotionClient, tables_payload: dict):
    tables_object = client.databases_create(tables_payload)

    assert tables_object['object'] == 'database'
    assert tables_object['title'][0]['text']['content'] == 'tables'


from collections import namedtuple
PropertyMetadata = namedtuple('PropertyMetadata', ['name', 'type', 'value'])
"""Alternative approach for the CursorResult class to access column metadata.
"""

def get_property_data(row: List[Tuple[str, str, Any]]) -> List[PropertyMetadata]:
    return [PropertyMetadata(*column_desc) for column_desc in row]

def test_init_create_tables_w_dbapi(dbapi_cursor: Cursor, tables_parameters: dict):
    cursor = dbapi_cursor.execute(
        dict(endpoint='databases', request='create'),
        parameters=tables_parameters
    )
 
    results = cursor.fetchall()
    assert cursor.rowcount == 1

    created_db = results[0]
    created_db_id = created_db[0][-1]
    assert cursor.lastrowid == uuid.UUID(created_db_id).int
    
    _ = cursor.execute(
        dict(endpoint='databases', request='retrieve'), 
        {'payload': dict(id=created_db_id), 'params': {}}
    )
    
    results = cursor.fetchall()
    assert cursor.rowcount == 1
    retrieved_db = results[0]
    retrieved_db_id = retrieved_db[0][-1]

    assert retrieved_db_id == created_db_id
    assert retrieved_db == created_db
 
