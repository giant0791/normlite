import copy
import pdb
from typing import Any, Dict
import uuid
import pytest
from normlite.notiondbapi.dbapi2 import Cursor, InterfaceError

@pytest.fixture
def parameters() -> Dict[str,Any]:
    """Proivde a well-formed parameters fixture for the `Cursor.execute()` method"""
    # The DBAPI implementation for Notion requires to specify the parent object as part of
    # the operation specification
    parent = dict(type = 'database_name', database_id = 'd9824bdc-8445-4327-be8b-5b47500af6ce')
    payload = {
        'properties': {
            'id': {'number': ':id'},
            'name': {'title': [{'text': {'content': ':name'}}]},
            'grade': {'rich_text': [{'text': {'content': ':grade'}}]}
        },
        'parent': parent
    }
    params = {                                 # params contains the bindings
        'id': 98765,
        'name': 'Ada Lovelace',
        'grade': 'A'
    }
    
    return dict(payload=payload, params=params)



def test_dbapi_cursor_execute(dbapi_cursor: Cursor, parameters: dict):
    operation = dict(endpoint = 'pages', request = 'create')
    dbapi_cursor.execute(operation, parameters)
    dbapi_cursor.fetchall()

    assert dbapi_cursor.rowcount == 1

def test_dbapi_cursor_concat_calls(dbapi_cursor: Cursor, parameters: dict):
    operation = dict(endpoint = 'pages', request = 'create')
    results = dbapi_cursor.execute(operation, parameters).fetchall()
    
    assert len(results) == 1

def test_dbapi_cursor_no_properties(dbapi_cursor: Cursor, parameters: Dict[str, Any]):
    operation = dict(endpoint = 'pages', request = 'create')
    parameters['payload'].pop('properties')
    with pytest.raises(
        InterfaceError, 
        match='Missing "properties" object in payload:'):
        dbapi_cursor.execute(operation, parameters)

def test_cursor_bind_params(dbapi_cursor: Cursor, parameters: Dict[str, Any]):
    expected_bound_payload = copy.deepcopy(parameters['payload'])
    expected_bound_payload['properties']['id'] = dict(number=1)
    expected_bound_payload['properties']['name'] = {'title': [{'text': {'content': 'Isaac Newton'}}]}
    expected_bound_payload['properties']['grade'] = {'rich_text': [{'text': {'content': 'B'}}]}

    bound_payload = dbapi_cursor._bind_parameters(parameters)
    assert expected_bound_payload == bound_payload
    
def test_cursor_execute_w_param_binding(dbapi_cursor: Cursor, parameters: Dict[str, Any]):
    title = [dict(text=dict(content='students'))]
    parameters['payload'].update({"title": title})
    operation = dict(endpoint = 'pages', request = 'create')

    dbapi_cursor.execute(operation, parameters)
    new_row = dbapi_cursor.fetchall()
    assert len (new_row) == 1
    assert dbapi_cursor.rowcount == 1

    obj_id = new_row[0][0][-1]  # object id is always first element, id value always last 
    assert dbapi_cursor.lastrowid == uuid.UUID(obj_id).int

