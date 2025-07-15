import pytest
from normlite.notiondbapi.dbapi2 import Cursor, InterfaceError


def test_dbapi_cursor_fetchall(dbapi_cursor: Cursor):
    """Create a database if it does not exists"""

    rows = dbapi_cursor.fetchall()
    expected_rows = [
        [('id', 'number', '12345'),  ('grade', 'rich-text', 'B'), ('name', 'title', 'Isaac Newton')],
        [('id', 'number', '67890'),  ('grade', 'rich-text', 'A'), ('name', 'title', 'Galileo Galilei')]
    ]

    assert expected_rows == rows

def test_dbapi_cursor_execute(dbapi_cursor: Cursor):
    """Create a new page in the Notion database"""

    # The DBAPI implementation for Notion requires to specify the parent object as part of
    # the operation specification
    parent = dict(type = 'database_name', database_id = 'd9824bdc-8445-4327-be8b-5b47500af6ce')
    operation = dict(endpoint = 'pages', request = 'create')
    parameters = {
        'properties': {
            'id': {'number': 1},
            'name': {'title': [{'text': {'content': 'Isaac Newton'}}]},
            'grade': {'rich_text': [{'text': {'content': 'B'}}]}
        },
        'parent': parent
    }

    dbapi_cursor.execute(operation, parameters)
    assert dbapi_cursor.fetchall() == []
    assert dbapi_cursor.rowcount == 0

def test_dbapi_cursor_concat_calls(dbapi_cursor: Cursor):
    parent = dict(type = 'database_name', database_id = 'd9824bdc-8445-4327-be8b-5b47500af6ce')
    operation = dict(endpoint = 'pages', request = 'create')
    parameters = {
        'properties': {
            'id': {'number': 1},
            'name': {'title': [{'text': {'content': 'Isaac Newton'}}]},
            'grade': {'rich_text': [{'text': {'content': 'B'}}]}
        },
        'parent': parent
    }

    assert dbapi_cursor.execute(operation, parameters).fetchall() == []


def test_dbapi_cursor_no_parent(dbapi_cursor: Cursor):
    operation = dict(endpoint = 'pages', request = 'create')
    properties = {
        'id': {'number': 1},
        'name': {'title': [{'text': {'content': 'Isaac Newton'}}]},
        'grade': {'rich_text': [{'text': {'content': 'B'}}]}
    }
    parameters = {'properties': properties}

    with pytest.raises(
        InterfaceError, 
        match='"parent" object not specified in parameters for: pages.create.'):
        dbapi_cursor.execute(operation, parameters)

def test_dbapi_cursor_no_properties(dbapi_cursor: Cursor):
    operation = dict(endpoint = 'pages', request = 'create')
    parent = dict(type = 'database_name', database_id = 'd9824bdc-8445-4327-be8b-5b47500af6ce')
    parameters = {'parent': parent}

    with pytest.raises(
        InterfaceError, 
        match='"properties" object not specified in parameters for: pages.create.'):
        dbapi_cursor.execute(operation, parameters)
    
        

