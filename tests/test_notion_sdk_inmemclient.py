import pytest
import pdb

from normlite.notion_sdk.client import InMemoryNotionClient

def test_inmemclient_create_info_schema():
    client = InMemoryNotionClient()

    tables_id = 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'  # fake id for the database called 'tables' holding the information schema.
    tables_obj = client._get_by_title('tables', 'database')
    assert tables_obj.get('id') == tables_id

    tables_properties = tables_obj.get('properties')
    columns = [name for name in tables_properties.keys()]
    assert columns == ['table_name', 'table_schema', 'table_catalog', 'table_id']

def test_inmemclient_open_an_existing_database():
    client = InMemoryNotionClient()
    db_name = 'memory'


    # create the database students
    # parent is the memory page
    payload = {
        'parent': {
            'type': 'page_id',
            'page_id': memory_page.get('id')           # parent is the dedicated memory page
        },
        "title": [
            {
                "type": "text",
                "text": {
                    "content": "students",
                    "link": None
                },
                "plain_text": "students",
                "href": None
            }
        ],
        'properties': {
            'id': {'number': {}},
            'name': {'title': {}},
            'grade': {'rich_text': {}}
        }
    }
    students_db = client._add('database', payload)
    students_db_id = students_db.get('id')
    assert students_db_id

    # add the students table to tables
    payload = {
        'parent': {
            'type': 'database_id',
            'database_id': client._tables_db_id
        },
        'properties': {
            'table_name': {'title': [{'text': {'content': 'students'}}]},
            'table_schema': {'rich_text': [{'text': {'content': ''}}]},
            'table_catalog': {'rich_text': [{'text': {'content': db_name}}]},
            'table_id': {'rich_text': [{'text': {'content': students_db_id}}]},
        }
    }
    
    # lookup students database: Engine.has_table()
    tables_obj = client._get_by_title('tables', 'database')

    


