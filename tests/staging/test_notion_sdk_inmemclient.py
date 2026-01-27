import pytest
import pdb

from normlite.notion_sdk.client import InMemoryNotionClient

def test_inmemclient_databases_query():
    client = InMemoryNotionClient(
        '12345678-0000-0000-1111-123456789012',                         # mock ws_id 
        'abababab-3333-3333-3333-abcdefghilmn',                         # mock ischema_page_id
        '66666666-6666-6666-6666-666666666666'                          # mock tables_id
    )

    # create the memory page for the in memory database
    payload = {
        'parent': {
            'type': 'page_id',
            'page_id': client._ws_id
        },
        "properties": {
            "Name": {
                "title": [
                    {
                        "text": {
                            "content": "memory"
                        }
                    }
                ]
            }   
        }
    }

    memory_page = client.pages_create(payload)

    # create the database students
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
            'student_id': {'number': {}},
            'name': {'title': {}},
            'grade': {'rich_text': {}}
        }
    }
    students_db = client.databases_create(payload)
    students_db_id = students_db.get('id')

    # add rows to the students table
    parent = {
        'type': 'database_id',
        'database_id': students_db_id
    }

    data = [
        {
            'student_id': {'number': 3141592}, 
            'name': {'title': [{'text': {'content': "Timmy O'Toole"}}]}, 
            'grade': {'rich_text': [{'text': {'content': 'C'}}]}
        },
        {
            'student_id': {'number': 6535897}, 
            'name': {'title': [{'text': {'content': "Andrey Conall"}}]}, 
            'grade': {'rich_text': [{'text': {'content': 'C'}}]}
        },
        {
            'student_id': {'number': 9323846}, 
            'name': {'title': [{'text': {'content': "Daniel Sarosh"}}]}, 
            'grade': {'rich_text': [{'text': {'content': 'A'}}]}
        },
        {
            'student_id': {'number': 2643383}, 
            'name': {'title': [{'text': {'content': "Odis Degado"}}]}, 
            'grade': {'rich_text': [{'text': {'content': 'C'}}]}
        },
        {
            'student_id': {'number': 2795028}, 
            'name': {'title': [{'text': {'content': "Veda Goettig"}}]}, 
            'grade': {'rich_text': [{'text': {'content': 'A'}}]}
        },
    ]

    for rec in data:
        page_payload = {'parent': {}, 'properties': {}}
        page_payload['parent'].update(parent)
        page_payload['properties'].update(rec)
        client.pages_create(page_payload)

    # query the students database
    result = client.databases_query({
        'database_id': students_db_id,
        'filter': {
            'and': [
                {
                    'property': 'student_id',
                    'number': {
                        'less_than': 6535897
                    }
                },
                {
                    'property': 'grade',
                    'rich_text': {
                        'equals': 'C'
                    }
                }
            ]
        }
    })
     
    assert len(result) == 2

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
    memory_page = {'id': 'fake_id'}

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

    


