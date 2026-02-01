import copy
import pdb
import uuid
import pytest

from normlite.notion_sdk.client import InMemoryNotionClient, NotionError

_STUDENTS_ID_ = "11111111-1111-1111-1111-111111111111"

@pytest.fixture
def fresh_client():
    yield InMemoryNotionClient()

@pytest.fixture
def payload_template() -> dict:
    return {
        "parent": {},
        "properties": {}
    }

@pytest.fixture
def page_payload(payload_template: dict) -> dict:
    payload = copy.deepcopy(payload_template)
    payload['parent'] = {"type": "database_id", "database_id": _STUDENTS_ID_}
    payload['properties'] = {
        "grade": {"type": "rich_text", "rich_text": [{"text": {"content": "C"}}]},
        "name": {"type": "title", "title": [{"text": {"content": "Ada Lovelace"}}]},
        "id": {"type": "number", "number": 32165}
    }

    return payload

@pytest.fixture
def page_payloads() -> list[dict]:
    payloads = list()
    parent = {'type': 'database_id', 'database_id': _STUDENTS_ID_}
    payloads.append({
        'parent': parent,
        "properties": {
            "grade": {"type": "rich_text", "rich_text": [{"text": {"content": "B"}}]},
            "name": {"type": "title", "title": [{"text": {"content": "Isaac Newton"}}]},
            "id": {"type": "number", "number": 12345}
        }
    })

    payloads.append({
        'parent': parent,
        "properties": {
            "grade": {"type": "rich_text", "rich_text": [{"text": {"content": "A"}}]},
            "name": {"type": "title", "title": [{"text": {"content": "Galileo Galilei"}}]},
            "id": {"type": "number", "number": 67890}
        }
    })

    payloads.append({
        'parent': parent,
        "properties": {
            "grade": {"type": "rich_text", "rich_text": [{"text": {"content": "C"}}]},
            "name": {"type": "title", "title": [{"text": {"content": "Ada Lovelace"}}]},
            "id": {"type": "number", "number": 32165}
        }
    })

    return payloads

@pytest.fixture
def database_payload(payload_template: dict) -> dict:
    payload = copy.deepcopy(payload_template)
    payload['parent'] = {"type": "page_id", "page_id": InMemoryNotionClient._ROOT_PAGE_ID_}
    payload['title'] = {"title": [{"text": {"content": "students", "link": None}}]}
    payload['properties'] = {
        "name": {"title": {}},
        "grade": {"rich_text": {}},
        "id": {"number": {}}
    }

    return payload

@pytest.fixture
def client(fresh_client: InMemoryNotionClient, database_payload: dict) -> InMemoryNotionClient:
    # pre-fill database for testing endpoint pages, which relies on availability of the parent database
    # IMPORTANT: You must add the parent page for the database first
    fresh_client._add('database', database_payload, _STUDENTS_ID_)
    return fresh_client

@pytest.fixture
def for_page_queries(client: InMemoryNotionClient, page_payloads: list[dict]) -> InMemoryNotionClient:
    ids = [
        '680dee41-b447-451d-9d36-c6eaff13fb45',
        '680dee41-b447-451d-9d36-c6eaff13fb46',
        '680dee41-b447-451d-9d36-c6eaff13fb47'
    ]
    for id_, payload in enumerate(page_payloads):
        _ = client._add('page', payload, ids[id_])

    return client

def test_client_store_correctly_initialized(fresh_client: InMemoryNotionClient, paccessor):
    root_page_id = InMemoryNotionClient._ROOT_PAGE_ID_
    
    assert fresh_client._store_len() == 1
    assert paccessor.get_page_title(fresh_client._get_by_id(root_page_id)) == InMemoryNotionClient._ROOT_PAGE_TITLE_

def test_client_get_by_id(for_page_queries: InMemoryNotionClient, paccessor):
    newton = for_page_queries._get_by_id('680dee41-b447-451d-9d36-c6eaff13fb45')
    galileo = for_page_queries._get_by_id('680dee41-b447-451d-9d36-c6eaff13fb46')
    ada = for_page_queries._get_by_id('680dee41-b447-451d-9d36-c6eaff13fb47')
    assert newton
    assert galileo
    assert ada
    assert paccessor.get_text_property_value('name', 'title', newton) == 'Isaac Newton'
    assert paccessor.get_number_property_value('id', newton) == 12345
    assert paccessor.get_text_property_value('name', 'title', galileo) == 'Galileo Galilei'
    assert paccessor.get_number_property_value('id', galileo) == 67890
    assert paccessor.get_text_property_value('name', 'title', ada) == 'Ada Lovelace'
    assert paccessor.get_number_property_value('id', ada) == 32165

def test_client_add_page(fresh_client: InMemoryNotionClient, database_payload: dict, page_payload: dict, paccessor):
    fresh_client._add('database', database_payload, page_payload['parent']['database_id'])
    page = fresh_client._add('page', page_payload)
    retrieved_page = fresh_client._get_by_id(page['id'])
    assert paccessor.get_text_property_value('name', 'title', retrieved_page) == "Ada Lovelace"
    assert paccessor.get_number_property_value('id', retrieved_page) == 32165

def test_client_add_database(fresh_client: InMemoryNotionClient, database_payload: dict, paccessor):
    database = fresh_client._add('database', database_payload)
    property_names = list(database['properties'].keys())
    property_types = [paccessor.get_db_prop_type(prop_name, database) for prop_name in property_names]
    assert all(property_types)
    retrieved_db = fresh_client._get_by_id(database['id'])
    retrieved_db_prop_types = [paccessor.get_db_prop_type(prop_name, retrieved_db) for prop_name in property_names]
    assert all(retrieved_db_prop_types)
    assert retrieved_db_prop_types == property_types

# =================================================================
# Endpoint: pages, happy path
# =================================================================

def test_client_pages_create(client: InMemoryNotionClient, page_payload: dict):
    page_created = client.pages_create(page_payload)

    # the store contains the database (pre-filled in) and the new page
    assert client._store_len() == 2 + 1            
    assert page_created['id'] == client._store[page_created['id']].get('id')

def test_client_pages_retrieve(client: InMemoryNotionClient, page_payload: dict):
    page_created = client.pages_create(page_payload)
    page_retrieved = client.pages_retrieve({'page_id': page_created.get('id')})

    # the store contains the database (pre-filled in) and the newly created page
    assert client._store_len() == 2 + 1
    assert page_retrieved == client._store[page_created['id']]

def test_client_pages_update(client: InMemoryNotionClient, page_payload: dict):
    page_created = client.pages_create(page_payload)
    page_id = page_created.get('id')
    _ = client.pages_update({
        'page_id': page_id,
        'archived': True,
        'in_trash': True,
        'properties': {
            'grade': {'rich_text': [{'text': {'content': 'A'}}]}
        }
    })
    page_retrieved = client.pages_retrieve({'page_id': page_created.get('id')})
    assert page_retrieved == page_retrieved

# =================================================================
# Endpoint: pages, error cases
# =================================================================
def test_client_pages_create_no_id_in_body(client: InMemoryNotionClient, page_payload: dict):
    bad_payload = copy.deepcopy(page_payload)
    bad_payload['parent'].pop('database_id')

    with pytest.raises(NotionError, match='body.parent.database_id should be defined'):
        _ = client.pages_create(bad_payload) 

def test_client_pages_create_could_not_find_database(fresh_client: InMemoryNotionClient, page_payload: dict):
    not_found_id = page_payload['parent']['database_id']
    error_message = f'Could not find database with ID: {not_found_id}'
    with pytest.raises(NotionError, match=error_message):
        _ = fresh_client.pages_create(page_payload) 

def test_client_pages_retrieve_invalid_url(client: InMemoryNotionClient):
    with pytest.raises(NotionError, match='Invalid request URL'):
        _ = client.pages_retrieve({})

def test_client_pages_retrieve_could_not_find_page(client: InMemoryNotionClient, page_payload: dict):
    _ = client.pages_create(page_payload)
    bad_id = str(uuid.uuid4())
    with pytest.raises(NotionError, match=f'Could not find page with ID: {bad_id}'):
        _ = client.pages_retrieve({'page_id': bad_id})

def test_client_pages_update_invalid_url(client: InMemoryNotionClient, page_payload: dict):
    page_created = client.pages_create(page_payload)
    _ = page_created.get('id')
    with pytest.raises(NotionError, match='Invalid request URL'):
        _ = client.pages_update({
        'archived': True,
        'in_trash': True,
        'properties': {
            'grade': {'rich_text': [{'text': {'content': 'A'}}]}
        }
    })

def test_client_pages_update_validation_error(client: InMemoryNotionClient, page_payload: dict):
    page_created = client.pages_create(page_payload)
    page_id = page_created.get('id')
    with pytest.raises(NotionError, match='Body failed validation:'):
        _ = client.pages_update({
            'page_id': page_id,
        })
        
# =================================================================
# Endpoint: databases, happy path
# =================================================================
def test_client_databases_create(fresh_client: InMemoryNotionClient, database_payload: dict):
    database_created = fresh_client.databases_create(database_payload)
    prop_objects = database_created['properties'].values()
    generated_prop_ids = [prop_id for prop_id in prop_objects]
    
    assert fresh_client._store_len() == 1 + 1
    assert database_created['id'] == list(fresh_client._store.keys())[-1]
    assert all(generated_prop_ids)

def test_client_databases_retrieve(client: InMemoryNotionClient, database_payload: dict):
    database_created = client.databases_create(database_payload)
    database_retrieved = client.databases_retrieve({'database_id': database_created.get('id')})
    
    # the store contains the database (pre-filled in) and the newly created page
    assert client._store_len() == 2 + 1
    assert database_retrieved == client._store[database_created['id']]

def test_client_databases_query(for_page_queries: InMemoryNotionClient, paccessor):
    assert for_page_queries._store_len() == 5
    results = for_page_queries.databases_query({
        'database_id': _STUDENTS_ID_,
        'filter': {
            'and': [
                {
                    'property': 'id',
                    'number': {
                        'greater_than': 12000
                    }
                },
                {
                    'property': 'name',
                    'title': {
                        'does_not_contain': 'Ada'
                    }

                }
            ]
            
        }
    })
    assert results['object'] == 'list'
    assert len(results['results']) == 2
    assert paccessor.get_text_property_value('name', 'title', results['results'][0]) == 'Isaac Newton'
    assert paccessor.get_text_property_value('name', 'title',   results['results'][1]) == 'Galileo Galilei'

# =================================================================
# Endpoint: databases, error cases
# =================================================================
def test_client_databases_create_no_id_in_body(client: InMemoryNotionClient, database_payload: dict):
    bad_payload = copy.deepcopy(database_payload)
    bad_payload['parent'].pop('page_id')

    with pytest.raises(NotionError, match='body.parent.page_id should be defined'):
        _ = client.databases_create(bad_payload) 

def test_client_databases_create_could_not_find_database(fresh_client: InMemoryNotionClient, page_payload: dict):
    not_found_id = page_payload['parent']['database_id']
    error_message = f'Could not find database with ID: {not_found_id}'
    with pytest.raises(NotionError, match=error_message):
        _ = fresh_client.pages_create(page_payload) 

def test_client_databases_retrieve_invalid_url(client: InMemoryNotionClient):
    with pytest.raises(NotionError, match='Invalid request URL'):
        _ = client.databases_retrieve({})

def test_client_databases_retrieve_could_not_find_page(client: InMemoryNotionClient, database_payload: dict):
    _ = client.databases_create(database_payload)
    bad_id = str(uuid.uuid4())
    with pytest.raises(NotionError, match=f'Could not find database with ID: {bad_id}'):
        _ = client.databases_retrieve({'database_id': bad_id})


