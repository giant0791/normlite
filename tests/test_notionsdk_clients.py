import pdb
import pytest

from normlite.notion_sdk.client import InMemoryNotionClient

@pytest.fixture
def fresh_client():
    yield InMemoryNotionClient()

def generate_store_content() -> list[dict]:
    store_content = list()
    store_content.append({
        "object": "page",
        "id": "680dee41-b447-451d-9d36-c6eaff13fb45",
        "archived": False,
        "in_trash": None,
        "properties": {
        "grade": {"type": "rich_text", "rich_text": [{"text": {"content": "B"}}]},
        "name": {"type": "title", "title": [{"text": {"content": "Isaac Newton"}}]},
        "id": {"type": "number", "number": 12345}
        }
    })

    store_content.append({
        "object": "page",
        "id": "680dee41-b447-451d-9d36-c6eaff13fb46",
        "archived": False,
        "in_trash": None,
        "properties": {
        "grade": {"type": "rich_text", "rich_text": [{"text": {"content": "A"}}]},
        "name": {"type": "title", "title": [{"text": {"content": "Galileo Galilei"}}]},
        "id": {"type": "number", "number": 67890}
        }
    })

    store_content.append({
        "object": "page",
        "id": "680dee41-b447-451d-9d36-c6eaff13fb47",
        "archived": False,
        "in_trash": None,
        "properties": {
            "grade": {"type": "rich_text", "rich_text": [{"text": {"content": "C"}}]},
            "name": {"type": "title", "title": [{"text": {"content": "Ada Lovelace"}}]},
            "id": {"type": "number", "number": 32165}
        }
    })

    return store_content

def get_name(obj: dict) -> str:
    return obj['properties']['name']['title'][0]['text']['content']

def get_id(obj: dict) -> int:
    return obj['properties']['id']['number']

def get_grade(obj: dict) -> str:
    pass

def get_db_prop_type(name: str, obj: dict) -> str:
    return obj['properties'][name].get('type', None)

def test_client_store_len(fresh_client: InMemoryNotionClient):
    assert fresh_client._store_len() == 0

def test_client_create_store(fresh_client: InMemoryNotionClient):
    content = generate_store_content() 
    fresh_client._create_store(content)
    assert fresh_client._store_len() == len(content)

def test_client_get_by_id(fresh_client: InMemoryNotionClient):
    assert not fresh_client._get_by_id('680dee41-b447-451d-9d36-c6eaff13fb47')
    content = generate_store_content() 
    fresh_client._create_store(content)
    newton = fresh_client._get_by_id('680dee41-b447-451d-9d36-c6eaff13fb45')
    galileo = fresh_client._get_by_id('680dee41-b447-451d-9d36-c6eaff13fb46')
    ada = fresh_client._get_by_id('680dee41-b447-451d-9d36-c6eaff13fb47')
    assert newton
    assert galileo
    assert ada
    assert get_name(newton) == 'Isaac Newton'
    assert get_id(newton) == 12345
    assert get_name(galileo) == 'Galileo Galilei'
    assert get_id(galileo) == 67890
    assert get_name(ada) == 'Ada Lovelace'
    assert get_id(ada) == 32165

def test_client_add_page(fresh_client: InMemoryNotionClient):
    page_payload = {
        "parent": {"type": "page_id"},
        "properties": {
            "grade": {"type": "rich_text", "rich_text": [{"text": {"content": "C"}}]},
            "name": {"type": "title", "title": [{"text": {"content": "Ada Lovelace"}}]},
            "id": {"type": "number", "number": 32165}
        }
    }

    page = fresh_client._add('page', page_payload)
    retrieved_page = fresh_client._get_by_id(page['id'])
    assert get_name(retrieved_page) == "Ada Lovelace"
    assert get_id(retrieved_page) == 32165

def test_client_add_database(fresh_client: InMemoryNotionClient):
    db_payload = {
        "parent": {"type": "page_id"},
        "properties": {
            "grade": {"rich_text": {}},
            "name": {"title": {}},
            "id": {"number": {}}
        },
        "title": [{"text": {"content": "students"}}]
    }

    database = fresh_client._add('database', db_payload)
    property_names = list(database['properties'].keys())
    property_types = [get_db_prop_type(prop_name, database) for prop_name in property_names]
    assert all(property_types)
    retrieved_db = fresh_client._get_by_id(database['id'])
    retrieved_db_prop_names = list(retrieved_db['properties'].keys())
    retrieved_db_prop_types = [get_db_prop_type(prop_name, retrieved_db) for prop_name in property_names]
    assert all(retrieved_db_prop_types)
    assert retrieved_db_prop_types == property_types
    #print(f'\n{database}')
