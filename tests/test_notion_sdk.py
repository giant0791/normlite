from typing import Any, Dict
from datetime import datetime
import uuid
from normlite.notion_sdk.client import AbstractNotionClient, FakeNotionClient

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


def test_pages_create(client: AbstractNotionClient):
    page_object = {
        'parent': {
            'type': 'database_id',
            'database_id': 'd9824bdc-8445-4327-be8b-5b47500af6ce'
        },
        'properties': {
            'Name': {
                'title': [
                    {
                        'text': {'content': 'Tuscan kale'}
                    }
                ]
            },
            'Description': {
                'rich_text': [
                    {
                        'text': {'content': 'A dark green leafy vegetable'}
                    }
                ]
            }
        }
    }
    new_page: Dict[str, Any] = client.pages_create(page_object)
    retrieved_page: Dict[str, Any] = client.pages_retrieve(new_page['id'])

    assert retrieved_page is not {}
    assert retrieved_page == new_page