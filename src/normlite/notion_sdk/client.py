from typing import Dict, Any
import uuid

from normlite.notion_sdk import _fake_notion_store

def notion_pages_create(payload: Dict[str, Any]) -> Dict[str, Any]:
    new_page = dict()
    new_page['object'] = 'page'
    new_page['id'] = str(uuid.uuid4())
    new_page.update(payload)
    _fake_notion_store["store"].append(new_page)

    return new_page

