from __future__ import annotations
from typing import Dict, Any
from abc import ABC, abstractmethod
import uuid
from datetime import datetime

from normlite.notion_sdk import _fake_notion_store

class AbstractNotionClient(ABC):
    def __init__(self, auth: str):
        self._auth = auth

    @abstractmethod
    def pages_create(payload: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def pages_retrieve(self, id: str) -> Dict[str, Any]:
        raise NotImplementedError
    
    @abstractmethod
    def databases_create(payload: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

class FakeNotionClient(AbstractNotionClient):
    def __init__(self, auth: str):
        super().__init__(auth)

    def pages_create(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        new_page = dict()
        new_page['object'] = 'page'
        new_page['id'] = str(uuid.uuid4())
        current_date = datetime.now()
        new_page['created_id'] = current_date.isoformat()
        new_page['archived'] = False
        new_page['in_trash'] = False
        new_page.update(payload)
        _fake_notion_store["store"].append(new_page)

        return new_page
    
    def pages_retrieve(self, id: str) -> Dict[str, Any]:
        retrieved_page = dict()

        for obj in _fake_notion_store['store']:
            object_type = obj.get('object')
            object_id = obj.get('id')
            if object_type == 'page' and object_id == id:
                retrieved_page = obj
                break
        
        return retrieved_page

    def databases_create(payload: Dict[str, Any]) -> Dict[str, Any]:
        pass
