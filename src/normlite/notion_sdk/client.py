# notion_sdk/client.py 
# Copyright (C) 2025 Gianmarco Antonini
#
# This module is part of normlite and is released under the GNU Affero General Public License.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations
import pdb
from typing import Dict, Any, Optional, Set
from abc import ABC, abstractmethod
import uuid
from datetime import datetime

class NotionError(Exception):
    pass

class AbstractNotionClient(ABC):
    allowed_operations: Set[str] = set()

    def __init__(self, auth: str, ischema_page_id: Optional[str] = None):
        self._auth = auth
        self._ischema_page_id = ischema_page_id if ischema_page_id else str(uuid.uuid4())
        AbstractNotionClient.allowed_operations = {
            name
            for name in AbstractNotionClient.__abstractmethods__
        }

    def __call__(self, endpoint: str, request: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        method_name = f"{endpoint}_{request}"
        if method_name not in self.__class__.allowed_operations:
            raise NotionError(
                f"Unsupported operation: '{method_name}'. "
                f"Allowed: {sorted(self.__class__.allowed_operations)}"
            )
        method = getattr(self, method_name)
        return method(payload)

    @property
    def ischema_page_id(self) -> str:
        return self._ischema_page_id

    @abstractmethod
    def pages_create(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def pages_retrieve(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError
    
    @abstractmethod
    def databases_create(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError
    
    @abstractmethod
    def databases_retrieve(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Retrieve a database object for the provided ID

        Args:
            payload (Dict[str, Any]): A dictionary containing the database id as key.

        Returns:
            Dict[str, Any]: The retrieved database object or and empty dictionary if no
            databased object for the provided ID were found
        """
        raise NotImplementedError

class FakeNotionClient(AbstractNotionClient):
    def __init__(self, auth: str, ischema_page_id: str):
        super().__init__(auth, ischema_page_id)

        # Initialize the store
        self._store = {
            "store": [],
        }

        # Create the page 'information_schema'
        # Note: In the real Notion store, the 'parent' object is the workspace,
        # so the information schema page cannot be programmatically created via the API.
        # In the fake store the parent's page id is just random.
        payload = {
            'parent': {                     
                'type': 'page_id',
                'page_id': str(uuid.uuid4())
            },
            'properties': {
                'Name': {'title': [{'text': {'content': 'information_schema'}}]}
            }
        }
        ischema_page = self._add('page', payload)

        # Update the ischema page id with the one just created
        self._ischema_page_id = ischema_page['id']


    def _get(self, id: str) -> Dict[str, Any]:
        if self._store_len() > 0:
            for o in self._store['store']:
                if o['id'] == id:
                    return o
                
        return {}
    
    def _get_by_title(self, title: str, type: str) -> Dict[str, Any]:
        if self._store_len() > 0:
            for o in self._store['store']:
                if o['object'] == 'type':
                    object_title = o.get
                    return o
                
        return {}

    def _add(self, type: str, payload: Dict[str, Any]) -> Dict[str, Any]: 
        # check well-formedness of payload
        # Note: "properties" object existence is validated previously when binding parameters 
        if not payload.get('parent', None):
            # objects being added need to have the parent they belog to
            raise NotionError(f'Missing "parent" object in payload: {payload}')  
                    
        new_page = dict()
        new_page['object'] = type
        new_page['id'] = str(uuid.uuid4())
        current_date = datetime.now()
        new_page['created_id'] = current_date.isoformat()
        new_page['archived'] = False
        new_page['in_trash'] = False
        new_page.update(payload)
        if type == 'database':
            new_page['is_inline'] = False
            properties = new_page['properties']
            for prop_name, prop_obj in properties.items():
                prop_type = list(prop_obj.keys())
                properties[prop_name]['type'] = prop_type[0]

        self._store["store"].append(new_page)

        return new_page

    def _store_len(self) -> int:
        return len(self._store)

    def pages_create(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._add('page', payload)
    
    def pages_retrieve(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if self._store_len() > 0:
            retrieved_page = self._get(payload['id'])
            if retrieved_page['object'] == 'page':
                return retrieved_page
        
        return {}

    def databases_create(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._add('database', payload)
    
    def databases_retrieve(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            retrieved_object = self._get(payload['id'])
        except KeyError:
            raise NotionError('Bad payload provided, missing "database_id"')

        return retrieved_object
