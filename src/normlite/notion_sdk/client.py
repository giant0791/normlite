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

"""Provide several client classes to the Notion API.

This module provides high level client classes to abstract away the details
of the Notion REST API.
Two classes are best suited for testing: :class:`InMemoryNotionClient` which holds in memory the
Notion data like pages and databases, and :class:`FileBasedNotionClient` which adds the capability
to store the Notion data as a JSON file on the file system.
"""

from __future__ import annotations
import json
from pathlib import Path
from typing import List, Optional, Self, Set, Type
from types import TracebackType
from abc import ABC, abstractmethod
import uuid
from datetime import datetime

class NotionError(Exception):
    """Exception raised for all errors related to the Notion REST API."""
    pass

class AbstractNotionClient(ABC):
    """Base class for a Notion API client.

    """
    allowed_operations: Set[str] = set()
    """The set of Notion API calls."""

    def __init__(self):
        self._ischema_page_id = None
        """The object id for ``information_schema`` page."""

        AbstractNotionClient.allowed_operations = {
            name
            for name in AbstractNotionClient.__abstractmethods__
        }

    def __call__(self, endpoint: str, request: str, payload: dict) -> dict:
        """Enable function call style for REST Notion API client objects.

        Example::
        
            # create add a new Notion page to the database with id = 680dee41-b447-451d-9d36-c6eaff13fb46
            operation = {"endpoint": "pages", "request": "create"}
            payload = {
                'parent': {
                    "type": "database_id",
                    "page_id": "680dee41-b447-451d-9d36-c6eaff13fb46"
                },
                'properties': {
                    'Name': {'title': [{'text': {'content': title}}]}
                }
            }
            client = InMemoryNotionClient()
            try:
                object_ = client(
                    operation['endpoint'],
                    operation['request'],
                    payload
                )
            except KeyError as ke:
                raise NotionError(f"Missing required key in operation dict: {ke.args[0]}")

        Args:
            endpoint (str): The REST API endpoint, example: ``databases``. 
            request (str): The REST API request, example: ``create``.
            payload (dict): The JSON object as payload.

        Raises:
            NotionError: Unknown or unsupported operation. 

        Returns:
            dict: The JSON object returned by the NOTION API.
        """
        method_name = f"{endpoint}_{request}"
        if method_name not in self.__class__.allowed_operations:
            raise NotionError(
                f"Unknown or unsupported operation: '{method_name}'. "
                f"Allowed: {sorted(self.__class__.allowed_operations)}"
            )
        method = getattr(self, method_name)
        return method(payload)

    @property
    def ischema_page_id(self) -> Optional[str]:
        return self._ischema_page_id 

    @abstractmethod
    def pages_create(self, payload: dict) -> dict:
        """Create a page object.

        This method creates a new page that is a child of an existing page or database.

        Args:
            payload (dict): The JSON object containing the required payload as specified by the Notion API.

        Returns:
            dict: The page object.
        """
        raise NotImplementedError

    @abstractmethod
    def pages_retrieve(self, payload: dict) -> dict:
        """Retrieve a page object.

        This method is used as follows::

            # retrieve page with id = "680dee41-b447-451d-9d36-c6eaff13fb46"
            operation = {"endpoint": "pages", "request": "create"}
            payload = {"id": "680dee41-b447-451d-9d36-c6eaff13fb46"}
            client = InMemoryNotionClient()
            try:
                object_ = client(
                    operation['endpoint'],
                    operation['request'],
                    payload
                )
            except KeyError as ke:
                raise NotionError(f"Missing required key in operation dict: {ke.args[0]}")

        Args:
            payload (dict): The JSON object containing the id to be retrieved.

        Returns:
            dict: The page object containing the page properties only, not page content.
        """
        raise NotImplementedError
    
    @abstractmethod
    def databases_create(self, payload: dict) -> dict:
        """Create a database as a subpage in the specified parent page, with the specified properties schema.

        Args:
            payload (dict): The JSON object containing the required payload as specified by the Notion API.

        Returns:
            dict: The created database object.
        """
        raise NotImplementedError
    
    @abstractmethod
    def databases_retrieve(self, payload: dict) -> dict:
        """Retrieve a database object for the provided ID

        Args:
            payload (dict): A dictionary containing the database id as key.

        Returns:
            dict: The retrieved database object or and empty dictionary if no
            databased object for the provided ID were found
        """
        raise NotImplementedError
    
class InMemoryNotionClient(AbstractNotionClient):
    _store: dict = {
        "store": []
    }
    """The dictionary simulating the Notion store. It's a class attribute, so all instances share the same store."""

    def _create_store(self, store_content: List[dict] = []) -> None:
        """Provide helper to create the simulated Notion store.

        Args:
            store_content (List[dict], optional): The initial content for the Notion store. Defaults to ``[]``.
        """
        if store_content:
            InMemoryNotionClient._store = {
                "store": store_content,
            }
        else:
            InMemoryNotionClient._store = {
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

    def _get(self, id: str) -> dict:
        # TODO: rewrite using filter()
        if self._store_len() > 0:
            for o in InMemoryNotionClient._store['store']:
                if o['id'] == id:
                    return o
                
        return {}
    
    def _get_by_title(self, title: str, type: str) -> dict:
        """Return the first occurrence in the store of page or database with the passed title."""
        # TODO: rewrite using filter()
        if self._store_len() > 0:
            for o in InMemoryNotionClient._store['store']:
                if o['object'] == type and type == 'database':
                    object_title = o.get('title')
                    if object_title and object_title[0]['text']['content'] == title:
                        return o
                elif o['object'] == type and type == 'page':
                    properties = o.get('properties')
                    for pv in properties.values():
                        prop_title = pv.get('title')
                        if prop_title and prop_title[0]['text']['content'] == title:
                            return o
        return {}

    def _add(self, type: str, payload: dict) -> dict: 
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

        InMemoryNotionClient._store["store"].append(new_page)

        return new_page

    def _store_len(self) -> int:
        return len(InMemoryNotionClient._store['store'])
    
    def pages_create(self, payload: dict) -> dict:
        return self._add('page', payload)
    
    def pages_retrieve(self, payload: dict) -> dict:
        if self._store_len() > 0:
            retrieved_page = self._get(payload['id'])
            if retrieved_page['object'] == 'page':
                return retrieved_page
        
        return {}

    def databases_create(self, payload: dict) -> dict:
        return self._add('database', payload)
    
    def databases_retrieve(self, payload: dict) -> dict:
        try:
            retrieved_object = self._get(payload['id'])
        except KeyError:
            raise NotionError('Bad payload provided, missing "database_id"')

        return retrieved_object

class FileBasedNotionClient(InMemoryNotionClient):
    """Enhance the in-memory client with file based persistence.

    This class extends the base :class:`InMemoryNotionClient` by providing the capability
    to store and load the simulated Notion store content to and from the underlying file.
    In addition, this class implements the context manager protocol allowing the following usage::

        # persistently add new pages to my-database.json
        client = FileBasedNotionClient("my-database.json")
        with client as c:
            c.pages_create(payload1)   # payload* are previously created JSON Notion objects to be added
            c.pages_create(payload2)
            c.pages_create(payload3)
    """
    def __init__(self, file_path: str):
        super().__init__()

        self.file_path = file_path
        """The absolute path to the file storing the data contained in the file-base Notion client."""

    def load(self) -> List[dict]:
        """Load the store content from the underlying file.

        Returns:
            List[dict]: The JSON object as list of dictionaries containing the store.
        """
        with open(self.file_path, 'r') as file:
            return json.load(file)
    
    def __enter__(self) -> Self:
        """Initialize the Notion store in memory.

        When the context manager is entered, the Notion store is read in memory, if the corresponding
        file existes. Otherwise, the store in memory is initialized with an empty list.

        Returns:
            Self: This instance as required by the context manager protocol.
        """
        if Path(self.file_path).exists():
            # The file containing the Notion store exists, read it into the _store class attribute
                store = self.load()
                self._create_store(store['store'])  # pass the list of objects as store content 
                return self
            
        else:
            # No file exists, initialize the _store class attribute
            self._create_store()
            return self
        
    def dump(self, store_content: List[dict]) -> None:
        """Dump the store content onto the underlying file.

        Args:
            store_content (List[dict]): The current store content present in memory.
        """

        with open(self.file_path, 'w') as file:
            json.dump(store_content, file, indent=2)

    def __exit__(
        self,
        exctype: Optional[Type[BaseException]] = None,
        excinst: Optional[BaseException] = None,
        exctb: Optional[TracebackType] = None,
    ) -> Optional[bool]:
        """Dump the Notion stored to the file.

        Args:
            exctype (Optional[Type[BaseException]]): The exception class. Defaults to ``None``.
            excinst (Optional[BaseException]): The exception instance. Defaults to ``None``.
            exctb (Optional[TracebackType]): The traceback object. Defaults to ``None``.

        Returns:
            Optional[bool]: ``None`` as it is customary for context managers.
        """

        self.dump(FileBasedNotionClient._store)

