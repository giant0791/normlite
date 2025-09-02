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
from dataclasses import dataclass
import json
import operator
from pathlib import Path
import pdb
from typing import List, Optional, Protocol, Self, Sequence, Set, Type
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

        self._tables_db_id = None

        AbstractNotionClient.allowed_operations = {
            name
            for name in AbstractNotionClient.__abstractmethods__
        }

    def __call__(self, endpoint: str, request: str, payload: dict) -> dict:
        """Enable function call style for REST Notion API client objects.

        Example::
        
            # Add a new Notion page to the database with id = 680dee41-b447-451d-9d36-c6eaff13fb46
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
    def pages_update(self, payload: dict) -> dict:
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
    
    @abstractmethod
    def databases_query(self, payload: dict) -> List[dict]:
        """Get a list pages contained in the database.

        Args:
            payload (dict): A dictionary that must contain a "database_id" key for the database to
                query and "filter" object to select.

        Returns:
            List[dict]: The list containing the page pbjects or ``[]``, if no pages have been found.
        """
        raise NotImplementedError
    
class InMemoryNotionClient(AbstractNotionClient):
    """Provide a simple but complete in-memory Notion client
    
    :class:`InMemoryNotionClient` fully implements the Notion API and mimics the Notion's store behavior.
    It automatically creates the database management datastructures.

    .. versionchanged:: 0.7.0 :class:`InMemoryNotionClient` automatically creates the `information_schema` page and the `tables` database.

    """
    def __init__(
            self, 
            ws_id: Optional[str] = None,
            ischema_page_id: Optional[str] = None,
            tables_db_id: Optional[str] = None
    ):
        super().__init__()
        if ws_id:
            self._ws_id = ws_id
        else:
            self._ws_id = '00000000-0000-0000-0000-000000000000'

        if ischema_page_id:
            self._ischema_page_id = ischema_page_id
        else:
            self._ischema_page_id = '66666666-6666-6666-6666-666666666666'

        if self._tables_db_id:
            self._tables_db_id = tables_db_id
        else: 
            self._tables_db_id = 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'

        self._store: dict = {
            "store": []
        }
        """The dictionary simulating the Notion store. 
        It's an instance attribute to avoid unwanted side effects and 
        provide more behavioral predictability.

        .. versionchanged:: 0.7.0 Fix https://github.com/giant0791/normlite/issues/45
            Fix issue with asymmetric file based Notion client.
        """

        self._create_store()

    def _create_store(self, store_content: List[dict] = []) -> None:
        """Provide helper to create the simulated Notion store.

        Args:
            store_content (List[dict], optional): The initial content for the Notion store. Defaults to ``[]``.
        """
        if store_content:
            self._store = {
                "store": store_content,
            }
        else:
            self._store = {
                "store": [],
            }

    def _get(self, id: str) -> dict:
        # TODO: rewrite using filter()
        if self._store_len() > 0:
            for o in self._store['store']:
                if o['id'] == id:
                    return o
                
        return {}
    
    def _get_by_title(self, title: str, type: str) -> dict:
        """Return the first occurrence in the store of page or database with the passed title."""
        # TODO: rewrite using filter()
        if self._store_len() > 0:
            for o in self._store['store']:
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

    def _add(self, type: str, payload: dict, id: Optional[str] = None) -> dict: 
        # check well-formedness of payload
        # Note: "properties" object existence is validated previously when binding parameters 
        if not payload.get('parent', None):
            # objects being added need to have the parent they belog to
            raise NotionError(f'Missing "parent" object in payload: {payload}')  
                    
        new_page = dict()
        new_page['object'] = type

        # use the provided id for behavioral predictability if available 
        new_page['id'] = id if id else str(uuid.uuid4())
        current_date = datetime.now()
        new_page['created_id'] = current_date.isoformat()
        new_page['archived'] = False
        new_page['in_trash'] = False
        new_page.update(payload)
        if type == 'database':
            new_page['is_inline'] = False

        # add the type key to each property for both pages and databases
        properties = new_page['properties']
        for prop_name, prop_obj in properties.items():
            prop_type = list(prop_obj.keys())
            properties[prop_name]['type'] = prop_type[0]

        self._store["store"].append(new_page)

        return new_page

    def _store_len(self) -> int:
        return len(self._store['store'])
    
    def pages_create(self, payload: dict) -> dict:
        return self._add('page', payload)
    
    def pages_retrieve(self, payload: dict) -> dict:
        if self._store_len() > 0:
            retrieved_page = self._get(payload['id'])
            if retrieved_page['object'] == 'page':
                return retrieved_page
        
        return {}
    
    def pages_update(self, payload)-> dict:
        if self._store_len() > 0:
            page_to_update = self._get(payload.get('id'))
            if page_to_update and page_to_update['object'] == 'page':
                data = payload.get('data')
                if data and 'archived' in data.keys():
                    page_to_update['archived'] = data['archived']
                    return page_to_update
                elif data and 'in_trash' in data.keys():
                    page_to_update['in_trash'] = data['in_trash']
                elif data and 'properties' in data.keys():
                    for prop, value in data['properties'].items():
                        page_to_update['properties'][prop] = value
                else:
                    raise NotionError(
                        f'Connot update page: {payload.get('id')}, '
                        f'data: {payload.get('data')}'
                    )
            else:
                raise NotionError(
                    f'Object with id: {payload.get('id')} not found or not a page object.'
                )


    def databases_create(self, payload: dict) -> dict:
        return self._add('database', payload)
    
    def databases_retrieve(self, payload: dict) -> dict:
        try:
            retrieved_object = self._get(payload['id'])
        except KeyError:
            raise NotionError('Bad payload provided, missing "database_id"')

        return retrieved_object
    
    def databases_query(self, payload: dict) -> List[dict]:
        query_result = []
        if self._store_len() > 0:        
            db_id = payload['database_id']
            for obj in self._store['store']:
                if obj['object'] == 'page' and obj['parent']['type'] == 'database_id':
                    # select only pages whose parent is a database
                    if obj['parent']['database_id'] == db_id:
                        # select only pages belonging to the db specified in the payload
                        filter = _Filter(obj, payload)
                        if filter.eval():
                            query_result.append(obj)

        return query_result

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

#--------------------------------------------------
# Private classes for implementing database queries
#--------------------------------------------------

class _Condition:
    _op_map: dict = {
        'equals': operator.eq,
        'greater_than': operator.gt,
        'less_than': operator.lt,
        'contains': operator.contains,
        'or': operator.or_,
        'and': operator.and_,
        'not': operator.not_
    }

    def __init__(self, page: dict, condition: dict):
        prop_name = condition['property']
        self.property_obj = page['properties'][prop_name]
        self.type_name, self.type_filter = self._extract_filter(condition)

    def _extract_filter(self, cond: dict) -> tuple[str, dict]:
        """Return (type_name, filter_dict) from a Notion condition."""
        return next((k, v) for k, v in cond.items() if k != "property")

    def eval(self) -> bool:
        #pdb.set_trace()
        op, val = next(iter(self.type_filter.items()))
        try:
            func = _Condition._op_map[op]
            if self.type_name in ['title', 'rich_text']:
                operand = self.property_obj[self.type_name][0]['text']['content']
            else:
                operand = self.property_obj[self.type_name]
            result = func(operand, val)
            return result
        except KeyError as ke:
            raise Exception(f'Operator: {ke.args[0]} not supported or unknown') 

class _CompositeCondition(_Condition):
    def __init__(self, logical_op: str, conditions: Sequence[_Condition]):
        self.logical_op = logical_op
        self.conditions = conditions

    def eval(self) -> bool:
        # IMPORTANT: You have to first eval all the conditions in an iterable!
        iterable_cond = [cond.eval() for cond in self.conditions]
        if self.logical_op == 'and':
            result = all(iterable_cond)
        elif self.logical_op == 'or':
            result = any(iterable_cond)
        else:
            raise Exception(f'Logical operator {self.logical_op} not supported or unknown')
        
        return result

class _Filter(_Condition):
    """Initial implementation, it **does not supported nested composite conditions**."""
    def __init__(self, page: dict, filter: dict):
        self.page = page
        self.filter = filter
        self.compiled: _Condition = None
        
    def _compile(self) -> None:
        filter_obj: dict = self.filter['filter']
        is_composite = filter_obj.get('and') or filter_obj.get('or')
        if is_composite:
            conditions = []
            logical_op, conds = next(iter(filter_obj.items()))
            conditions = [_Condition(self.page, cond) for cond in conds]
            self.compiled = _CompositeCondition(logical_op, conditions)
        else:
            self.compiled = _Condition(self.page, filter_obj)

    def eval(self) -> bool:
        if not self.compiled:
            self._compile()

        return self.compiled.eval()
