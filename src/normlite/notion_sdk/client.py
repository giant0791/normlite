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
import copy
from dataclasses import dataclass
import json
from pathlib import Path
import pdb
from typing import Any, List, NoReturn, Optional, Self, Set, Type
from types import TracebackType
from abc import ABC, abstractmethod
import uuid
from datetime import datetime
import random
import string
import urllib.parse
import warnings

from normlite.notion_sdk.types import normalize_filter_date, normalize_page_date

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
        """The object id for ``information_schema`` page.
        
        .. deprecated:: 0.7.0
            Do not use, it will be removed in a future version.
            Use the keyword arguments of the :class:`normlite.engine.base.Engine`.
            
        """

        self._tables_db_id = None
        """The object id for ``tables`` database.
        
        .. deprecated:: 0.7.0
            Do not use, it will be removed in a future version.
            Use the keyword arguments of the :class:`normlite.engine.base.Engine`.
            
        """

        AbstractNotionClient.allowed_operations = {
            name
            for name in AbstractNotionClient.__abstractmethods__
        }

    def __call__(
            self, 
            endpoint: str, 
            request: str,
            path_params: Optional[dict] = None,
            query_params: Optional[dict] = None, 
            payload: Optional[dict] = None
    ) -> dict:
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
            path_params (dict): Optional REST API path parameters, example: ``{"page_id": "b55c9c91-384d-452b-81db-d1ef79372b75"}
            query_params (dict): The REST API query parameters, example: ``{"filter_properties": ["title", "status"]}
            payload (dict): The JSON object as payload (also called body of the request).

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
        return method(path_params, query_params=query_params, payload=payload)

    @property
    def ischema_page_id(self) -> Optional[str]:
        """Return object id for ``information_schema`` page.
        
        .. deprecated:: 0.7.0
            Do not use, it will be removed in a future version.
            
        """
        return self._ischema_page_id 

    @abstractmethod
    def pages_create(
            self, 
            path_params: Optional[dict] = None,
            query_params: Optional[dict] = None, 
            payload: Optional[dict] = None
    ) -> dict:
        """Create a page object.

        This method creates a new page that is a child of an existing page or database.


        Args:
            payload (dict): The JSON object containing the required payload as specified by the Notion API.

        Returns:
            dict: The createdpage object with the property identifiers as the only key for each property object.
        """
        raise NotImplementedError

    @abstractmethod
    def pages_retrieve(
            self, 
            path_params: Optional[dict] = None,
            query_params: Optional[dict] = None, 
            payload: Optional[dict] = None
       ) -> dict:
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
    def pages_update(
            self, 
            path_params: Optional[dict] = None,
            query_params: Optional[dict] = None, 
            payload: Optional[dict] = None
    ) -> dict:
        """Update a page object.
        
        Use this API to modify attributes of a Notion page object, such as properties, title, etc.
        The payload follows the specific JSON required by Notion.
        The identifier of the Notion page to be udated (*path* parameter) shall be provided as key "page_id" in the payload.
        Here an example of a Python payload:

        .. code-block:: python
            
            # payload to update page with id 59833787-2cf9-4fdf-8782-e53db20768a5
            # with a new value for the property "student_id"
            {
                "page_id": "59833787-2cf9-4fdf-8782-e53db20768a5",
                "properties" : {
                    "student_id": {
                        "number": 654321
                    }
                } 
            }

        This is how the returned object looks like:

        .. code-block:: python

            {
                "object": "page",
                "id": "59833787-2cf9-4fdf-8782-e53db20768a5",
                
                # other keys ommitted for brevity

                "properties": {
                    "student_id": {
                        "id": "zag~"
                    }

                    # other properties omitted for brevity
                }
            }

        Args:
            payload (dict): The JSON object containing the required payload as specified by the Notion API.

        Returns:
            dict: The updated page object with the property identifiers as the only key for each property object.
        """
        raise NotImplementedError
    
    @abstractmethod
    def databases_create(
            self, 
            path_params: Optional[dict] = None,
            query_params: Optional[dict] = None, 
            payload: Optional[dict] = None
    ) -> dict:
        """Create a database as a subpage in the specified parent page, with the specified properties schema.

        Args:
            payload (dict): The JSON object containing the required payload as specified by the Notion API.

        Returns:
            dict: The created database object.
        """
        raise NotImplementedError
    
    @abstractmethod
    def databases_retrieve(
            self, 
            path_params: Optional[dict] = None,
            query_params: Optional[dict] = None, 
            payload: Optional[dict] = None
    ) -> dict:
        """Retrieve a database object for the provided ID

        Args:
            payload (dict): A dictionary containing the database id as key.

        Returns:
            dict: The retrieved database object or and empty dictionary if no
            databased object for the provided ID were found
        """
        raise NotImplementedError
    
    @abstractmethod
    def databases_query(
            self, 
            path_params: Optional[dict] = None,
            query_params: Optional[dict] = None, 
            payload: Optional[dict] = None
    ) -> List[dict]:
        """Get a list pages contained in the database.

        Args:
            path_params (dict): A dictionary containing a "database_id" key for the database to
                query.
            query_params (dict): A dictionary containing "filter" object to select.
            payload (dict): A dictionary that must contain a "database_id" key for the database to
                query and "filter" object to select.

        Returns:
            List[dict]: The list containing the page pbjects or ``[]``, if no pages have been found.
        """
        raise NotImplementedError
    
class InMemoryNotionClient(AbstractNotionClient):
    """Provide a simple but complete in-memory Notion client.
    
    :class:`InMemoryNotionClient` fully implements the Notion API and mimics the Notion's store behavior.
    This class is best suited for testing purposes as it avoids the HTTP communication.
    It has been designed to mimic as close as possible the behavior of Notion, including error messages.

    .. versionchanged:: 0.7.0 
        In this version, the :attr:`_store` is a Python :type:`dict` to provide random access.
        The object indentifier is used as key and the object itself is the value.
        Additionally, the store is always initialized with a root page as is the case for Notion internal integrations.

    .. deprecated:: 0.7.0
        The :meth:`__init__` parameters **shall not** be used anymore, they will be removed in a future verison. 
        The datastructures info schema page and tables are created by the :class:`normlite.engine.base.Engine`.
        Clients do not have knowledge of these datastructures.

    """

    _ROOT_PAGE_ID_ =        'ZZZZZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZZZZZZZZZ'
    """Fake root page identifier."""

    _ROOT_PAGE_PARENT_ID_ = 'YYYYYYYY-0000-1111-WWWWWWWWWWWWWWWWW'
    """Fake root page parent identifier."""

    _ROOT_PAGE_TITLE_ =     'ROOT_PAGE'
    """Fake root page title."""

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

        self._store: dict[str, Any] = {}
        """The dictionary simulating the Notion store. 
        It's an instance attribute to avoid unwanted side effects and 
        provide more behavioral predictability.

        .. versionchanged:: 0.7.0
            - Add root page by default in the constructor.
            - Fix issue with asymmetric file based Notion client (https://github.com/giant0791/normlite/issues/45).
        """
        self._store = {
            InMemoryNotionClient._ROOT_PAGE_ID_: self._new_object(
                'page', 
                {
                    'parent': {
                        'type': 'page_id',
                        'page_id': InMemoryNotionClient._ROOT_PAGE_PARENT_ID_,
                    },
                    'properties': {
                        'Title': {'title': [{'text': {'content': InMemoryNotionClient._ROOT_PAGE_TITLE_}}]}                   
                    }
                }
            )
        }

    def _create_store(self, store_content: List[dict] = []) -> None:
        """Provide helper to create the simulated Notion store.

        .. deprecated:: 0.7.0
            Do **not** use this helper method, it will break the internal store.
            There is currently no replacement. 
            Here a short code snippet to correctly pre-fill the store
            for test purposes.

            .. code-block:: python

                def for_page_queries(client: InMemoryNotionClient, page_payloads: list[dict]) -> InMemoryNotionClient:
                    ids = [
                        '680dee41-b447-451d-9d36-c6eaff13fb45',
                        '680dee41-b447-451d-9d36-c6eaff13fb46',
                        '680dee41-b447-451d-9d36-c6eaff13fb47'
                    ]
                    for id_, payload in enumerate(page_payloads):
                        _ = client._add('page', payload, ids[id_])

                    return client

        Args:
            store_content (List[dict], optional): The initial content for the Notion store. Defaults to ``[]``.

        """
        warnings.warn(
            '`_create_store()` is deprecated and will be removed in a future version. '
            'To add pages for testing purposes, use a fixture instead.' 
        )
        if store_content:
            for obj in store_content:
                oid = obj['id']
                self._store[oid] = obj
        else:
            self._store = {}

    def _get(self, id: str) -> dict:
        warnings.warn(
            '`_get()` is deprecated and will be removed in a future version. '
            'Use `_get_by_id()` instead' 
        )
        return self._get_by_id(id)
    
    def _get_by_id(self, id: str) -> dict:
        if self._store_len() == 0:
            return {}
    
        try:
            return self._store[id]
        except KeyError:                
            return {} 
   
    def _get_by_title(self, title: str, type: str) -> dict:
        """Return the first occurrence in the store of page or database with the passed title."""
        # TODO: rewrite using filter()
        if self._store_len() > 0:
            for o in self._store.values():
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
    
    def _new_object(self, type_: str, payload: dict, id: Optional[str] = None) -> dict:
        new_page_or_db = dict()
        new_page_or_db['object'] = type_

        # use the provided id for behavioral predictability if available 
        new_page_or_db['id'] = id if id else str(uuid.uuid4())
        current_date = datetime.now()
        new_page_or_db['created_id'] = current_date.isoformat()
        new_page_or_db['archived'] = False
        new_page_or_db['in_trash'] = False
        new_page_or_db.update(payload)
    
        return new_page_or_db

    def _add_database(self, new_object: dict) -> dict:
        parent_page_id = new_object.get('parent').get('page_id', None)
        if parent_page_id is None:
            raise NotionError(
                """
                    Body failed validation: 
                    body.parent.page_id should be defined, instead was undefined.
                """
            )

        if not self._get_by_id(parent_page_id):
            raise NotionError(
                f"""
                    Could not find page with ID: {parent_page_id}.
                    Make sure the relevant pages and databases are shared with your integration.
                """
            )

        new_object['is_inline'] = False
        properties = new_object['properties']
        for prop_name, prop_obj in properties.items():
            # inject keys "type" and "id" in each property:
            # ids are generated for databases, for "title" the 
            # id = "title"
            prop_type = list(prop_obj.keys())[0]
            properties[prop_name]['type'] = prop_type
            properties[prop_name]['id'] = (
                'title' 
                if prop_type == 'title' 
                else 
                self._generate_property_id()
            )

        return new_object

    def _add_page_to_database(self, new_page: dict) -> dict:
        """Helper to add a new page to an exisiting database.
        
        This method injects the "type" and "id" objects from the database into
        each property in the new_page['properties'] object.
        Since the new_page is being created, it stores properties with **values**.
        This method returns the new_page augmented with the "type" and "id" objects for all
        of its properties.
        """
        # retrieve database schema
        parent = new_page.get('parent')
        schema = self._get_by_id(parent.get('database_id'))
        if not schema:
            # no database found for this page object
            raise NotionError(
                f"""
                    Could not find database with ID: {parent.get('database_id')}.
                    Make sure the relevant pages and databases are shared with your integration.
                """
            )
        
        schema_properties = schema.get('properties')
        new_page_properties = new_page.get('properties')
        for key in new_page_properties.keys():
            page_prop = new_page_properties.get(key)
            schema_prop = schema_properties.get(key)
            if schema_prop is None:
                # all page "properties" keys must match the parent database's properties
                raise NotionError(
                    f"""
                        Could not find page property: {key} in database: {schema.get('title')} 
                        Make sure the relevant pages and databases are shared with your integration.
                    """
                )

            schema_value_key = list(schema_prop.keys())[0]
            schema_value = schema_prop[schema_value_key]
            page_prop[key] == {**schema_value, **page_prop}

        return new_page
    
    def _add_page_to_page(self, new_page: dict) -> dict:
        parent = new_page.get('parent')
        parent_page = self._get_by_id(parent.get('page_id'))
        if not parent_page:
            raise NotionError(
                f"""
                    Could not find page with ID: {parent.get('page_id')}.
                    Make sure the relevant pages and databases are shared with your integration.
                """
            )

        new_page_properties = new_page.get('properties')
        prop_key = list(new_page_properties.keys())
        prop_value = new_page_properties.get(prop_key[0])

        if len(prop_key) != 1 or prop_value.get('title') is None:
            # title is the only valid property in the properties body parameter.
                raise NotionError(
                    f"""
                        New page is a child of page: {new_page.get('parent').get('page_id')}. 
                        "title" is the only valid property in the properties body parameter.
                    """
                )

        if prop_value.get('type') is None:
            prop_value['type'] = 'title'
        
        return new_page

    def _add_page(self, new_page: dict) -> dict:
        if new_page.get('parent').get('type') == 'database_id':
            return self._add_page_to_database(new_page)
        else:
            return self._add_page_to_page(new_page)
        
    def _add_page_old(self, new_object: dict) -> dict:
        """DEPRECATED: It's been refactored. """
        # TODO: refactor to check parent ids availability and existence first
        ret_new_pg = copy.deepcopy(new_object)
        ret_new_pg.pop('properties')
        property_keys = [key for key in new_object.get('properties').keys()]
        ret_new_pg['properties'] = {name: {} for name in property_keys}
        parent = new_object.get('parent')
        properties = new_object['properties']
        if parent.get('type') == 'database_id':
            # parent is a database, copy database property ids into page property ids
            database_id = parent.get('database_id', None)
            if database_id is None:
                raise NotionError(
                    'Body failed validation: body.parent.database_id should be defined, instead was undefined.'
                )
            
            schema = self._get_by_id(parent.get('database_id'))
            if not schema:
                # no database found for this page object
                raise NotionError(
                    f'Could not find database with ID: {parent.get('database_id')} '
                    'Make sure the relevant pages and databases are shared with your integration.'
                )
                
            for prop_name, prop_obj in properties.items():
                # Bug fix: the page properties contain the following keys: "id", "type" and the value of "type" as key to represent the value
                # Example:
                # "Last ordered": {
                #   "id": "Jsfb",
                #   "type": "date",
                #   "date": {
                #       "start": "2022-02-22",
                #       "end": null,
                #       "time_zone": null
                #   }
                # }

                # construct new page object for the store
                prop_id = schema['properties'][prop_name]['id']
                prop_type = schema['properties'][prop_name]['type']
                prop_obj['id'] =  prop_id
                prop_obj['type'] = prop_type

                # constrcut the page object to be returned
                ret_new_pg['properties'][prop_name]['id'] = prop_id
        else:
            # parent is a page, generate new property ids
            # TODO: add test for availability of page under page_id
            # IMPORTANT: If you do this, you break pre-filling of fresh clients for testing purposes.
            # Consider always adding a root page to mimic the internal integrations case.
            # See https://developers.notion.com/reference/post-page#choosing-a-parent
            for prop_name, prop_obj in properties.items():
                prop_type = list(prop_obj.keys())
                
                # new pages with parent = page have only the 'title' type
                prop_obj['id'] = 'title' if prop_type[0] == 'title' else self._generate_property_id()
                ret_new_pg['properties'][prop_name]['id'] = prop_obj['id']

        return ret_new_pg
    
    def _raise_if_validation_fails(self, type_: str, payload: dict) -> NoReturn:
        # check well-formedness of payload
        # Note: "properties" object existence is validated previously when binding parameters 
        parent = payload.get('parent', None)
        if not parent:
            # objects being added need to have the parent they belog to
            raise NotionError('Body failed validation: body.parent should be defined, instead was undefined')  
        
        if type_ not in ['page', 'database']:
            raise NotionError(
                f'Body failed validation: body.parent.type should be either '
                f'"page" or "database", instead "{type_}" was defined')

        if type_ == 'database' and not payload.get('title'):
            raise NotionError(
                f'Body failed validation: body.parent.title should be defined '
                f'for database object, instead was undefined')
        
        if not payload.get('properties', None):
            raise NotionError('Body failed validation: body.properties should be defined, instead was undefined')  
        
    def _add(self, type_: str, payload: dict, id: Optional[str] = None) -> dict: 
        """Add Notion objects to the store.
        
        This utility method handles 3 use cases:
            - add a database
            - add a page to an existing database
            - add a page to an existing page


        .. versionchanged: 0.7.0
            This method now ensures payload validation and orchestrates the object creation and
            storing in the internal data structure.
        """
        self._raise_if_validation_fails(type_, payload)
        new_object = self._new_object(type_, payload, id)
        if type_ == 'page':
            ret_object = self._add_page(new_object)
        elif type_ == 'database':
            ret_object = self._add_database(new_object)
        elif type_ == 'data_source':
            raise NotionError('"data_source" type not supported yet')
        else:
            raise NotionError(f'"{type_}" not supported or unknown')

        self._store[new_object['id']] = new_object
        return ret_object

    def _generate_property_id(self) -> str:
        """
        Generate a pseudo Notion-like property id.
        
        These ids are short, random strings containing
        letters and a few special characters, then URL-encoded.
        """
        # generate an identifier of length between 4 and 6 chars
        length = random.randint(4, 6)

        # plausible alphabet based on decoded examples:
        alphabet = string.ascii_letters + string.digits + ":;@[]?`"

        # generate random sequence
        raw = ''.join(random.choice(alphabet) for _ in range(length))

        # URL-encode non-alphanumeric characters to mimic Notion API output
        encoded = urllib.parse.quote(raw, safe=string.ascii_letters + string.digits)

        return encoded

    def _store_len(self) -> int:
        return len(self._store)
    
    def pages_create(
            self,
            path_params: Optional[dict] = None, 
            query_params: Optional[dict] = None, 
            payload: Optional[dict] = None
    ) -> dict:
        if not payload:
            raise NotionError(
                'Body failed validation: body empty or None (null).'
            )
        return self._add('page', payload)
    
    def pages_retrieve(
            self, 
            path_params: Optional[dict] = None,
            query_params: Optional[dict] = None, 
            payload: Optional[dict] = None
    ) -> dict:
        page_id = path_params.get('page_id', None)

        if page_id is None:
            raise NotionError(
                'Invalid request URL.'
            )

        retrieved_object = self._get_by_id(page_id)

        if not retrieved_object:
            raise NotionError(
                    f'Could not find page with ID: {page_id} '
                    'Make sure the relevant pages and databases are shared with your integration.'
            )

        return retrieved_object
    
    def pages_update(
            self, 
            path_params: Optional[dict] = None,
            query_params: Optional[dict] = None, 
            payload: Optional[dict] = None
    )-> dict:
        if self._store_len() > 0:
            page_id = path_params.get('page_id', None)

            if page_id is None:
                raise NotionError(
                    'Invalid request URL.'
                )

            page_to_update = self._get_by_id(page_id)
            if page_to_update and page_to_update['object'] == 'page':
                if 'archived' in payload.keys():
                    page_to_update['archived'] = payload['archived']
                    return page_to_update
                elif 'in_trash' in payload.keys():
                    page_to_update['in_trash'] = payload['in_trash']
                elif 'properties' in payload.keys():
                    for prop, value in payload['properties'].items():
                        page_to_update['properties'][prop] = value
                else:
                    raise NotionError(
                        'Body failed validation: body.archived or body.in_trash or '
                        'body.properties should be defined, instead was undefined.'
                    )
            else:
                raise NotionError(
                    f'Could not find page with id: {payload.get('page_id')}. '
                    'Make sure the relevant pages and databases are shared with your integration.'
                )

    def databases_create(
            self, 
            path_params: Optional[dict] = None, 
            query_params: Optional[dict] = None, 
            payload: Optional[dict] = None
    ) -> dict:
        if not payload:
            raise NotionError(
                'Body failed validation: body empty or None (null).'
            )
        return self._add('database', payload)
    
    def databases_retrieve(
            self, 
            path_params: Optional[dict] = None, 
            query_params: Optional[dict] = None, 
            payload: Optional[dict] = None
    ) -> dict:
        database_id = path_params.get('database_id', None)
        if database_id is None:
            raise NotionError(
                'Invalid request URL.'
            )

        retrieved_object = self._get_by_id(database_id)
        if not retrieved_object:
            raise NotionError(
                    f'Could not find database with ID: {database_id} '
                    'Make sure the relevant pages and databases are shared with your integration.'
            )

        return retrieved_object
    
    def _filter_properties(
            self, 
            original_obj: dict, 
            filter_list: Optional[list[str]] = []
        ) -> dict:
        props = original_obj.get('properties', {})
        filtered_props = {
            k: v for k, v in props.items()
            if not filter_list or k in filter_list
        }

        return {
            **original_obj,
            'properties': filtered_props
        }
    
    def databases_query(
            self, 
            path_params: Optional[dict] = None, 
            query_params: Optional[dict] = None, 
            payload: Optional[dict] = None
    ) -> dict:
        query_results = []
        query_result_object = {
            'object': 'list',
            'results': query_results,
            'next_cursor': None,
            'has_more': False,
            'type': 'page',
            'page': {}
        }

        if self._store_len() > 0:
            # perform search only if store contains data        
            database_id = path_params.get('database_id', None)
            if database_id is None:
                raise NotionError(
                    'Invalid request URL.'
                )

            for obj in self._store.values():
                if obj['object'] == 'page' and obj['parent']['type'] == 'database_id':
                    # select only pages whose parent is a database
                    if obj['parent']['database_id'] == database_id:
                        # select only pages belonging to the db specified in the payload
                        filter = _Filter(obj, payload)
                        if filter.eval():
                            # select only those pages for which the filter evaluates to True 
                            if query_params:
                                filter_properties = query_params.get('filter_properties', None)
                                if filter_properties is None:
                                    filter_properties = []
                                # return only those properties specified in the query params
                                query_results.append(
                                    self._filter_properties(obj, filter_properties)
                                )
                            else:
                                query_results.append(obj)

        return query_result_object      

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
def _parse_notion_date(value: Optional[str]) -> Optional[datetime]:
    """Helper for implementing after and before operators on dates."""
    if value is None:
        return None

    if not isinstance(value, str):
        raise TypeError(f"Expected date string, got {type(value)}")

    try:
        # Python 3.11+ handles ISO 8601 offsets cleanly
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(f"Invalid Notion date string: {value!r}")

class _Expression(ABC):
    @abstractmethod
    def eval(self) -> bool:
        pass

class _EmptyType:
    """Centralized sentinel class for signaling empty properties."""
    __slots__ = ()

    def __repr__(self):
        return "<EMPTY>"

EMPTY_DATE = _EmptyType()
EMPTY_TEXT = _EmptyType()
EMPTY_NUMBER = _EmptyType()
EMPTY_CHECKBOX = _EmptyType()


class _Condition(_Expression):
    _allowed_ops = {
        "title":     {"contains", "does_not_contain", "starts_with", "ends_with", "is_empty", "is_not_empty", "equals"},
        "rich_text": {"contains", "does_not_contain", "starts_with", "ends_with", "is_empty", "is_not_empty", "equals"},
        "number":    {"equals", "greater_than", "less_than"},
        "date":      {"after", "before", "equals", "does_not_equal", "is_empty", "is_not_empty"},
        "checkbox":  {"equals", "does_not_equal"},
    }

    _op_map = {
        # date
        "date.is_empty":                lambda a, _: a is None,
        "date.is_not_empty":            lambda a, _: a is not None,
        "date.equals":                  lambda a, b: a == b,
        "date.does_not_equal":          lambda a, b: a != b,
        "date.after": lambda a, b: (
            a["start"] is not None
            and b["start"] is not None
            and a["start"] > b["start"]
        ),
        "date.before": lambda a, b: (
            a["start"] is not None
            and b["start"] is not None
            and a["start"] < b["start"]
        ),

        # rich_text
        "rich_text.equals":             lambda a, b: a == b if a is not EMPTY_TEXT else False,
        "rich_text.is_empty":           lambda a, _: a is EMPTY_TEXT,
        "rich_text.is_not_empty":       lambda a, _: a is not EMPTY_TEXT,
        "rich_text.contains":           lambda a, b: False if a is EMPTY_TEXT else b in a,
        "rich_text.does_not_contain":   lambda a, b: True if a is EMPTY_TEXT else b not in a,
        "rich_text.starts_with":        lambda a, b: False if a is EMPTY_TEXT else a.startswith(b),
        "rich_text.ends_with":          lambda a, b: False if a is EMPTY_TEXT else a.endswith(b),

        # title
        "title.equals":                 lambda a, b: a == b if a is not EMPTY_TEXT else False,
        "title.is_empty":               lambda a, _: a is EMPTY_TEXT,
        "title.is_not_empty":           lambda a, _: a is not EMPTY_TEXT, 
        "title.contains":               lambda a, b: False if a is EMPTY_TEXT else b in a,
        "title.does_not_contain":       lambda a, b: True if a is EMPTY_TEXT else b not in a,
        "title.starts_with":            lambda a, b: False if a is EMPTY_TEXT else a.startswith(b),
        "title.ends_with":              lambda a, b: False if a is EMPTY_TEXT else a.endswith(b),

        # number
        "number.equals":                lambda a, b: a == b,
        "number.greater_than":          lambda a, b: a > b,
        "number.less_than":             lambda a, b: a < b,

        # checkbox
        "checkbox.equals":              lambda a, b: a is b,
    }


    def __init__(self, page: dict, condition: dict):
        self.page = page
        self.condition = condition

        self.prop_name = self._extract_property()
        self.property_obj = self._extract_property_obj()
        self.type_name, self.type_filter = self._extract_filter()
        self.actual_type = self._extract_actual_type()

        self._validate_type()
        self.op, self.value = self._extract_operator()
        self._validate_operator()

    def _extract_property(self) -> str:
        try:
            return self.condition["property"]
        except KeyError:
            raise ValueError("Filter condition missing 'property' key")

    def _extract_property_obj(self) -> dict:
        try:
            return self.page["properties"][self.prop_name]
        except KeyError:
            raise ValueError(f"Property '{self.prop_name}' not found on page")

    def _extract_filter(self) -> tuple[str, dict]:
        filters = [(k, v) for k, v in self.condition.items() if k != "property"]
        if len(filters) != 1:
            raise ValueError(f"Invalid filter structure for property '{self.prop_name}'")
        return filters[0]

    def _extract_actual_type(self) -> str:
        try:
            return self.property_obj['type']
        except Exception:
            raise ValueError(f"Malformed property object for '{self.prop_name}'")

    def _validate_type(self):
        if self.type_name != self.actual_type:
            raise ValueError(
                f"Invalid filter: property '{self.prop_name}' is of type '{self.actual_type}', "
                f"not '{self.type_name}'"
            )

    def _extract_operator(self):
        if len(self.type_filter) != 1:
            raise ValueError(f"Invalid operator specification for '{self.prop_name}'")
        return next(iter(self.type_filter.items()))

    def _validate_operator(self):
        allowed = self._allowed_ops[self.type_name]
        if self.op not in allowed:
            raise ValueError(
                f"Operator '{self.op}' not allowed for type '{self.type_name}'. "
                f"Allowed: {sorted(allowed)}"
            )

    def eval(self) -> bool:
        opname = f'{self.type_name}.{self.op}'
        func = self._op_map[opname]

        if self.type_name in ("title", "rich_text"):
            texts = self.property_obj[self.type_name]
            operand = (
                texts[0]["text"]["content"]
                if texts
                else EMPTY_TEXT
            )

        elif self.type_name == 'date':
            operand = normalize_page_date(self.property_obj.get("date"))

            # unary operators
            if self.op in ("is_empty", "is_not_empty"):
                return func(operand, None)

            # binary operators
            self.value = normalize_filter_date(self.value)

            if operand is None or self.value is None:
                return False

        else:
            operand = self.property_obj[self.type_name]

        return func(operand, self.value)

class _LogicalCondition(_Expression):
    def __init__(self, op: str, expressions: list[_Expression]):
        self.op = op
        self.expressions = expressions

        if self.op == "not" and len(expressions) != 1:
            raise ValueError("'not' operator requires exactly one condition")

    def eval(self) -> bool:
        if self.op == "and":
            return all(expr.eval() for expr in self.expressions)
        elif self.op == "or":
            return any(expr.eval() for expr in self.expressions)
        elif self.op == "not":
            return not self.expressions[0].eval()
        else:
            raise ValueError(f"Unknown logical operator '{self.op}'")

class _Filter:
    def __init__(self, page: dict, filter: dict):
        self.page = page
        self.filter = filter
        self.compiled: _Expression | None = None

    def _compile_expression(self, node: dict) -> _Expression:
        # Logical nodes
        if "and" in node:
            return _LogicalCondition(
                "and",
                [self._compile_expression(child) for child in node["and"]],
            )

        if "or" in node:
            return _LogicalCondition(
                "or",
                [self._compile_expression(child) for child in node["or"]],
            )

        if "not" in node:
            return _LogicalCondition(
                "not",
                [self._compile_expression(node["not"])],
            )

        # Leaf node
        return _Condition(self.page, node)

    def _compile(self):
        try:
            filter_obj = self.filter["filter"]
        except KeyError:
            raise ValueError("Filter missing 'filter' key")

        self.compiled = self._compile_expression(filter_obj)

    def eval(self) -> bool:
        if not self.compiled:
            self._compile()
        return self.compiled.eval()
