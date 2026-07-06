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
import json
from pathlib import Path
from typing import List, Optional, Self, Set, Type
from types import TracebackType
from abc import ABC, abstractmethod
import uuid
from datetime import datetime
import random
import string
import urllib.parse

from normlite.notion_sdk.getters import get_object_type, get_title
from normlite.notion_sdk.types import normalize_filter_date, normalize_page_date

class NotionError(Exception):
    """Exception raised for all errors related to the Notion REST API.
    
    This class mimics the HTTP response object returned by the Notion API.
    
    .. seealso::

        `Notion API status codes <https://developers.notion.com/reference/status-codes>`__

        
    .. versionchanged:: 0.8.0
    """
    def __init__(
        self,
        message: str,
        *,
        status_code: int = 400,
        code: str = "validation_error",
    ) -> None:
        self.status_code = status_code
        """HTTP status code
        
        .. seealso::

            `Notion API error codes and messages <https://developers.notion.com/reference/status-codes#error-codes>`__ 
        """

        self.code = code
        """Notion error code. 
        
        .. seealso::

            `Notion API error codes and messages <https://developers.notion.com/reference/status-codes#error-codes>`__ 
        """

        self.message = message
        """Notion error message.
        
        .. seealso::

            `Notion API error codes and messages <https://developers.notion.com/reference/status-codes#error-codes>__ 
        """

        # Preserve normal Exception behavior
        super().__init__(message)

    def to_response(self) -> dict:
        """
        Return a Notion-like error response payload.
        Useful for HTTP adapters and tests.
        """
        return {
            "object": "error",
            "status": self.status_code,
            "code": self.code,
            "message": self.message,
        }

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"status_code={self.status_code}, "
            f"code={self.code!r}, "
            f"message={self.message!r})"
        )

# Namespace UUID used to generate deterministic UUIDs
# Using the standard DNS namespace as a base
NAMESPACE_UUID = uuid.NAMESPACE_DNS

def encode_cursor(index: int) -> str:
    """Encodes an integer index into a valid, opaque UUID4 string."""
    # Step 1: Create a deterministic base UUID from the index to ensure uniqueness
    base_uuid = uuid.uuid5(NAMESPACE_UUID, f"cursor-{index}")
    
    # Step 2: Extract the raw bytes
    uuid_bytes = bytearray(base_uuid.bytes)
    
    # Step 3: Embed the integer into the last 4 bytes (supports up to 4.2 billion)
    # This keeps the integer safe inside the node payload of the UUID
    uuid_bytes[12:16] = index.to_bytes(4, byteorder='big')
    
    # Step 4: Enforce RFC 4122 UUID Version 4 variant and version bits
    uuid_bytes[6] = (uuid_bytes[6] & 0x0f) | 0x40  # Set version to 4
    uuid_bytes[8] = (uuid_bytes[8] & 0x3f) | 0x80  # Set variant to RFC 4122
    
    return str(uuid.UUID(bytes=bytes(uuid_bytes)))

def decode_cursor(cursor_str: str) -> int:
    """Decodes the opaque UUID4 string back into the original integer index."""
    try:
        # Convert string back to UUID object
        parsed_uuid = uuid.UUID(cursor_str)
        uuid_bytes = parsed_uuid.bytes
        
        # Extract the integer from the last 4 bytes of the payload
        index = int.from_bytes(uuid_bytes[12:16], byteorder='big')
        return index
    except ValueError as e:
        raise ValueError(f"Invalid or corrupted cursor string: {cursor_str}") from e
    
class _ClientQueryEngine:
    def __init__(
        self, 
        store: dict[str, dict],
        path_params: Optional[dict] = None,
        query_params: Optional[dict] = None,
        payload: Optional[dict] = None
    ) -> None:
        self._store = store
        self._path_params = path_params
        self._payload = payload or {}
        self._filter_props = []
        if query_params is not None:
            self._filter_props = query_params.get("filter_properties", [])
        self._query_results = []
        self._query_result_object = {
            'object': 'list',
            'results': self._query_results,
            'next_cursor': None,
            'has_more': False,
            'type': 'page',
            'page': {}
        }

    def _paginate(self) -> None:
        # pagination slices every time the same overall result,
        # but it uses the start_cursor for the next offset to begin to slice from
        requested_page_size = self._payload.get("page_size") or InMemoryNotionClient.NOTION_MAX_PAGE_SIZE
        page_size = min(requested_page_size, InMemoryNotionClient.NOTION_MAX_PAGE_SIZE)
        start_cursor = self._payload.get("start_cursor")
        offset = decode_cursor(start_cursor) if start_cursor is not None else 0
        end = offset + page_size
        
        # slice the next page and encode the next cursor if necessary
        next_page = [
            p 
            for p in self._query_results[offset:end]
        ]
        self._query_result_object["results"] = next_page
        if end < len(self._query_results):
            self._query_result_object["has_more"] = True
            self._query_result_object["next_cursor"] = encode_cursor(end)

    def _sort(self) -> None:
        sorts = self._payload.get("sorts")
        if sorts:
            for sort in reversed(sorts):
                prop = sort.get("property")
                direction = sort.get("direction", "ascending")

                if direction not in ("ascending", "descending"):
                    raise ValueError(f"Invalid sort direction '{direction}'")

                reverse = direction == "descending"

                def sort_key(page):
                    value = _extract_sort_value(page, prop)
                    is_empty = value in (None, EMPTY_TEXT, EMPTY_NUMBER)
                    return (is_empty, value)

                self._query_results.sort(key=sort_key, reverse=reverse)
        
    def _filter(self) -> None:
        data_source_id = (
            self._path_params.get('data_source_id') 
            if self._path_params 
            else None
        )

        if data_source_id is None:
            raise NotionError('Invalid request URL: data_source_id should be defined.')

        # payload is guarded in the __init__ to be non-None
        has_filter = self._payload.get('filter', False)

        # --------------------
        # Filtering phase
        # --------------------
        pages = []

        for obj in self._store.values():
            if obj.get("object") != "page":
                continue

            parent = obj.get("parent", {})

            if parent.get("data_source_id") != data_source_id:
                continue

            if obj.get("in_trash"):
                # always skip delete (in trash) pages
                continue

            if not has_filter:
                pages.append(obj)
                continue

            predicate = _Filter(obj, self._payload)
            if predicate.eval():
                pages.append(obj)
        
        
        self._query_results.extend(pages)    

    def _project(self) -> None:
        if not self._filter_props:
            return
        
        # project only if "filter_properties" has been provided
        # IMPORTANT: projection must be a pure per-row column transform 
        # on the paginated results, that's why it mutates the "result" value in-place.
        # This ensures the store stays untouched.
        self._query_result_object["results"] = [
            self._filter_properties(p, self._filter_props)
            for p in self._query_result_object["results"]
        ]
    
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

    def execute(self) -> dict:
        if len(self._store) == 0:
            return self._query_result_object
        
        self._filter()      # WHERE    - working set (may be live store refs)
        self._sort()        # ORDER_BY - 
        self._paginate()    # LIMIT    - pure windowing + cursors
        self._project()     # SELECT   - narrow columns, detach into copies
        return self._query_result_object

class AbstractNotionClient(ABC):
    """Base class for a Notion API client.

    """
    allowed_operations: Set[str] = set()
    """The set of Notion API calls."""

    NOTION_MAX_PAGE_SIZE = 100

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

    def close(self) -> None:
        """Release any resources held by this client.

        The default implementation is a no-op. Subclasses with persistent
        resources (e.g. open files) should override this method.

        .. versionadded:: 0.10.0
        """

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
            dict: The created page object with the property identifiers as the only key for each property object.
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
            query_params (dict): A dictionary containing "filter_properties" object to restrict properties returned.
            payload (dict): A dictionary that must contain the "filter" and optionally "sorts" objects modeling the query.

        Returns:
            List[dict]: The list containing the page objects or ``[]``, if no pages have been found.
        """
        raise NotImplementedError
    
    @abstractmethod
    def databases_update(
        self, 
        path_params: Optional[dict] = None,
        query_params: Optional[dict] = None, 
        payload: Optional[dict] = None
    ) -> dict:
        """Update the given database.

        This methods updates the database object — the properties, title, description, or whether it's in the trash — of a specified database.         

        .. versionadded:: 0.8.0

        Args:
            path_params (dict): A dictionary containing a "database_id" key for the database to
                query.
            query_params (dict): A dictionary containing "filter_properties" object to restrict properties returned.
            payload (dict): A dictionary that must contain the "filter" and optionally "sorts" objects modeling the query.

        Returns:
            dict: The updated database object.
        """
        raise NotImplementedError
    
    @abstractmethod
    def data_sources_query(
        self, 
        path_params: Optional[dict] = None,
        query_params: Optional[dict] = None, 
        payload: Optional[dict] = None
    ) -> List[dict]:
        """Get a list of pages contained in the data source.

        Args:
            path_params (dict): A dictionary containing a "data_source_id" key for the database to
                query.
            query_params (dict): A dictionary containing "filter_properties" object to restrict properties returned.
            payload (dict): A dictionary that must contain the "filter" and optionally "sorts" objects modeling the query.

        Returns:
            List[dict]: The list containing the page objects or ``[]``, if no pages have been found.

        .. versionadded:: 0.12.0
        """
        raise NotImplementedError
    
    @abstractmethod
    def data_sources_retrieve(
        self,
        path_params: Optional[dict] = None,
        query_params: Optional[dict] = None, 
        payload: Optional[dict] = None
    ) -> dict:
        """Retrieve a data source object for the provided ID

        Args:
            payload (dict): A dictionary containing the data source id as key.

        Returns:
            dict: The retrieved database object or and empty dictionary if no
            data source object for the provided ID were found
        """
        raise NotImplementedError
 
    @abstractmethod
    def search(
        self,
        path_params: Optional[dict] = None,
        query_params: Optional[dict] = None, 
        payload: Optional[dict] = None
    ) -> dict:
        """Search all pages and databases by title

        Searches all parent or child pages and databases that have been shared with an integration.
        Returns all pages or databases, excluding duplicated linked databases, that have 
        titles that include the query param. If no query param is provided, then the response contains 
        all pages or databases that have been shared with the integration.

        Args:
            path_params (Optional[dict], optional): Not used. Defaults to None.
            query_params (Optional[dict], optional): Not used. Defaults to None.
            payload (Optional[dict], optional): Dictionary specifying the text to be searched for with
                the key "query" and the object type with the key "property".

        Returns:
            dict: Dictionary as result object containing the search results. The list key "result" is empty,
                if no results were found.
        """


class InMemoryNotionClient(AbstractNotionClient):
    """Provide a simple but complete in-memory Notion client.
    
    :class:`InMemoryNotionClient` fully implements the Notion API and mimics the Notion's store behavior.
    This class is best suited for testing purposes as it avoids the HTTP communication.
    It has been designed to mimic as close as possible the behavior of Notion, including error messages.

    .. versionchanged:: 0.8.0
        Major refactor to provide more robust Notion-like behavior to the API methods 
        and adds object validation Notion objects.
        This version is prepared for the next step of refactor to support store persitence
        with the :class:`FileBasedNotionClient`.
    
    .. versionchanged:: 0.7.0 
        In this version, the :attr:`_store` is a Python :type:`dict` to provide random access.
        The object indentifier is used as key and the object itself is the value.
        Additionally, the store is always initialized with a root page as is the case for Notion internal integrations.

    .. deprecated:: 0.7.0
        The :meth:`__init__` parameters **shall not** be used anymore, they will be removed in a future verison. 
        The datastructures info schema page and tables are created by the :class:`normlite.engine.base.Engine`.
        Clients do not have knowledge of these datastructures.

    """
    _ROOT_PAGE_ID_ = 'ZZZZZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZZZZZZZZZ'
    """Fake root page identifier."""

    _ROOT_PAGE_PARENT_ID_ = 'YYYYYYYY-0000-1111-WWWWWWWWWWWWWWWWW'
    """Fake root page parent identifier."""

    _ROOT_PAGE_TITLE_ = 'ROOT_PAGE'
    """Fake root page title."""

    def __init__(
        self,
        ws_id: Optional[str] = None,
        ischema_page_id: Optional[str] = None,
        tables_db_id: Optional[str] = None,
    ):
        super().__init__()
        self._ws_id = ws_id or '00000000-0000-0000-0000-000000000000'
        self._ischema_page_id = ischema_page_id or '66666666-6666-6666-6666-666666666666'
        self._tables_db_id = tables_db_id or 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
        self._store: dict[str, dict] = {}

    # ------------------------------------------------------------------
    # Store invariants
    # ------------------------------------------------------------------

    def _ensure_root(self) -> None:
        """Create the root page at init time.
        
        .. versionchanged:: 0.8.0
            This method is no longer part of the client's initialization.
            Notion clients must enforce the invariant that the store is empty
            after creation.
            Users of this class have the responsibility to call this utility method,
            if required (see :meth:`normlite.engine.base.Engine._bootstrap` for more 
            details).

            
        """
        if self._ROOT_PAGE_ID_ not in self._store:
            self._store[self._ROOT_PAGE_ID_] = self._new_object(
                "page",
                {
                    "parent": {
                        "type": "page_id",
                        "page_id": self._ROOT_PAGE_PARENT_ID_,
                    },
                    "properties": {
                        "Title": {
                            "type": "title",
                            "id": "title",
                            "title": [
                                {"text": {"content": self._ROOT_PAGE_TITLE_}}
                            ]
                        }
                    },
                },
                id=self._ROOT_PAGE_ID_,
            )

    def _get_by_id(self, id: str) -> dict:
        """Simple accessor to the store by object id."""
        return self._store.get(id, {})
    
    def _get_by_title(self, text: str, object_: str) -> dict:
        """Return all objects with "title" text in the store. """

        query_results = []
        query_result_object = {
            'object': 'list',
            'results': query_results,
            'next_cursor': None,
            'has_more': False,
            'type': 'page',
            'page': {}
        }

        if self._store_len() == 0:
            return query_result_object
        
        for obj in self._store.values():
            if get_object_type(obj) != object_:
                continue

            title = get_title(obj)
            if title and title == text:
                query_results.append(obj)
            
        return query_result_object

    # ------------------------------------------------------------------
    # Object construction
    # ------------------------------------------------------------------

    def _new_object(self, type_: str, payload: dict, id: Optional[str] = None) -> dict:
        """Create a new Notion object."""

        now = datetime.now().isoformat()
        obj = {
            "object": type_,
            "id": id or str(uuid.uuid4()),
            "created_time": now,
            "archived": False,
            "in_trash": False,
        }
        obj.update(copy.deepcopy(payload))
        return obj

    # ------------------------------------------------------------------
    # Validation helpers
    # ------------------------------------------------------------------

    def _validate_payload_base(self, type_: str, payload: dict) -> None:
        if not payload:
            raise NotionError("Body failed validation: body empty or None (null).")

        parent = payload.get("parent")
        if not parent:
            raise NotionError(
                "Body failed validation: body.parent should be defined, instead was undefined."
            )

        parent_type = parent.get("type")
        if parent_type not in ("page_id", "data_source_id"):
            raise NotionError(
                f'Body failed validation: body.parent.type should be "page_id" or '
                f'"data_source_id", instead "{parent_type}" was defined.'
            )
        
        if type_ == "database" and parent_type != "page_id":
            raise NotionError(
                f'Body failed validation: body.parent.type should be "page_id", '
                f'instead "{parent_type}" was defined.'
            )

        if type_ == "database" and not payload.get("title"):
            raise NotionError(
                "Body failed validation: body.title should be defined for database object."
            )
        
        if type_ == "database" and "initial_data_source" not in payload:
            raise NotionError(
                "Body failed validation: body.initial_data_source should be defined, instead was undefined."
            )
        
        if type_ == "database" and "properties" not in payload["initial_data_source"]:
            raise NotionError(
                "Body failed validation: body.initial_data_source.properties should be defined, instead was undefined."
            )

        if type_ != "database" and "properties" not in payload:
            raise NotionError(
                "Body failed validation: body.properties should be defined, instead was undefined."
            )
        
    def _resolve_parent(self, payload: dict) -> tuple[str, dict]:
        parent = payload["parent"]
        if parent["type"] == "page_id":
            pid = parent.get("page_id")
            obj = self._get_by_id(pid)
            if not obj or obj["object"] != "page":
                raise NotionError(
                    f"Could not find page with ID: {pid}. "
                    "Make sure the relevant pages and databases are shared with your integration."
                )
            return "page", obj

        if parent["type"] == "database_id":
            did = parent.get("database_id")
            obj = self._get_by_id(did)
            if not obj or obj["object"] != "database":
                raise NotionError(
                    f"Could not find database with ID: {did}. "
                    "Make sure the relevant pages and databases are shared with your integration."
                )
            return "database", obj
        
        if parent["type"] == "data_source_id":
            oid = parent.get("data_source_id")
            obj = self._get_by_id(oid)
            if not obj or obj["object"] != "data_source":
                raise NotionError(
                    f"Could not find data source with ID: {oid}. "
                    "Make sure the relevant pages, databases, and data sources are shared with your integration."
                )
            return "data_source", obj

        raise AssertionError("Unreachable")

    # ------------------------------------------------------------------
    # Page finalization rules
    # ------------------------------------------------------------------

    def _finalize_page_under_page(self, page: dict) -> None:
        props = page["properties"]
        if len(props) != 1:
            raise NotionError(
                'New page is a child of a page. "title" is the only valid property.'
            )

        _, prop = next(iter(props.items()))
        if "title" not in prop or len(prop) != 1:
            raise NotionError(
                'New page is a child of a page. "title" is the only valid property.'
            )

        prop["type"] = "title"
        prop["id"] = "title"

    def _finalize_page_under_data_source(self, page: dict, data_source: dict) -> None:
        schema_props = data_source["properties"]
        page_props = page["properties"]

        if set(page_props.keys()) != set(schema_props.keys()):
            raise NotionError(
                f"Page properties must exactly match data source schema: "
                f"{sorted(schema_props.keys())}"
            )

        for name, schema_prop in schema_props.items():
            schema_type = schema_prop["type"]
            schema_id = schema_prop["id"]

            page_prop = page_props[name]
            if schema_type not in page_prop:
                raise NotionError(
                    f"Property '{name}' must be of type '{schema_type}'."
                )

            page_props[name] = {
                "id": schema_id,
                "type": schema_type,
                schema_type: page_prop[schema_type],
            }

    def _finalize_data_source_under_database(self, data_source: dict, database_id: str) -> None:
        data_source["parent"] = {
            "type": "database_id",
            "database_id": database_id
        }

    # ------------------------------------------------------------------
    # Database finalization
    # ------------------------------------------------------------------
    
    def _finalize_database(self, db: dict, data_source_id: str) -> None:
        parent_type, _ = self._resolve_parent(db)
        if parent_type != "page":
            raise NotionError("Databases can only be created under pages.")
        
        db["data_sources"] = [
            {"id": data_source_id}
        ]

        db["is_inline"] = False

    def _finalize_data_source(self, ds: dict) -> None:
        for _, prop in ds["properties"].items():
            prop_type = next(iter(prop.keys()))
            prop["type"] = prop_type
            prop["id"] = "title" if prop_type == "title" else self._generate_property_id()

    # ------------------------------------------------------------------
    # Normalization helpers
    # ------------------------------------------------------------------

    def _is_valid_uuid(self, oid: str) -> bool:
        if oid is None:
            return False
        
        if not isinstance(oid, str):
            return False

        try:
            # If the string is not a valid UUIDv4, this will raise a ValueError
            val = uuid.UUID(oid, version=4)
        except ValueError:
            return False
        
        # Ensure the string is in the standard canonical form
        return str(val) == oid       


    def _normalize_rich_text_item(self, rt: dict) -> dict:
        """
        Normalize a single rich-text item to Notion canonical form.
        """
        if "text" in rt:
            content = rt["text"].get("content")
            if not isinstance(content, str):
                raise NotionError("Invalid rich_text item: missing text.content")

            return {
                "type": "text",
                "text": {"content": content},
                "plain_text": content,
                "annotations": rt.get(
                    "annotations",
                    {
                        "bold": False,
                        "italic": False,
                        "strikethrough": False,
                        "underline": False,
                        "code": False,
                        "color": "default",
                    },
                ),
            }

        if "equation" in rt:
            expr = rt["equation"].get("expression")
            if not isinstance(expr, str):
                raise NotionError("Invalid equation rich_text item")

            return {
                "type": "equation",
                "equation": {"expression": expr},
                "plain_text": expr,
                "annotations": rt.get("annotations", {}),
            }

        raise NotionError(f"Unsupported rich_text item: {rt}")

    def _normalize_rich_text(self, value: list[dict]) -> list[dict]:
        if not isinstance(value, list):
            raise NotionError("rich_text must be a list")

        return [self._normalize_rich_text_item(rt) for rt in value]

    def _normalize_property(self, name: str, prop: dict) -> dict:
        prop_type = prop.get("type")
        if not prop_type:
            raise NotionError(f"Property '{name} 'missing 'type'")

        if prop_type == "title":
            if "title" not in prop:
                raise NotionError(f"title property '{name}' missing 'title' field")
            prop["title"] = self._normalize_rich_text(prop["title"])

        elif prop_type == "rich_text":
            if "rich_text" not in prop:
                raise NotionError(f"rich_text property '{name}' missing 'rich_text' field")
            prop["rich_text"] = self._normalize_rich_text(prop["rich_text"])

        elif prop_type == "relation":
            self._normalize_relation(name, prop["relation"])
                                   
        return prop

    def _normalize_properties(self, obj: dict) -> None:
        props = obj.get("properties")
        if not isinstance(props, dict):
            raise NotionError("Object missing properties")

        for name, prop in props.items():
            props[name] = self._normalize_property(name, prop)

    def _normalize_database_title(self, db: dict) -> None:
        title = db.get("title")
        if not isinstance(title, list):
            raise NotionError("Database title must be a rich_text list")

        db["title"] = self._normalize_rich_text(title)

    def _normalize_relation(self, name: str, value: list[dict]) -> list[dict]:
            if not isinstance(value, list):
                raise NotionError(f"relation property '{name}' must be a list of dictionaries containing object ids")
            
            is_list_of_dicts = all(isinstance(item, dict) for item in value)
            if  not is_list_of_dicts:
                raise NotionError(f"relation property '{name}' must be a list of dictionaries containing object ids")
            
            all_dicts_contain_oids = all(item.get("id") is not None for item in value)
            if not all_dicts_contain_oids:
                raise NotionError(f"relation property '{name}' contains some invalid object ids (missing 'id' key)")
            
            all_dicts_contain_valid_oids = all(self._is_valid_uuid(item.get("id")) for item in value)
            if not all_dicts_contain_valid_oids:
                raise NotionError(f"relation property '{name}' contains some invalid object ids (not a UUIDv4)")
            
            # Returns the (possibly transformed) list.
            # Today the return is identical to the input — validation-only, no real canonicalisation.
            return value

    def _update_database_properties(
        self,
        database: dict,
        updates: dict,
    ) -> None:
        if not isinstance(updates, dict):
            raise NotionError(
                "Database properties must be an object.",
                status_code=400,
                code="validation_error",
            )

        schema = database["properties"]

        def resolve_property(key: str) -> tuple[str, dict]:
            # ID match
            for name, prop in schema.items():
                if prop["id"] == key:
                    return name, prop
            # Name match
            if key in schema:
                return key, schema[key]

            raise NotionError(
                f"Property '{key}' does not exist.",
                status_code=400,
                code="validation_error",
            )

        for ref, spec in updates.items():
            name, prop = resolve_property(ref)
            prop_type = prop["type"]

            # ------------------------------------------------------------
            # Property deletion
            # ------------------------------------------------------------
            if spec is None:
                if prop_type == "title":
                    raise NotionError(
                        "Cannot delete title property.",
                        status_code=400,
                        code="validation_error",
                    )
                del schema[name]
                continue

            if not isinstance(spec, dict):
                raise NotionError(
                    f"Invalid property update for '{name}'.",
                    status_code=400,
                    code="validation_error",
                )

            # ------------------------------------------------------------
            # Rename
            # ------------------------------------------------------------
            if "name" in spec:
                if prop_type == "status":
                    raise NotionError(
                        "Cannot rename status property.",
                        status_code=400,
                        code="validation_error",
                    )

                new_name = spec["name"]
                if new_name in schema and new_name != name:
                    raise NotionError(
                        f"Property '{new_name}' already exists.",
                        status_code=400,
                        code="validation_error",
                    )

                schema[new_name] = schema.pop(name)
                name = new_name
                prop = schema[name]

            # ------------------------------------------------------------
            # Type / configuration update
            # ------------------------------------------------------------
            if prop_type in spec:
                # configuration update
                new_conf = spec[prop_type]

                if prop_type == "status":
                    raise NotionError(
                        "Cannot update status property schema.",
                        status_code=400,
                        code="validation_error",
                    )

                schema[name] = {
                    "id": prop["id"],
                    "type": prop_type,
                    prop_type: new_conf,
                }
                continue

            # type update
            new_type = next(iter(spec), {})

            if new_type:
                if prop_type == "title" and new_type != "title":
                    raise NotionError(
                        "Cannot change type of title property.",
                        status_code=400,
                        code="validation_error",
                    )

            schema[name] = {
                "id": prop["id"],
                "type": new_type,
                new_type: spec[new_type]
            }
                    


    # ------------------------------------------------------------------
    # Unified add entrypoint
    # ------------------------------------------------------------------

    def _add(self, type_: str, payload: dict, id: Optional[str] = None) -> dict:
        self._validate_payload_base(type_, payload)
        obj = self._new_object(type_, payload, id)

        if type_ == "page":
            parent_type, parent_obj = self._resolve_parent(obj)
            if parent_type == "page":
                self._finalize_page_under_page(obj)

            else:
                self._finalize_page_under_data_source(obj, parent_obj)

            self._normalize_properties(obj)
                

        elif type_ == "database":
            ds = self._new_object(
                "data_source", 
                payload = payload["initial_data_source"],
            )
            self._finalize_data_source(ds)
            self._finalize_data_source_under_database(ds, obj["id"])
            self._store[ds["id"]] = ds
            self._finalize_database(obj, ds["id"])
            self._normalize_database_title(obj)

        else:
            raise NotionError(f'"{type_}" not supported or unknown')

        self._store[obj["id"]] = obj
        return copy.deepcopy(obj)
    
    # ------------------------------------------------------------------
    # Utility methods
    # ------------------------------------------------------------------

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
    
    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    def pages_create(self, path_params=None, query_params=None, payload=None) -> dict:
        created_page = self._add("page", payload)

        # update properties to return property ids only as of Notion API version 2022-06-28
        # https://developers.notion.com/reference/page
        properties = created_page['properties']
        updated_props = {k: {'id': v['id']} for k, v in properties.items()}
        created_page['properties'] = updated_props
        return created_page


    def pages_retrieve(self, path_params=None, query_params=None, payload=None) -> dict:
        page_id = path_params.get("page_id")
        obj = self._get_by_id(page_id)
        if not obj:
            raise NotionError(
                f"Could not find page with ID: {page_id}. "
                "Make sure the relevant pages and databases are shared with your integration.",
                status_code=404,
                code="object_not_found"
            )
        return copy.deepcopy(obj)

    def pages_update(self, path_params=None, query_params=None, payload=None) -> dict:
        page_id = path_params.get("page_id") if path_params else None

        if page_id is None:
            raise NotionError(f"Invalid request URL: page_id should be defined.")
            
        obj = self._get_by_id(page_id)
        if not obj or obj["object"] != "page":
            raise NotionError(
                f"Could not find page with ID: {page_id}.",
                status_code=404,
                code='object_not_found'
            )

        if (
             "archived" not in payload and
             "in_trash" not in payload and
             "properties" not in payload
        ):
            raise NotionError(
                "Body failed validation: body.archived or body.in_trash or "
                "body.properties should be defined."
            )          

        if "archived" in payload:
            obj["archived"] = payload["archived"]
        
        if "in_trash" in payload:
            obj["in_trash"] = payload["in_trash"]
        
        if "properties" in payload:
            for k, v in payload["properties"].items():
                existing = obj["properties"].get(k)
                if existing is not None and "type" in existing:
                    prop_type = existing["type"]
                    if prop_type in v:
                        data = v[prop_type]
                        if prop_type in ("rich_text", "title"):
                            data = self._normalize_rich_text(data)

                        if prop_type == "relation":
                            data = self._normalize_relation(k, data)

                        existing[prop_type] = data
                    else:
                        obj["properties"][k] = v
                else:
                    obj["properties"][k] = v

        return copy.deepcopy(obj)

    def databases_create(self, path_params=None, query_params=None, payload=None) -> dict:
        return self._add("database", payload)

    def databases_retrieve(self, path_params=None, query_params=None, payload=None) -> dict:
        db_id = path_params.get("database_id") if path_params else None

        if db_id is None:
            raise NotionError(f"Invalid request URL: page_id should be defined.")

        obj = self._get_by_id(db_id)
        if not obj:
            raise NotionError(
                f"Could not find database with ID: {db_id}. "
                "Make sure the relevant pages and databases are shared with your integration.",
                status_code=404,
                code='object_not_found'
            )
        return copy.deepcopy(obj)

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

        if not self._store_len():
            return query_result_object
        
        payload = payload or {}

        database_id = path_params.get('database_id') if path_params else None
        if database_id is None:
            raise NotionError('Invalid request URL: database_id should be defined.')

        # if the in_trash filter is not set ("in_trash" not in payload), skip deleted by default
        skip_deleted_pages = (
            not payload.get("in_trash", True)      
            if payload 
            else
            None
        ) 

        has_filter = bool(payload and payload.get('filter'))
        sorts = payload.get("sorts") if payload else None

        filter_properties = []
        if query_params:
            filter_properties = query_params.get('filter_properties') or []

        # --------------------
        # Filtering phase
        # --------------------
        pages = []

        for obj in self._store.values():
            if obj.get("object") != "page":
                continue

            parent = obj.get("parent", {})
            if parent.get("type") != "database_id":
                continue

            if parent.get("database_id") != database_id:
                continue

            if (skip_deleted_pages and obj.get("in_trash")):
                continue

            if not has_filter:
                pages.append(obj)
                continue

            predicate = _Filter(obj, payload)
            if predicate.eval():
                pages.append(obj)

        # --------------------
        # Sorting phase
        # --------------------
        if sorts:
            for sort in reversed(sorts):
                prop = sort.get("property")
                direction = sort.get("direction", "ascending")

                if direction not in ("ascending", "descending"):
                    raise ValueError(f"Invalid sort direction '{direction}'")

                reverse = direction == "descending"

                def sort_key(page):
                    value = _extract_sort_value(page, prop)
                    is_empty = value in (None, EMPTY_TEXT, EMPTY_NUMBER)
                    return (is_empty, value)

                pages.sort(key=sort_key, reverse=reverse)

        # --------------------
        # Pagination
        # --------------------
        # recompute the view at every request
        requested_page_size = payload.get("page_size") or InMemoryNotionClient.NOTION_MAX_PAGE_SIZE
        page_size = min(requested_page_size, InMemoryNotionClient.NOTION_MAX_PAGE_SIZE)
        start_cursor = payload.get("start_cursor")
        offset = decode_cursor(start_cursor) if start_cursor is not None else 0
        end = offset + page_size

        # --------------------
        # Projection phase
        # --------------------
        for page in pages[offset:end]:
            if filter_properties:
                query_results.append(
                    self._filter_properties(page, filter_properties)
                )
            else:
                query_results.append(page)

        if end < len(pages):
            query_result_object["has_more"] = True
            query_result_object["next_cursor"] = encode_cursor(end)

        return query_result_object

    def databases_update(
            self, 
            path_params: Optional[dict] = None,
            query_params: Optional[dict] = None, 
            payload: Optional[dict] = None
    ) -> dict:

        # resolve database id
        database_id = path_params.get("database_id") if path_params else None
        if not database_id:
            raise NotionError(
                "Invalid request URL: database_id should be defined.",
                status_code=400,
                code="validation_error",
            )

        database = self._get_by_id(database_id)
        if not database or database.get("object") != "database":
            raise NotionError(
                f"Could not find database with ID: {database_id}. "
                "Make sure the relevant pages and databases are shared with your integration.",
                status_code=404,
                code="object_not_found",
            )

        if not payload:
            return copy.deepcopy(database)

        # top-level flags
        if "archived" in payload:
            database["archived"] = bool(payload["archived"])

        if "in_trash" in payload:
            database["in_trash"] = bool(payload["in_trash"])

        # database title update (top-level, not schema)
        if "title" in payload:
            title = payload["title"]
            if not isinstance(title, list):
                raise NotionError(
                    "Database title must be a rich_text array.",
                    status_code=400,
                    code="validation_error",
                )
            database["title"] = copy.deepcopy(title)

        # schema updates (DDL)
        if "properties" in payload:
            self._update_database_properties(
                database,
                payload["properties"],
            )

        return copy.deepcopy(database)
    
    def data_sources_query(
        self,
        path_params: Optional[dict] = None,
        query_params: Optional[dict] = None,
        payload: Optional[dict] = None
    ) -> dict:
        engine = _ClientQueryEngine(
            self._store,
            path_params=path_params,
            query_params=query_params,
            payload=payload
        )

        return engine.execute()
    
    def data_sources_retrieve(
        self,
        path_params: Optional[dict] = None,
        query_params: Optional[dict] = None,
        payload: Optional[dict] = None
    ) -> dict:
        ds_id = path_params.get("data_source_id") if path_params else None

        if ds_id is None:
            raise NotionError(f"Invalid request URL: data_source_id should be defined.")

        obj = self._get_by_id(ds_id)
        if not obj:
            raise NotionError(
                f"Could not find data source with ID: {ds_id}. "
                "Make sure the relevant pages and databases are shared with your integration.",
                status_code=404,
                code='object_not_found'
            )
        return copy.deepcopy(obj)

    def search(
            self, 
            path_params: Optional[dict] = None,
            query_params: Optional[dict] = None, 
            payload: Optional[dict] = None            
    ) -> dict:
        """Return all pages or databases whose title exactly matches the "query" text."""

        query = payload.get('query') if payload is not None else None
        filter = payload.get('filter') if payload is not None else None
        filter_by = None
        results = []
        result_object = {
            'object': 'list',
            'results': results,
            'next_cursor': None,
            'has_more': False,
            'type': 'page_or_database',
            'page': {}
        }        
        if filter is not None:
            if not isinstance(filter, dict):
                raise NotionError(
                    "Body failed validation: body.filter should be an object (not a string).",
                    status_code=400,
                    code="invalid_json"                    
                )

            filter_prop = filter.get('property')
            if filter_prop is None:
                raise NotionError(
                    "Body failed validation: body.property should be defined.",
                    status_code=400,
                    code="invalid_json"                    
                )
            
            filter_by = filter.get('value')
            if filter_by is None:
                raise NotionError(
                    "Body failed validation: body.value should be defined.",
                    status_code=400,
                    code="invalid_json"                    
                )
            
            if filter_by not in ("page", "database"):
                raise NotionError(
                    "Body failed validation: body.value should be either 'page' or 'database'.",
                    status_code=400,
                    code="invalid_json"                    
                )

        for obj in self._store.values():
            if filter_by is not None:
                if obj['object'] != filter_by:    
                    continue
            
            if query is not None:
                if query != get_title(obj):
                    continue

            results.append(obj)

        return result_object

    def find_child_page(self, parent_page_id: str, name: str) -> Optional[dict]:
        for obj in self._store.values():
            if obj["object"] != "page":
                continue

            parent = obj.get("parent", {})
            if parent.get("page_id") != parent_page_id:
                continue

            title = (
                obj.get("properties", {})
                .get("Name", {})
                .get("title", [])
            )

            if title and title[0]["text"]["content"] == name:
                return obj

        return None

    def find_child_database(self, parent_page_id: str, name: str) -> Optional[dict]:
        for obj in self._store.values():
            if obj["object"] != "database":
                continue

            parent = obj.get("parent", {})
            if parent.get("page_id") != parent_page_id:
                continue

            title = obj.get("title", [])
            if title and title[0]["text"]["content"] == name:
                return obj

        return None

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
    
    .. versionadded:: 0.8.0

    """

    STORE_VERSION = 1
    """Represent the store version used for compatibility check when reading data stores saved on filesystem."""


    def __init__(
        self, 
        path: str,
        *,
        read_only: bool = False,
        auto_load: bool = True,
        auto_flush: bool = True,
    ):
        super().__init__()

        self._path = Path(path)
        """The absolute path to the file storing the data contained in the file-base Notion client."""

        self._read_only = read_only
        """Readonly flag to avoid overwriting the file contents."""

        self._auto_load = auto_load
        """Auto load the store if ``True``."""

        self._auto_flush = auto_flush
        """Automatic flush """

        if self._auto_load and self._read_only and not self._path.exists():
            raise NotionError(
                f"Invalid request URL: {str(self._path)} not found",
                status_code=400,
                code="invalid_request_url"
            )

        if self._auto_load:
            self.load()

    def load(self) -> List[dict]:
        """Load the store content from the underlying file.

        Returns:
            List[dict]: The JSON object as list of dictionaries containing the store.
        """
        if not self._path.exists():
            self._store.clear()
            return

        with self._path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        version = data.get("version")
        if version != self.STORE_VERSION:
            raise NotionError(
                f"Unsupported store version: {version} "
                f"(expected {self.STORE_VERSION})"
            )

        objects = data.get("objects")
        if not isinstance(objects, dict):
            raise NotionError("Corrupted store: 'objects' missing or invalid")

        # IMPORTANT: store must contain canonical objects
        self._store = copy.deepcopy(objects)
    
    def flush(self) -> None:
        if self._read_only:
            return

        payload = {
            "version": self.STORE_VERSION,
            "objects": self._store,
        }

        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)

    def clear(self) -> None:
        self._store.clear()
        if self._path.exists() and not self._read_only:
            self._path.unlink()

    def __enter__(self) -> Self:
        """Initialize the Notion store in memory.

        When the context manager is entered, the Notion store is read in memory, if the corresponding
        file existes. Otherwise, the store in memory is initialized with an empty list.

        Returns:
            Self: This instance as required by the context manager protocol.
        """
        if self._auto_load:
            self.load()
        return self
        
    def close(self) -> None:
        """Flush the store to disk.

        .. versionadded:: 0.10.0
        """
        self.flush()

    def __exit__(
        self,
        exctype: Optional[Type[BaseException]] = None,
        excinst: Optional[BaseException] = None,
        exctb: Optional[TracebackType] = None,
    ) -> Optional[bool]:
        """Dump the Notion stored to the file.

        .. versionchanged:: 0.10.0
            Delegates to :meth:`close` instead of calling :meth:`flush` directly.

        Args:
            exctype (Optional[Type[BaseException]]): The exception class. Defaults to ``None``.
            excinst (Optional[BaseException]): The exception instance. Defaults to ``None``.
            exctb (Optional[TracebackType]): The traceback object. Defaults to ``None``.

        Returns:
            Optional[bool]: ``False`` as it is customary for context managers.
        """

        if self._auto_flush:
            self.close()
        return False  # never swallow exceptions

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
        "relation":  {"contains", "does_not_contain", "is_empty", "is_not_empty"},
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
        "checkbox.does_not_equal":      lambda a, b: a is not b,     

        # relation
        "relation.contains":            lambda a, b: b in [i["id"] for i in a],
        "relation.does_not_contain":    lambda a, b: b not in [i["id"] for i in a],
        "relation.is_empty":            lambda a, _: len(a) == 0,
        "relation.is_not_empty":        lambda a, _: len(a) > 0, 
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

def _extract_sort_value(page: dict, prop_name: str):
    try:
        prop = page["properties"][prop_name]
    except KeyError:
        raise ValueError(f"Sort property '{prop_name}' not found on page")

    prop_type = prop["type"]

    if prop_type in ("title", "rich_text"):
        texts = prop[prop_type]
        return (
            texts[0]["text"]["content"]
            if texts
            else EMPTY_TEXT
        )

    if prop_type == "number":
        return prop["number"]

    if prop_type == "checkbox":
        return prop["checkbox"]

    if prop_type == "date":
        date = prop.get("date")
        normalized = normalize_page_date(date)
        return normalized["start"] if normalized else None

    raise ValueError(f"Sorting not supported for property type '{prop_type}'")
