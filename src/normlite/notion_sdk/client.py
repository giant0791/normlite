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
from typing import List, Optional, Self, Set, Type
from types import TracebackType
from abc import ABC, abstractmethod
import uuid
from datetime import datetime
import random
import string
import urllib.parse

from normlite.notion_sdk.getters import get_object_type, get_property_type, get_title
from normlite.notion_sdk.types import normalize_filter_date, normalize_page_date

class NotionError(Exception):
    """Exception raised for all errors related to the Notion REST API."""
    def __init__(
        self,
        message: str,
        *,
        status_code: int = 400,
        code: str = "validation_error",
    ) -> None:
        self.status_code = status_code
        self.code = code
        self.message = message

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
        if parent_type not in ("page_id", "database_id"):
            raise NotionError(
                f'Body failed validation: body.parent.type should be "page_id" or '
                f'"database_id", instead "{parent_type}" was defined.'
            )

        if type_ == "database" and not payload.get("title"):
            raise NotionError(
                "Body failed validation: body.title should be defined for database object."
            )

        if "properties" not in payload:
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

        name, prop = next(iter(props.items()))
        if "title" not in prop or len(prop) != 1:
            raise NotionError(
                'New page is a child of a page. "title" is the only valid property.'
            )

        prop["type"] = "title"
        prop["id"] = "title"

    def _finalize_page_under_database(self, page: dict, database: dict) -> None:
        schema_props = database["properties"]
        page_props = page["properties"]

        if set(page_props.keys()) != set(schema_props.keys()):
            raise NotionError(
                f"Page properties must exactly match database schema: "
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

    # ------------------------------------------------------------------
    # Database finalization
    # ------------------------------------------------------------------

    def _finalize_database(self, db: dict) -> None:
        parent_type, _ = self._resolve_parent(db)
        if parent_type != "page":
            raise NotionError("Databases can only be created under pages.")

        for name, prop in db["properties"].items():
            prop_type = next(iter(prop.keys()))
            prop["type"] = prop_type
            prop["id"] = "title" if prop_type == "title" else self._generate_property_id()

        db["is_inline"] = False

    # ------------------------------------------------------------------
    # Normalization helpers
    # ------------------------------------------------------------------

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

    def _normalize_property(self, prop: dict) -> dict:
        prop_type = prop.get("type")
        if not prop_type:
            raise NotionError("Property missing 'type'")

        if prop_type == "title":
            if "title" not in prop:
                raise NotionError("Title property missing 'title' field")
            prop["title"] = self._normalize_rich_text(prop["title"])

        elif prop_type == "rich_text":
            if "rich_text" not in prop:
                raise NotionError("rich_text property missing 'rich_text' field")
            prop["rich_text"] = self._normalize_rich_text(prop["rich_text"])

        return prop

    def _normalize_properties(self, obj: dict) -> None:
        props = obj.get("properties")
        if not isinstance(props, dict):
            raise NotionError("Object missing properties")

        for name, prop in props.items():
            props[name] = self._normalize_property(prop)

    def _normalize_database_title(self, db: dict) -> None:
        title = db.get("title")
        if not isinstance(title, list):
            raise NotionError("Database title must be a rich_text list")

        db["title"] = self._normalize_rich_text(title)

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
                self._finalize_page_under_database(obj, parent_obj)

            self._normalize_properties(obj)
                

        elif type_ == "database":
            self._finalize_database(obj)
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

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    def pages_create(self, path_params=None, query_params=None, payload=None) -> dict:
        return self._add("page", payload)

    def pages_retrieve(self, path_params=None, query_params=None, payload=None) -> dict:
        page_id = path_params.get("page_id")
        obj = self._get_by_id(page_id)
        if not obj:
            raise NotionError(
                f"Could not find page with ID: {page_id}. "
                "Make sure the relevant pages and databases are shared with your integration."
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
                "Make sure the relevant pages and databases are shared with your integration."
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

        database_id = path_params.get('database_id') if path_params else None
        if database_id is None:
            raise NotionError('Invalid request URL: database_id should be defined.')

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
        # Projection phase
        # --------------------
        for page in pages:
            if filter_properties:
                query_results.append(
                    self._filter_properties(page, filter_properties)
                )
            else:
                query_results.append(page)

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
    STORE_VERSION = 1

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
            Optional[bool]: ``False`` as it is customary for context managers.
        """

        if self._auto_flush:
            self.flush()
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
