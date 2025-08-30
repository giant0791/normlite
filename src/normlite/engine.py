# engine.py 
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

"""Provide factory function and convenient engine proxy object to connect and interact to Notion integrations.

Examples of supported use cases:
    >>> NOTION_TOKEN = "<secret-token>"
    >>> NOTION_VERSION = "2022-06-28"
    >>> # create an engine to connect to an internal integration
    >>> engine = create_engine(f"normlite+auth://internal?token={NOTION_TOKEN}&version={NOTION_VERSION}")

    >>> # create an engine to connect to an in-memory test integration:
    >>> engine = create_engine("normlite:///:memory:")

    >>> # create an engine to connect to a file-backed test integration 
    >>> engine = create_engine("normlite:///path/to/my/test-integration.db")

Experimental use case (**incomplete** and **not tested**):
    >>> NOTION_CLIENT_ID = "<client_id>"
    >>> NOTION_CLIENT_SECRET = "<client_secret>"
    >>> NOTION_AUTH_URL = "<auth_url>"
    >>> # create an engine to connect to an external integration (experimental!!!)
    >>> engine = create_engine(
    >>>    f"normlite+auth://external?client_id={NOTION_CLIENT_ID}"
    >>>    f"&client_secret={NOTION_CLIENT_SECRET}"
    >>>    f"&auth_url={NOTION_AUTH_URL}"
    >>> )
"""
from __future__ import annotations
from pathlib import Path
import pdb
from typing import Dict, Any, List, Optional, TypeAlias
from dataclasses import dataclass
from typing import Optional, Literal, Union
from urllib.parse import urlparse, parse_qs, unquote
import os

from normlite.exceptions import ArgumentError
from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.sql.schema import Table

@dataclass
class NotionAuthURI:
    """Provide a helper data structure to hold URI schema elements for an internal or external Notion integration.
    
    Important:
        Experimental code! Do not use!
    """
    kind: Literal["internal", "external"]
    token: Optional[str] = None
    version: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    auth_url: Optional[str] = None


@dataclass
class NotionSimulatedURI:
    """Provide an a helper data structure to hold URI schema elements for test integrations."""
    kind: Literal["simulated"]
    """The kind of the integration."""
    
    mode: Literal["memory", "file"]
    """The mode the integration."""

    path: Optional[str] = None 
    """The path to the database file (``None`` for in-memory integrations)."""
    
    file: Optional[str] = None
    """The database file name (``None`` for in-memory integrations).""" 

NotionURI: TypeAlias = Union[NotionAuthURI, NotionSimulatedURI]
"""Type for the URI."""

def _parse_uri(uri: str) -> NotionURI:
    """Provide helper function to parse a normlite URI."""
    parsed = urlparse(uri)

    if not parsed.scheme in ["normlite+auth", "normlite"]:
        raise ValueError(f"Unsupported URI scheme: {parsed.scheme}")

    host = parsed.hostname
    path = unquote(parsed.path)

    if host in ("internal", "external"):
        params = parse_qs(parsed.query)
        def get_param(name: str) -> Optional[str]:
            return params.get(name, [None])[0]

        if host == "internal":
            return NotionAuthURI(
                kind="internal",
                token=get_param("token"),
                version=get_param("version")
            )
        elif host == "external":
            return NotionAuthURI(
                kind="external",
                client_id=get_param("client_id"),
                client_secret=get_param("client_secret"),
                auth_url=get_param("auth_url")
            )

    elif host is None:
        # Simulated integrations
        if path == "/:memory:":
            return NotionSimulatedURI(
                kind="simulated",
                mode="memory",
                path=None,
                file=None
            )
        elif path.startswith("/"):
            return NotionSimulatedURI(
                kind="simulated",
                mode="file",
                path=str(Path(path).absolute()),
                file=Path(path).name
            )
        else:
            raise ValueError(f"Invalid simulated integration path: {path}")

    else:
        raise ValueError(f"Unsupported notion integration type: {host}")

def create_engine(
        uri: str,
        **kwargs: Any
) -> Engine:
    """Create a new engine proxy object to connect and interact to the Notion integration denoted by the supplied URI.

    This is a factory function to create :class:``Engine`` proxy object based on the parameters 
    specified in the supplied URI.

    Args:
        uri (str): The URI denoting the integration to connect to.

    Returns:
        Engine: The engine proxy object.
    """
    return Engine(_parse_uri(uri), **kwargs)

class Engine:
    """Provide a convenient proxy object to connect and interact with Notion integrations.

    Note:
        In future versions, this class will be the proxy for handling different kind of clients.

    Examples of possible future extensions:
    
        >>> # create a proxy object to a :memory: integration
        >>> engine = create_engine('normlite::///:memory:')
        >>> isinstance(engine.client, InMemoryNotionClient)
        True 
    """
    def __init__(self, uri: NotionURI, **kwargs: Any) -> None:
        #pdb.set_trace()

        if kwargs is None:
            raise ArgumentError('Expected keyword arguments, but none were provided.')

        self._uri = uri
        """The Notion URI denoting the integration to connect to."""
        
        self._database = uri.mode if uri.mode == 'memory' else uri.file.split('.')[0]
        """The database name: ``memory`` if mode is memory, the file name without extension if mode is file."""

        self._ws_id = None
        """The workspace id to which all the pages are added to."""

        self._db_parent_id = None
        """Parent id for the current database."""

        self._database_id = None
        """The id of the current database."""

        self._ischema_page_id = None
        """Id for the information schema page."""

        self._tables_id = None
        """Id for the Notion database tables."""

        if 'ws_id' in kwargs:
            self._ws_id = kwargs['ws_id']

        if isinstance(uri, NotionAuthURI):
            raise NotImplementedError(
                f'Neither internal nor external integration URIs are supported yet (simulated only).'
            )
        
        if isinstance(uri, NotionSimulatedURI):
            if uri.mode == 'memory':
                if '_mock_ws_id' in kwargs:
                    self._ws_id = kwargs['_mock_ws_id']

                if '_mock_db_parent_id' in kwargs:
                    self._database_id = kwargs['_mock_db_parent_id']

                if '_mock_ischema_page_id' in kwargs:
                    self._ischema_page_id = kwargs['_mock_ischema_page_id']

                if '_mock_tables_id' in kwargs:
                    self._tables_id = kwargs['_mock_tables_id']
 
                self._client = InMemoryNotionClient(self._ws_id, self._ischema_page_id, self._tables_id)

            if uri.mode == 'file':
                raise NotImplementedError
            
        if self._ws_id is None:
            raise ArgumentError('Missing "ws_id" in passed keyword arguments.')

        page_for_database = self._get_page_for_database(self._database)
        self._db_parent_id = page_for_database.get('id')

    def _create_sim_client(self, uri: NotionSimulatedURI) -> InMemoryNotionClient:
        """Provide helper method to instantiate the correct client based on the URI provided."""
        return InMemoryNotionClient(self._ws_id, self._ischema_page_id, self._tables_id)
       
    def inspect(self) -> Inspector:
        return Inspector(self)
    
    def _get_page_for_database(self, name: str) -> dict:
        # lookup the page: open an existing database
        page_for_database = self._client._get_by_title(name, 'page')
        if page_for_database:
            # found the database page
            return page_for_database
        
        # no existing pages found: create a new database
        payload = {
            'parent': {
                'type': 'page_id',
                'page_id': self._ws_id  
            },
            'properties': {
                'Name': {'title': [{'text': {'content': name}}]}
            }
        }

        return self._client._add('page', payload)
        
class Inspector:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def has_table(self, table_name: str) -> bool:
        pass

    def reflect_table(self, table: Table) -> None:
        # 1. Get the table id by looking up the table name
        page = self._engine._client.databases_
        if not page:
            # table not found, it cannot be reflected

        

        # 2. Get the Notion database object
        # 3. Construct the table by parsing the properties

