# engine.py
# Copyright (C) 2009-2025 Gianmarco Antonini
#
# This module is part of normlite and is released under
# the <tbd> License
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
from typing import Dict, Any, List, Optional, TypeAlias
from dataclasses import dataclass
from typing import Optional, Literal, Union
from urllib.parse import urlparse, parse_qs, unquote
import os

from normlite.notion_sdk.client import FakeNotionClient

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
) -> Engine:
    """Create a new engine proxy object to connect and interact to the Notion integration denoted by the supplied URI.

    This is a factory function to create :class:``Engine`` proxy object based on the parameters 
    specified in the supplied URI.

    Args:
        uri (str): The URI denoting the integration to connect to.

    Returns:
        Engine: The engine proxy object.
    """
    return Engine(_parse_uri(uri))

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
    def __init__(self, uri: NotionURI) -> None:
        if isinstance(uri, NotionAuthURI):
            raise NotImplementedError(f'External integration URIs not supported yet (only internal).')

        self._uri = uri
        """The Notion URI denoting the integration to connect to."""
        
        self._database = uri.mode if uri.mode == 'memory' else uri.file.split('.')[0]
        """The database name: ``'memory'`` if mode is memory, the file name without extension if mode is file."""

        # TODO: Refactor FakeNotionClient to InMemoryNotionClient (or similar)
        # and add the api version as init parameter, example:
        # self.client = InMemoryNotionClient(self.uri.token, self._uri.version)
        self.client =  self._create_sim_client(self._uri)
        page: Dict[str, Any] = self.client.pages_create({
            "Title": {"id": "title", "title": self._database}
        })

        self._database_id = page.get('id')

    def _create_sim_client(self, uri: NotionSimulatedURI) -> FakeNotionClient:
        """Provide helper method to instantiate the correct client based on the URI provided."""

        # TODO: remove api_key parameter from FakeNotionClient __init__ method
        # TODO: remove hard coded ischema_page_id
        return FakeNotionClient('', '680dee41-b447-451d-9d36-c6eaff13fb46')
    
    @property
    def database(self) -> str:
        # TODO: Refactor to return self.
        return self._database
    
    @property
    def database_id(self) -> str:
        return self._database_id

