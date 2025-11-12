# normlite/engine/base.py 
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
from pathlib import Path
import pdb
from typing import Any, Optional, TypeAlias, TYPE_CHECKING
from dataclasses import dataclass
from typing import Optional, Literal, Union
from urllib.parse import urlparse, parse_qs, unquote
import uuid

from normlite.cursor import CursorResult
from normlite.exceptions import ArgumentError, NormliteError
from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.sql.compiler import NotionCompiler
from normlite.sql.type_api import Boolean, Number, String
from normlite.notiondbapi.dbapi2 import Connection as DBAPIConnection, IsolationLevel

if TYPE_CHECKING:
    from normlite.sql.schema import Table, Column, HasIdentifierMixin
    from normlite.sql.base import Executable

class Connection:
    """Provide high level API to a connection to Notion databases.

    This class delegates the low level implementation of its methods to the DBAPI counterpart
    :class:`dbapi2.Connection`.

    .. versionadded:: 0.7.0
    
    """

    def __init__(self, engine: Engine):
        self._engine = engine
        
    @property
    def connection(self) -> DBAPIConnection:
        return self._engine.raw_connection()

    def execute(self, stmt: Executable, parameters: Optional[dict] = None) -> CursorResult:
        """Execute an SQL statement.

        This method executes both DML and DDL statements in an enclosing (implicit) transaction.
        When it is called for the first time, it sets up the enclosing transaction.
        All subsequent calls to this method add the statements to the enclosing transaction.
        Use either :meth:`commit()` to commit the changes permanently or :meth:`rollback()` to
        rollback.

        Note:
            **Non-mutating** statements like ``SELECT`` returns their result immediately after the
            :meth:`Connection.execute()` returns. All **mutating** statements like ``INSERT`` 
            (see :class:`normlite.sql.dml.Insert`),``UPDATE`` or ``DELETE`` return an 
            **empty** result immediately.

        Important:
            The cursor result object associated with the last :meth:`Connection.execute()` contains
            a list of all result sets of the statements executed within the enclosing transaction.

        Args:
            stmt (Executable): The statement to execute.
            parameters (Optional[dict]): An optional dictionary containing the parameters to be
                bound to the SQL statement.   

        Returns:
            CursorResult: The result of the statement execution as cursor result.

        .. versionadded:: 0.7.0

        """

        return stmt._execute_on_connection(self, parameters)

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

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

        if isinstance(uri, NotionAuthURI):
            raise NotImplementedError(
                f'Neither internal nor external integration URIs are supported yet (simulated only).'
            )        

        if kwargs is None:
            raise ArgumentError('Expected keyword arguments, but none were provided.')

        self._uri = uri
        """The Notion URI denoting the integration to connect to."""

        self._database = None
        """The database name: For simulated URIs, ``memory`` if mode is memory, the file name without extension if mode is file."""

        self._db_page_id = None
        """The page id for the database this engine is connected to."""

        self._ws_id = None
        """The workspace id to which all the pages are added to."""

        self._ischema_page_id = None
        """Id for the information schema page."""

        self._tables_id = None
        """Id for the Notion database tables."""

        self._client = None
        """The Notion client this engine interacts with."""

        self._init_client = True
        """Whether the client shall be initialized with the ``normlite`` datastructures. Defaults to ``True``."""

        if 'ws_id' in kwargs:
            self._ws_id = kwargs['ws_id']

        if 'init_client' in kwargs:
            self._init_client = kwargs['init_client']

        self._isolation_level: IsolationLevel = 'AUTOCOMMIT'
        
        self._process_args(**kwargs)
        self._create_client(uri)

        self._dbapi_connection = DBAPIConnection(self._client)
            
        if self._ws_id is None:
            raise ArgumentError('Missing "ws_id" in passed keyword arguments.')
        
        self._sql_compiler = NotionCompiler()

    def _process_args(self, **kwargs: Any) -> None:
        if '_mock_ws_id' in kwargs:
            self._ws_id = kwargs['_mock_ws_id']

        if '_mock_ischema_page_id' in kwargs:
            self._ischema_page_id = kwargs['_mock_ischema_page_id']

        if '_mock_tables_id' in kwargs:
            self._tables_id = kwargs['_mock_tables_id']

        if '_mock_db_page_id' in kwargs:
            self._db_page_id = kwargs['_mock_db_page_id']

    def _create_client(self, uri: NotionSimulatedURI) -> None:
        """Provide helper method to instantiate the correct client based on the URI provided."""

        if uri.mode != 'memory':
            raise NotImplementedError
        
        self._database = uri.mode if uri.mode == 'memory' else uri.file.split('.')[0]

        # create client
        self._client = InMemoryNotionClient()

        # create information_schema page
        # Create the page 'information_schema'
        # Note: In the real Notion store, the 'parent' object is the workspace,
        # so the information schema page cannot be programmatically created via the API.
        # In the fake store the parent's page id is just random.
        if self._ws_id is None:
            self._ws_id = str(uuid.uuid4())

        if self._init_client:
            self._init_info_schema()
            self._init_tables()
            self._init_database()
      
    def inspect(self) -> Inspector:
        """Return an inspector object.
        
        .. versionadded:: 0.7.0

        """
        return Inspector(self)
    

    def _init_info_schema(self) -> None:
        # Currently it always create the information_schema page
        # In the future, it shall first check if it exists
        payload = {
            'parent': {                     
                'type': 'page_id',
                'page_id': self._ws_id
            },
            'properties': {
                'Name': {'title': [{'text': {'content': 'information_schema'}}]}
            }
        }
        self._client._add('page', payload, self._ischema_page_id)

    def _init_tables(self) -> None:
        # create the database 'tables'
        payload = {
            'parent': {
                'type': 'page_id',
                'page_id': self._ischema_page_id
            },
            "title": [
                {
                    "type": "text",
                    "text": {
                        "content": "tables",
                        "link": None
                    },
                    "plain_text": "tables",
                    "href": None
                }
            ],
            'properties': {
                'table_name': {'title': {}},
                'table_schema': {'rich_text': {}},
                'table_catalog': {'rich_text': {}},
                'table_id': {'rich_text': {}}
            }
        }
        self._client._add('database', payload, self._tables_id)

        # add tables itself to tables
        # Note: By construction, normlite always has a tables database (the table name tables) and 
        # a tables page (the row in the tables database)
        self._client._add('page', {
            'parent': {
                'type': 'database_id',
                'database_id': self._tables_id
            },
            'properties': {
                'table_name': {'title': [{'text': {'content': 'tables'}}]},
                'table_schema': {'rich_text': [{'text': {'content': 'information'}}]},
                'table_catalog': {'rich_text': [{'text': {'content': 'normlite'}}]},
                'table_id': {'rich_text': [{'text': {'content': self._tables_id}}]}
            }
        })

    def _init_database(self) -> None:
        payload = {
            'parent': {
                'type': 'page_id',
                'page_id': self._ws_id

            },
            'properties': {
                'Name': {'title': [{'text': {'content': self._database}}]}
            }
        }
        self._client._add('page', payload, self._db_page_id)

    def _add_table(self, table_name: str, catalog: str, table_id: str) -> None:
        self._client._add('page', {
            'parent': {
                'type': 'database_id',
                'database_id': self._tables_id
            },
            'properties': {
                'table_name': {'title': [{'text': {'content': table_name}}]},
                'table_schema': {'rich_text': [{'text': {'content': ''}}]},
                'table_catalog': {'rich_text': [{'text': {'content': catalog}}]},
                'table_id': {'rich_text': [{'text': {'content': table_id}}]}
            }
        })

    def connect(self) -> Connection:
        return Connection(self)

    def raw_connection(self) -> Connection:
        return self._dbapi_connection
                   
class Inspector:
    """Provide an inspector facilities for inspecting ``normlite`` objects.

    .. versionadded:: 0.7.0
    
    """
    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        """The engine it connects to."""

    def has_table(self, table_name: str) -> bool:
        """Check whether the specified table name exists in the database being inspected.

        Args:
            table_name (str): The name of the table to search for.

        Raises:
            NormliteError: If more than one table with the same name is found in the same catalog (database).

        Returns:
            bool: ``True`` if the table exists, ``False`` otherwise. 
        """

        # query all rows with the table_name in tables
        result = self._find_table_in_catalog(
            table_name,
            'normlite' if table_name == 'tables' else self._engine._database
        )

        if len(result) > 1:
            raise NormliteError(f'Internal error. Found: {len(result)} {table_name} in catalog: {self._engine._database}')

        if len(result) == 0:
            return False
        
        return True
            
    def reflect_table(self, table: Table) -> None:
        """Construct a table by reflecting it from the database.

        Args:
            table (Table): The table object to reflect.

        Raises:
            NormliteError: If more than one table exists in the database or if the table does not exist.
            NormliteError: If an unsupported property type is detected during reflection.
        """
        from normlite.sql.schema import Column

        # 1. Get the table id by looking up the table name
        pages = self._find_table_in_catalog(
            table.name, 
            'normlite' if table.name == 'tables' else self._engine._database
        )

        if len(pages) != 1:
            raise NormliteError(f'Reflection of multiple or non existing tables not supported yet: {pages}')
        
        table_id_prop = pages[0]['properties']['table_id']
        table._database_id = table_id_prop['rich_text'][0]['text']['content']

        # 2. Get the Notion database object
        database = self._engine._client.databases_retrieve({'id': table._database_id})

        # 3. Construct the table by parsing the properties
        for col_name, col_spec in database['properties'].items():
            col_type = col_spec['type']
            col = None
            if col_type in ['title', 'rich_text']:
                col = Column(col_name, String(is_title=(col_type == 'title')))
            elif col_type == 'number':
                format = col_spec.get('format')
                col = Column(col_name, Number(format)) if format else Column(col_name, Number('number'))
            elif col_type == 'checkbox':
                col = Column(col_name, Boolean())
            else:
                raise NormliteError(f'Unsupported property type during table reflection: {col_spec}')
            
            table.append_column(col)

        # 4. ensure implicit columns are created
        table._ensure_implicit_columns()

        # 5. reflect primary keys
        # TODO: Not implemented yet

        # 6. reflect foreign keys
        # TODO: Not implemented yet

    def get_id(self, has_id: HasIdentifierMixin) -> str:
        """Return the Notion object id for the supplied table.

        Args:
            table (Table): The table object to get the object id from.
        """

        return has_id.get_oid()

    def _find_table_in_catalog(self, name: str, catalog: str) -> dict:
        return self._engine._client.databases_query({
            'database_id': self._engine._tables_id,
            'filter': {
                'and': [
                    {
                        'property': 'table_name',
                        'title' : {
                            'equals': name
                        }
                    },
                    {
                        'property': 'table_catalog',
                        'rich_text': {
                            'equals': catalog
                        }
                    }
                ]
            }
        })