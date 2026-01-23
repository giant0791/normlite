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
"""Establish the foundational engine layer for connecting to Notion or 
simulated integrations (:memory:, file-based).

Here a quick example of how to use :class:`Engine` and :class:`Connection`.

.. code-block:: python

    from datetime import datetime
    from normlite import create_engine, Engine, Connection
    from normlite import MetaData, Column, Table, CreateTable
    from normlite import insert, Insert

    # create the engine to interact with Notion (use in-memory client version)
    engine: Engine = create_engine('normlite///:memory')
    
    # create database schema
    metadata = MetaData()
    students = Table(
        'students',
        metadata,
        Column('name', String(is_title=True)),
        Column('grade', String()),
        Column('student_id', Number()),
        Column('registered_since', Date()),
        Column('active', Boolean())
    )

    # connect to the Notion store
    with engine.connect() as conn:
        # create DDL CREATE TABLE statement and
        # create the table students
        ddl_stmt: CreateTable = CreateTable(students)
        _ = conn.execute(ddl_stmt)

        # add some rows
        dml_stmt: Insert = insert(students).values(
            name='Galileo Galilei',
            grade='A',
            student_id=123456,
            registered_since=(datetime(1580, 09, 11), ),
            active=False
        )
        result: CursorResult = conn.execute(dml_stmt)

        # access data returned by the DML statement excution
        row = result.one()
        print(f'{row[SpecialColumns.NO_ID]}')             # '680dee41-b447-451d-9d36-c6eaff13fb45'  
"""

from __future__ import annotations
from pathlib import Path
import pdb
from typing import Any, Optional, Sequence, TypeAlias, TYPE_CHECKING
from dataclasses import dataclass
from typing import Optional, Literal, Union
from urllib.parse import urlparse, parse_qs, unquote
import uuid

from normlite.engine.cursor import CursorResult
from normlite.engine.context import DMLExecContex, ExecutionContext
from normlite.engine.interfaces import _distill_params
from normlite.exceptions import ArgumentError, ObjectNotExecutableError
from normlite.future.engine.reflection import ReflectedColumnInfo, ReflectedTableInfo
from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.sql.base import Compiled
from normlite.sql.compiler import NotionCompiler
from normlite.sql.ddl import HasTable, ReflectTable
from normlite.notiondbapi.dbapi2 import Connection as DBAPIConnection, IsolationLevel, Cursor as DBAPICursor

if TYPE_CHECKING:
    from normlite.sql.schema import Table, HasIdentifier
    from normlite.sql.base import Executable
    from normlite.engine.interfaces import _CoreAnyExecuteParams

class Connection:
    """Provide high level API to a connection to Notion databases.

    This class delegates the low level implementation of its methods to the DBAPI counterpart
    :class:`dbapi2.Connection`.
    The :class:`Connection` object is procured by calling the :meth:`Engine.connect()` method of
    the :class:`Engine` class, and provides services for execution of SQL statements.

    .. versionadded:: 0.7.0
        This version implements the ``autocommit`` isolation level.
        The transaction management methods like :meth:`commit()` and :meth:`rollback()` have been
        added but do nothing. 
    
    """

    def __init__(self, engine: Engine):
        self._engine = engine
        """The engine used to procure the underlying DBAPI connection."""
        
    @property
    def connection(self) -> DBAPIConnection:
        """Provide the underlying DBAPI connection managed by this connection object."""
        return self._engine.raw_connection()

    def execute(
            self, 
            stmt: Executable, 
            parameters: Optional[_CoreAnyExecuteParams] = None
    ) -> CursorResult:
        """Execute an SQL statement.

        This method executes both DML and DDL statements in an enclosing (implicit) transaction.
        When it is called for the first time, it sets up the enclosing transaction.
        All subsequent calls to this method add the statements to the enclosing transaction.
        Use either :meth:`commit()` to commit the changes permanently or :meth:`rollback()` to
        rollback.

        Note:
            **Non-mutating** statements like ``SELECT`` returns their result immediately after the
            :meth:`Connection.execute()` returns. All **mutating** statements like ``INSERT`` 
            (see :class:`normlite.sql.dml.Insert`), ``UPDATE`` or ``DELETE`` return an 
            **empty** result immediately.

 
       Important:
            The cursor result object associated with the last :meth:`Connection.execute()` contains
            a single result set of the last statement executed.

        Args:
            stmt (Executable): The statement to execute.
            parameters (Optional[d_CoreAnyExecuteParams]): An optional mapping or sequence of mappings containing the parameters to be
                bound to the SQL statement.   

        Returns:
            CursorResult: The result of the statement execution as cursor result.

        .. versionchanged: 0.8.0

        .. versionadded:: 0.7.0

        """
        
        # normalize parameters to sequence of mappings
        distilled_params = _distill_params(parameters)

        # delegate to statement execution trigger
        try:
            exec_method = stmt._execute_on_connection(self, distilled_params)
        except AttributeError as ae:
            raise ObjectNotExecutableError(stmt) from ae 
        
        return exec_method


    def commit(self) -> None:
        """Commit the transaction currently in progress.
        
        Note:
            Work in progress. This method currently does nothing.
        """
        pass

    def rollback(self) -> None:
        """Rollback the transaction currently in progress.
        
        Note:
            Work in progress. This method currently does nothing.
        """
        pass

    def __enter__(self) -> Connection:
        return self
    
    def __exit__(self, type_: Any, value: Any, traceback: Any) -> None:
        pass

@dataclass
class NotionAuthURI:
    """Provide a helper data structure to hold URI schema elements for an internal or external Notion integration.
    
    Warning:
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

    .. code-block:: python

        from normlite import create_engine

        # procure an Engine object for connecting to a Notion internal integration
        NOTION_TOKEN = 'secret-token'
        NOTION_VERSION = '2022-06-28'

        engine: Engine = create_engine(
            f'normlite+auth://internal?token={NOTION_TOKEN}&version={NOTION_VERSION}'
        )  

    Args:
        uri (str): The URI denoting the integration to connect to.

    Returns:
        Engine: The engine proxy object.
    """
    return Engine(_parse_uri(uri), **kwargs)

class Engine:
    """Provide a convenient proxy object of database connectivity to Notion integrations.

    An :class:`Engine` object is instantiated by the factory :func:`create_engine()`.

    Examples of possible future extensions:
    
        >>> # create a proxy object to a :memory: integration
        >>> engine = create_engine('normlite::///:memory:')

        >>> # create a proxy object to a Notion internal integration
        >>> NOTION_TOKEN = 'secret-token'
        >>> NOTION_VERSION = '2022-06-28' 
        >>> engine = create_engine(f'normlite+auth://internal?token={NOTION_TOKEN}&version={NOTION_VERSION}')
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
        """Isolation level for transactions. Defaults to ``AUTOCOMMIT``."""
        
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

        Factory method to procure :class:`Inspector` objects.
        
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
        """Procure a new :class:`Connection` object."""
        return Connection(self)

    def raw_connection(self) -> Connection:
        """Provide the underlying DBAPI connection."""
        return self._dbapi_connection
    
    def _check_if_exists(self, table_name: str) -> HasTable:
        """Helper method to check for existence of a table."""
        # execute the DDL statement
        with self.connect() as connection:
            hastable = HasTable(
                table_name, 
                self._tables_id, 
                'normlite' if table_name == 'tables' else self._database
            )

            # result is not needed, the check result
            # can be queried using the found() method 
            _ = connection.execute(hastable)

        return hastable
        
    def _reflect_table(self, table: Table) -> ReflectedTableInfo:
            has_table = self._check_if_exists(table.name)
            if has_table.found():
                with self.connect() as connection:
                    table.set_oid(has_table.get_oid())
                    reflect_table = ReflectTable(table)
                    _ = connection.execute(reflect_table)

            else:
                raise ArgumentError(f"No table found with name: {table.name}")
            
            return reflect_table._as_info()
    
    #----------------------------------------------------
    # Execution context management methods
    #----------------------------------------------------

    def _create_execution_context(self, stmt: Executable, compiled: Compiled) -> ExecutionContext:
        if compiled.is_ddl:
            return DMLExecContex(
                cursor=self._dbapi_connection.cursor(),
                statement=stmt,
                compiled=compiled
            )
        else:
            pass

    def do_execute(
            self,
            cursor: DBAPICursor,
            operation: dict,
            parameters: dict,
    ) -> None:
        cursor.execute(operation, parameters)
                       
class Inspector:
    """Provide facilities for inspecting database objects.

    The :class:`Inspector` acts as a proxy for to the classes inspection facilities, 
    providing a consitent interface.

    .. versionadded:: 0.7.0
    
    """
    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        """The engine it connects to."""

    def get_columns(self, table_name: str) -> Sequence[ReflectedColumnInfo]:
        from normlite.sql.schema import Table, MetaData
        metadata = MetaData()
        table = Table(table_name, metadata)
        database_info = self._engine._reflect_table(table)
        return database_info.get_columns()

    def has_table(self, table_name: str) -> bool:
        """Check whether the specified table name exists in the database being inspected.

        This method queries the INFORMATION_SCHEMA.tables database to check existence of the
        requested table.

        .. versionchanged:: 0.8.0
            This method uses the new class :class:`normlite.sql.ddl.HasTable` as executable construct.

        .. versionadded:: 0.7.0
            This method uses internal private helper to query the tables database and check existence.

        Args:
            table_name (str): The name of the table to search for.

        Raises:
            NormliteError: If more than one table with the same name is found in the same catalog (database).

        Returns:
            bool: ``True`` if the table exists, ``False`` otherwise. 
        """
        hastable = self._engine._check_if_exists(table_name)
        return hastable.found()
    
    def reflect_table(self, table: Table) -> ReflectedTableInfo:
        return self._engine._reflect_table(table)
            
    def get_oid(self, has_id: HasIdentifier) -> str:
        """Return the Notion object id for the supplied table.

        This method takes a :py:class:`normlite.sql.schema.HasIdentifier` object and returns
        the Notion identifier associated with the Notion object.
        
        .. code-block:: python
        
            # reflect the Notion database into a Table object
            inspector = engine.inspect()
            metadata = MetaData()
            students: Table = Table('students', metadata)
            inspector.reflect_table(students)

            # access the Notion database object identifier
            print(f'Object ID: "{inspector.get_oid(students)}"')     # uuid string

            # access the Notion property identifier associate to a column
            print(
                f'Property "{students.c.student_name.name}": '
                f'"{inspector.get_oid(students)}"')                  # uuid string associated to the property

        .. versionadded:: 0.7.0
            The current version supports both Notion object and property identifiers.

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