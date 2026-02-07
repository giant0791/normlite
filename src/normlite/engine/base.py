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
from typing import Any, Mapping, Optional, Sequence, TypeAlias, TYPE_CHECKING, overload
from dataclasses import dataclass
from typing import Optional, Literal, Union
from urllib.parse import urlparse, parse_qs, unquote

from normlite.engine.cursor import CursorResult
from normlite.engine.context import ExecutionContext, ExecutionStyle
from normlite.engine.interfaces import ReturningStrategy, _distill_params, IsolationLevel
from normlite.exceptions import ArgumentError, NormliteError, ObjectNotExecutableError
from normlite.engine.reflection import ReflectedColumnInfo, ReflectedTableInfo
from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.sql.compiler import NotionCompiler
from normlite.sql.ddl import HasTable, ReflectTable
from normlite.notiondbapi.dbapi2 import Connection as DBAPIConnection, Cursor as DBAPICursor, ProgrammingError
from normlite.utils import frozendict
from normlite.notion_sdk.getters import get_property, get_rich_text_property_value, get_title_property_value

if TYPE_CHECKING:
    from normlite.sql.schema import Table, HasIdentifier
    from normlite.sql.base import Executable
    from normlite.engine.interfaces import _CoreAnyExecuteParams, IsolationLevel, CompiledCacheType, ExecutionOptions

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

    _execution_options: ExecutionOptions

    def __init__(self, engine: Engine):
        self._engine = engine
        """The engine used to procure the underlying DBAPI connection."""

        # TODO: initialize the connection's execution options with the engine's one
        # self._execute_options = frozendict(**engine._execution_options)
        self._execution_options = frozendict()
        
    @property
    def connection(self) -> DBAPIConnection:
        """Provide the underlying DBAPI connection managed by this connection object."""
        return self._engine.raw_connection()

    @overload
    def execution_options(
        self,
        *,
        compiled_cache: Optional[CompiledCacheType] = ...,
        logging_token: str = ...,
        isolation_level: IsolationLevel = ..., # type: ignore
        returning_strategy: ReturningStrategy = "echo",
        preserve_rowcount: bool = False,            
        **opts: Any
    ) -> Connection:
        ...

    @overload
    def execution_options(self, **opts: Any) -> Connection:
        ...

    def execution_options(self, **opts: Any) -> Connection:
        """Update the execution options **in-place** returning the same connection's instance.
        
        .. versionadded:: 0.8.0
        """

        self._execution_options = self._execution_options | frozendict(opts)
        return self

    def get_execution_options(self) -> ExecutionOptions:
        """Return the execution options that will take effect during execution.
        
        .. versionadded:: 0.8.0

        .. seealso::
        
            :meth:`Engine.execution_execution_options`
        """
        return self._execution_options

    def execute(
            self, 
            stmt: Executable, 
            parameters: Optional[_CoreAnyExecuteParams] = None,
            *,
            execution_options: Mapping[str, Any] = None
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
        
        # delegate to statement execution trigger
        try:
            exec_method = stmt._execute_on_connection(
                self, 
                parameters, 
                execution_options=execution_options
            )
        except AttributeError as ae:
            raise ObjectNotExecutableError(stmt) from ae 
        
        return exec_method
    
    def _execute_context(
            self,
            elem: Executable,
            parameters: Optional[_CoreAnyExecuteParams] = None,
            *,
            execution_options: Mapping[str, Any]
    ) -> CursorResult:

        # 1. compile the statement
        compiler = self._engine._sql_compiler
        compiled = elem.compile(compiler)

        # 2. distill parameters
        distilled_params = _distill_params(parameters)  

        # 3. create the execution context
        ctx = ExecutionContext(
            self._engine,
            self,
            self._engine.raw_connection().cursor(),
            compiled,
            distilled_params,
            execution_options=execution_options
        )

        # 4. normalize params, options, payload
        ctx.pre_exec()
        if ctx.execution_style == ExecutionStyle.SINGLE:
            return self._execute_single(ctx)
        
        raise NotImplementedError('SINGLE execution style supported only.')

    def _execute_single(self, context: ExecutionContext) -> CursorResult:
        elem = context.invoked_stmt

        # 5. statement prepares execution
        elem._setup_execution(context)      
        
        # 6. side effects happen (HTTP)
        self._engine.do_execute(
            context.cursor, 
            context.operation, 
            context.parameters
        )

        # 7. the execution is mechanically complete before semantic interpretation begins
        #    make lastrowid, rowcount, etc. available to the result
        context.post_exec()

        # 8. result interpretation
        #    semantic reconstruction / reflection for DDL statements
        elem._finalize_execution(context)   

        return context.setup_cursor_result()

    def _resolve_execution_options(self, stmt_execution_options: ExecutionOptions) -> ExecutionOptions:
        """Resolve the connection's execution options with the statement's ones."""
        pass
    
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
        *,
        execution_options: Optional[ExecutionOptions] = None,
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
    return Engine(_parse_uri(uri), execution_options=execution_options, **kwargs)

@dataclass(frozen=True)
class SystemTablesEntry:
    name: str
    catalog: str
    schema: str
    table_id: str
    is_dropped: bool

    @classmethod
    def from_dict(cls, page_obj: dict) -> SystemTablesEntry:       
        name = get_title_property_value(
            get_property(
                page_obj, 
                'table_name'
            )            
        )

        catalog = get_rich_text_property_value(
            get_property(
                page_obj, 
                'table_catalog'
            )
        )

        schema = get_rich_text_property_value(
            get_property(
                page_obj, 
                'table_schema'
            )
        )

        table_id = get_rich_text_property_value(
            get_property(
                page_obj, 
                'table_id'
            )
        )

        is_dropped = page_obj['in_trash']

        return cls(
            name=name,
            catalog=catalog,
            schema=schema,
            table_id=table_id,
            is_dropped=is_dropped
        ) 
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

        .. versionchanged:: 0.8.0
            This version provides a revised bootstrap algorithm for the population of normlite pages/database
            that includes discovery and avoids overwriting.
            The root page is created for simulated clients only.
    """

    _execution_options: ExecutionOptions
    """Engine's execution options."""

    def __init__(
            self, uri: 
            NotionURI, 
            *,
            execution_options: Optional[ExecutionOptions] = None,
            **kwargs: Any) -> None:
        if isinstance(uri, NotionAuthURI):
            raise NotImplementedError(
                "Auth-based integrations are not supported yet."
            )

        self._uri = uri
        """The Notion URI denoting the integration to connect to."""

        self._execution_options = frozendict(execution_options or {})

        # -------------------------
        # Engine identity / config
        # -------------------------
        self._root_page_id: Optional[str] = kwargs.get("root_page_id")
        """The id for the root page in the integration."""

        self._user_database_name: Optional[str] = kwargs.get("user_database_name")
        """The database name.
        
        For simulated URIs, ``memory`` if mode is memory, the file name without extension if mode is file.
        For :class:`NotionAuthURI`, the name in the keyword argument user_database_name.

        .. versionchanged:: 0.8.0
            Renamed to make the intend clearer.
        """

        self._database: Optional[str] = None
        """The database name.
        
        .. deprecated:: 0.8.0
        """

        self._ws_id: Optional[str] = kwargs.get("ws_id")

        self._ischema_page_id: Optional[str] = None
        """Id for the information schema page."""

        self._tables_id: Optional[str] = None
        """Id for the Notion database tables."""

        self._user_tables_page_id: Optional[str] = None
        """ Page id of the parent page of all databases created in the integration.
        
        .. versionadded:: 0.8.0
        """

        self._client = None
        """The Notion client this engine interacts with."""

        self._init_client: bool = kwargs.get("init_client", True)
        """Whether the client shall be initialized with the ``normlite`` datastructures. Defaults to ``True``."""

        self._isolation_level: IsolationLevel = "AUTOCOMMIT" # type: ignore
        """Isolation level for transactions. Defaults to ``AUTOCOMMIT``."""

        self._create_client(uri)

        self._dbapi_connection = DBAPIConnection(self._client)
        """The underlying DBAPI connetion.
        
        .. versionadded:: 0.7.0
        """

        self._sql_compiler = NotionCompiler()
        """The SQL compiler.
        
        .. versionadded:: 0.7.0
        """

    @overload
    def execution_options(
        self,
        *,
        compiled_cache: Optional[CompiledCacheType] = ...,
        logging_token: str = ...,
        isolation_level: IsolationLevel = ..., # type: ignore
        returning_strategy: ReturningStrategy = "echo",
        preserve_rowcount: bool = False,            
        **opts: Any
    ) -> Engine:
        ...

    @overload
    def execution_options(self, **opts: Any) -> Engine:
        ...

    def execution_options(self, **opts: Any) -> Engine:
        """Update the execution options **in-place** returning the same engine's instance.
        
        .. versionadded:: 0.8.0
        """

        self._execution_options = self._execution_options | frozendict(opts)
        return self

    def get_execution_options(self) -> ExecutionOptions:
        """Return the execution options that will take effect during execution.
        
        .. versionadded:: 0.8.0

        .. seealso::

            :meth:`Engine.execution_execution_options`
        """
        return self._execution_options

    # -------------------------------------------------
    # Client creation + bootstrap
    # -------------------------------------------------

    def _create_client(self, uri: NotionSimulatedURI) -> None:
        if uri.mode not in ("memory", "file"):
            raise NotImplementedError

        self._client = InMemoryNotionClient()

        # Resolve database name
        if uri.mode == "memory":
            self._database = "memory"
            self._user_database_name = self._user_database_name or "memory"
            self._root_page_id = self._root_page_id or self._client._ROOT_PAGE_ID_
        else:
            self._database = Path(uri.file).stem
            self._user_database_name = (
                self._user_database_name or self._database
            )
            if not self._root_page_id:
                raise ArgumentError("root_page_id is required for file-based engines")

        if self._init_client:
            if uri.mode in ("memory", "file"):
                # root page is required for simulated clients
                self._client._ensure_root()

            self._bootstrap()

    # -------------------------------------------------
    # Bootstrap logic (idempotent)
    # -------------------------------------------------

    def _bootstrap(self) -> None:

        # 1. information_schema page
        self._ischema_page_id = self._get_or_create_page(
            parent_id=self._root_page_id,
            name="information_schema",
        )

        # 2. tables database
        self._tables_id = self._get_or_create_database(
            parent_id=self._ischema_page_id,
            name="tables",
            properties={
                "table_name": {"title": {}},
                "table_schema": {"rich_text": {}},
                "table_catalog": {"rich_text": {}},
                "table_id": {"rich_text": {}},
            },
        )

       # 3. ensure tables self-row exists
        self._ensure_sys_tables_self_row()

        # 4. user tables page
        self._user_tables_page_id = self._get_or_create_page(
            parent_id=self._root_page_id,
            name=self._user_database_name,
        )

    # -------------------------------------------------
    # Find-or-create helpers
    # -------------------------------------------------

    def _get_or_create_page(self, parent_id: str, name: str) -> str:
 
        page = self._client.find_child_page(parent_id, name)
        if page:
            return page["id"]
        
        page = self._client._add(
            "page",
            {
                "parent": {"type": "page_id", "page_id": parent_id},
                "properties": {
                    "Name": {"title": [{"text": {"content": name}}]}
                },
            },
        )

        return page['id']

    def _get_or_create_database(
        self,
        parent_id: str,
        name: str,
        properties: dict,
    ) -> str:
        db = self._client.find_child_database(parent_id, name)
        if db:
            return db["id"]

        db = self._client._add(
            "database",
            {
                "parent": {"type": "page_id", "page_id": parent_id},
                "title": [{"type": "text", "text": {"content": name}}],
                "properties": properties,
            },
        )

        return db['id']

    def _ensure_sys_tables_self_row(self) -> None:
        self._ensure_sys_tables_row(
            name='tables',
            schema='information_schema',
            catalog=self._user_database_name,
            table_id=self._tables_id 
        )

    def _ensure_sys_tables_row(
            self,
            name: str,
            schema: Optional[str] = 'not_used',
            *,
            catalog: str,
            table_id: str
    ) -> None:
        self._get_or_create_sys_tables_row(
            name,
            schema,
            table_catalog=catalog,
            table_id=table_id
        )

    # -------------------------------------------------
    # Table creation / reflection helpers 
    # -------------------------------------------------

    def _find_sys_tables_row(
        self,
        table_name: str,
        *,
        table_catalog: Optional[str] = None,
    ) -> Optional[SystemTablesEntry]:
        """Return the tables row (Notion page object) for a table or None if it does not exist."""

        catalog = table_catalog or self._user_database_name

        response = self._client.databases_query(
            path_params={
                "database_id": self._tables_id,
            },

            payload={
                "filter": {
                    "and": [
                        {
                            "property": "table_name",
                            "title": {"equals": table_name},
                        },
                        {
                            "property": "table_catalog",
                            "rich_text": {"equals": catalog},
                        },
                    ]
                },
            }
        )

        results = response.get("results", [])

        if len(results) > 1:
            raise NormliteError(
                f"Multiple tables named '{table_name}' found in catalog '{catalog}'"
            )

        return SystemTablesEntry.from_dict(results[0]) if results else None

    def _get_or_create_sys_tables_row(
            self, 
            table_name: str, 
            table_schema: Optional[str] = 'not_used',
            *,
            table_catalog: str, 
            table_id: str,
            if_exists: bool = False
    ) -> SystemTablesEntry:
        """Helper to get or create a new row in the tables system table."""
        existing = self._find_sys_tables_row(table_name, table_catalog=table_catalog)

        if existing is not None and not existing.is_dropped:
            if if_exists:
                raise ProgrammingError(
                    f"Table '{table_name}' already exists in catalog '{table_catalog}'"
                )

            return existing

        page_obj = self._client.pages_create(
            payload={
                "parent": {
                    "type": "database_id",
                    "database_id": self._tables_id,
                },
                "properties": {
                    "table_name": {
                        "title": [{"text": {"content": table_name}}]
                    },
                    "table_schema": {
                        "rich_text": [{"text": {"content": table_schema}}]
                    },
                    "table_catalog": {
                        "rich_text": [{"text": {"content": table_catalog}}]
                    },
                    "table_id": {
                        "rich_text": [{"text": {"content": table_id}}]
                    },
                },
            },
        )

        return SystemTablesEntry.from_dict(page_obj)


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
    
    # -------------------------------------------------
    # Public API
    # -------------------------------------------------

    def inspect(self) -> Inspector:
        """Return an inspector object.

        Factory method to procure :class:`Inspector` objects.
        
        .. versionadded:: 0.7.0

        """
        return Inspector(self)
    
    def connect(self) -> Connection:
        """Procure a new :class:`Connection` object."""
        return Connection(self)

    def raw_connection(self) -> DBAPIConnection:
        """Provide the underlying DBAPI connection."""
        return self._dbapi_connection
    
    #----------------------------------------------------
    # Execution context management methods
    #----------------------------------------------------

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
            This method uses the new :meth:`Engine._find_sys_tables_row` API.

        .. versionadded:: 0.7.0
            This method uses internal private helper to query the tables database and check existence.

        Args:
            table_name (str): The name of the table to search for.

        Raises:
            NormliteError: If more than one table with the same name is found in the same catalog (database).

        Returns:
            bool: ``True`` if the table exists, ``False`` otherwise. 
        """
        table_entry = self._engine._find_sys_tables_row(table_name, table_catalog=self._engine._user_database_name)
        return table_entry is not None and not table_entry.is_dropped
    
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