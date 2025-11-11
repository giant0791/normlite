# notiondbapi/dbapi2.py
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
import copy
import pdb
from typing import Any, Dict, Iterator, List, Literal, Optional, Self, Sequence, Tuple, TypeAlias, Union
import uuid
from flask.testing import FlaskClient

from normlite.notion_sdk.client import AbstractNotionClient, NotionError
from normlite.notiondbapi._model import NotionDatabase, NotionPage
from normlite.notiondbapi._parser import parse_database, parse_page
from normlite.notiondbapi._visitor_impl import ToDescVisitor, ToRowVisitor

DBAPIParamStyle = Literal[
    'qmark',     
    'numeric',   
    'named',     
    'format',
    'pyformat'
]
"""Public type for param style used by the cursor."""

DBAPIExecuteParameters: TypeAlias = dict     # in future, Union[dict, Sequence[dict]] for multi execute params
"""Type for parameters passed to for SQL statement execution."""

class Error(Exception):
    """Base class of all other error exceptions.

    It can be used this to catch all errors with one single ``except`` statement. 
    """
    pass

class InterfaceError(Error):
    """Exception raised for errors that are related to the database interface.

    For example, it is raised when an operation is not supported by the Notion API.
    """

class DatabaseError(Error):
    """Exception raised for errors that are related to the database. 
    """

class InternalError(Error):
    """Exception raised when the database encounters an internal error.

    For example, the cursor is not valid anymore, an unexpected object has been returned by
    the underlying database driver (i.e. the Notion API).
   """
    
class OperationalError(DatabaseError):
    """Exception raised for errors that are related to the database’s operation and not necessarily under 
    the control of the programmer.

    Example situations are an unexpected disconnect occurs, the data source name is not found, 
    a transaction could not be processed, a memory allocation error occurred during processing, etc. 

    .. versionadded:: 0.7.0

    """

class Cursor:
    """Provide database base cursor functionalty according to the DBAPI 2.0 specification (PEP 249).
    
    Note:
        the :class:`Cursor` does not support transaction awareness. Use :class:`CompositeCursor` for fully
        DBAPI 2.0 compliant cursor.

    .. versionadded:: 0.7.0

    """
    def __init__(self, client: AbstractNotionClient):
        self._client = client
        """The client implementing the Notion API."""

        self._result_set = None
        """The result set returned by the last :meth:`.execute()`. It is set by :meth:`._parse_result_set()`"""
        
        self._paramstyle: DBAPIParamStyle = 'named'
        """The default parameter style applied."""

        self._description: tuple = None
        """Provide information describing one result column."""

        self._closed = False
        """Whether this cursor is closed. Always ``False`` after initialitation."""

    @property
    def description(self) -> tuple:
        """Provide the cursor description.
        
        This read-only attribute is a sequence of 7-item sequences.   
        
        Each of these sequences contains information describing one result column:

        * ``name``: The column name.

        * ``type_code``: The type code used to map the Notion to the Python type system. Currently, it is just a string.

        * ``display_size``: Not used. Always ``None``.
        
        * ``internal_size``: Not used. Always ``None``.

        * ``precision``: Not used. Always ``None``.
        
        * ``scale``: Not used. Always ``None``.

        * ``null_ok``: Not used. Always ``None``.

        This attribute will be ``None`` for operations that do not return rows or if the cursor has not had an operation invoked via the 
        :meth:`execute()` or :meth:`executemany()` methods yet.
        """
        return self._description
    
    @property
    def rowcount(self) -> int:
        """This read-only attribute specifies the number of rows that 
           the last :meth:`.execute()` produced.

        Returns:
            int: Number of rows. `-1` if in case no :meth:`.execute()` has been performed 
                 on the cursor or the rowcount of the last operation cannot be 
                 determined by the interface.
        """
        return -1 if self._result_set is None else len(self._result_set)
    
    @property
    def lastrowid(self) -> Optional[int]:
        """This read-only attribute provides the rowid of the last modified row.

        Most Notion API calls return an object with an id, which is used as rowid. 
        If the operation does not set a rowid, this attribute is set to ``None``.

        Note:
            ``normlite`` considers both inserted and updated rows as modified rows.
            This means that :attr:`.lastrowid` returns non ``None`` values after either
            an ``INSERT`` or ``UPDATE`` statement.

            ``normlite`` also defines semantics of ::attr::`.lastrowid` in case the last executed 
            statement modified more than one row, e.g. when using ``INSERT`` with :meth:`.executemany()` or
            ``UPDATE`` and its ``SELECT`` clause returns multiple rows.
            
            :attr:`.lastrowid` returns a 128-bit integer representation of the object id, which can be 
            used to driectly access Notion objects.

        Example:
            >>> object_id = str(uuid.UUID(int=cursor.lastrowid))
            >>> print(object_id)
            680dee41-b447-451d-9d36-c6eaff13fb46

        Returns:
            Optional[int]: A 128-bit integer representing the UUID object id or `None`. 
        """
        if not self._result_set:
            # Either result set is empty or semantics is undefined (example: result set is None)
            return None
        
        # extract the object UUID, 2nd element of the last row as str
        lastrowid = self._result_set[-1][0]   
        
        return uuid.UUID(lastrowid).int

    @property
    def paramstyle(self) -> DBAPIParamStyle:
        """String constant stating the type of parameter marker formatting expected by the interface. 
            
        
        Supported values are:
            ``named``: Named mark style, e.g. ``WHERE name=:name``

        Note
            It always returns ``named``.

        Returns:
            DBAPIParamStyle: The paramstyle currently in use.
        """
        return self._paramstyle

    def _parse_object(self, obj: Dict[str, Any]) -> Tuple[tuple]:
        """Parse a Notion database or page from a list object into a list of tuples.

        Examples:
            >>> # parse database object returned from databases.create
            >>> row = cursor._parse_object({
            >>>     "object": "database",
            >>>     "id": "bc1211ca-e3f1-4939-ae34-5260b16f627c",
            >>>     "title": [{
            >>>     "type": "text",
            >>>     "text": {"content": "students"}
            >>>     }],
            >>>     "properties": {
            >>>         "id": {"id": "evWq", "name": "id", "type": "number", "number": {}},
            >>>         "name": {"id": "title", "name": "name", "type": "title", "title": {}},
            >>>         "grade": {"id": "V}lX", "name": "grade", "type": "rich_text", "rich_text": {}},
            >>>     },
            >>> })
            >>> print(row)
            ('database', 'bc1211ca-e3f1-4939-ae34-5260b16f627c', 
            None, None                     # "archived" and "in_trash" missing
            'students',                    # database name
            'id', 'evWq', 'number', {},    # column metadata: <col_name>, <col_id>, <col_type>, <col_val>
            'name', 'title', 'title', {}),
            'grade','V}lX', rich_text', {})

            >>> # parse page object returned from pages.create  

        Args:
            obj (Dict[str, Any]): _description_

        Raises:
            ValueError: If ``'object'`` is neither ``'page'`` or ``'database'``.

        Returns:
            Tuple[str, str, Any]: The tuple representing the successfully parsed row.
        """
        object_ = obj['object']
        if not object_ in ['page', 'database']:
            raise ValueError(
                f'Unexpected object: {object_}. '
                'Only "page" or "database" objects supported.'
            )
    
        oid = obj.get('id', None)
        if not oid:
            raise InterfaceError(f'Missing object id in: {obj}')
        
        row_visitor = ToRowVisitor()
        desc_visitor = ToDescVisitor()
        if object_ == 'page':
            page: NotionPage = parse_page(obj)
            row = page.accept(row_visitor)
            self._description = page.accept(desc_visitor)
        elif object_ == 'database':
            database: NotionDatabase = parse_database(obj)
            row = database.accept(row_visitor)
            self._description = database.accept(desc_visitor)
         
        else:
            raise InterfaceError(f'Expected "page" or "database", received: "{object_}"')

        return row

    def _parse_result_set(self, returned_obj: Dict[str, Any]) -> None:
        """Parse the JSON object returned by the command execution into the cursor's result set.
        """
        if returned_obj['object'] == 'list':
            # Multiple objects returned by the API call, example: databases.query.
            # Parsing requires constructing a list of rows
            results_as_json = returned_obj.get("results", [])

        else:
            # Single page or database object returned by the API calls: 
            # pages.create, pages.query, databases.create, databases.query with filters
            results_as_json = [returned_obj]

        # Parse the objects in the result set
        self._result_set: List[tuple] = []
        for page_or_database in results_as_json:
            try: 
                row = self._parse_object(page_or_database)
                self._result_set.append(row)
            except ValueError as ve:
                raise InterfaceError(
                    f'Unable to parse object: {page_or_database}, '
                    f'{ve}'
                ) from ve
        
    def __iter__(self) -> Iterator[tuple]:
        """Make cursors compatible with the iteration protocol.

        .. versionchanged:: 0.5.0
            Calling this method on a closed cursor raises the :exc:`Error`.

        Raises:
            Error: If the cusors is closed.

        Yields:
            Iterator[Iterable[tuple]]: The next row in the result set.
        """
        if self._closed:
            raise Error(
                'This cursor is closed. '
                'Cannot fetch rows or execute operations on a closed cursor'
            )

        while self._result_set:
            next_row = self._result_set.pop(0)
            yield next_row

    def fetchone(self) -> Optional[tuple]:
        """Fetch the next row of a query result set.

        This method returns the next row or ``None`` when no more data is available.

        Note:    
            The current implementation guarantees that a call to this method will only move 
            the associated cursor forward by one row.

        .. versionchanged:: 0.5.0
            Calling this method on a closed cursor raises the :exc:`Error`.

        Raises:
            Error: If the cusors is closed.
            InterfaceError: If the previous call to :meth:`.execute()` did not produce any result set
                            or no call was issued yet.

        Returns:
            Optional[tuple]: The next row as single tuple, or an empty tuple when no more data is available.
        """
        if self._closed:
            raise Error(
                'This cursor is closed. '
                'Cannot fetch rows or execute operations on a closed cursor'
            )

        if self._result_set is None:
            # the previous call to .execute*() did not produce any result set 
            # or no call was issued yet.
            raise InterfaceError(
                'Cursor result set is empty. '
                'The previous call to .execute*() did not produce any result set '
                'or no call was issued yet '
                'or you are attempting to fetch from an empty result set. '
                'Hint: .rowcount is 0 if .execute*() did not produce any result set '
                'or -1 if no call was issued yet.' 
            )

        # assume fetched rows exausted, all results consumed        
        if len(self._result_set) > 0:
            # fetched rows not exausted yet, consume return next row
            return self._result_set.pop(0)
        
        # no more rows in the result set
        self._result_set = None
        return None

    def fetchall(self) -> List[tuple]:
        """Fetch all rows of this query result. 
        
        This method returns all the remaining rows contained in this query result as a sequence of sequences 
        (e.g. a list of tuples). 
        Please refer to :mod:`notiondbapi` for a detailed description of how Notion JSON objects are
        parsed and cross-compiled into Python ``tuple`` objects.

        Important:
            After a call to the :meth:`.fetchall()` the result set is exausted (empty). Any subsequent call
            to this method returns an empty sequence. 

        .. versionchanged:: 0.5.0
            Calling this method on a closed cursor raises the :exc:`Error`.

        Raises:
            Error: If the cursor is closed.
            InterfaceError: If the previous call to :meth:`.execute()` did not produce any result set
                            or no call was issued yet.

        Returns:
            List[tuple]: The list containing all the remaining queried rows. ``[]`` if no rows are available.
        """

        if self._closed:
            raise Error(
                'This cursor is closed. '
                'Cannot fetch rows or execute operations on a closed cursor'
            )

        if self._result_set is None:
            # the previous call to .execute*() did not produce any result set 
            # or no call was issued yet.
            raise InterfaceError(
                'Cursor result set is empty. '
                'The previous call to .execute*() did not produce any result set '
                'or no call was issued yet '
                'or you are attempting to fetch from an empty result set. '
                'Hint: .rowcount is 0 if .execute*() did not produce any result set '
                'or -1 if no call was issued yet.' 
            )

        # assume result set is empty
        results = []
        if len(self._result_set) > 0:
            results = list(self)
            
            # Important: This ensures that any subsequent call returns an empty list
            self._result_set = []          

        return results
    
    def execute(self, operation: dict, parameters: Optional[DBAPIExecuteParameters] = None) -> Self:
        """Prepare and execute a database operation (query or command).

        Parameters may be provided as a mapping and will be bound to variables in the operation.
        In Notion, the use of placeholders for variable is redundant, since the properties to 
        which the values are to be bound are known by name.
        In the current implementation, the parameter style implemented in the cursor is:
        `named`.
        The :meth:`execute()` methods implements a return interface to enable concatenating 
        calls on :class:`Cursor` methods.

        Important:
            :meth:`.execute()` stores the executed command result(s) in the internal
            result set. Always call this method prior to :meth:`BaseCursor.fetchone()` and :meth:`Cursor.fetchall()`,
            otherwise an :exc:`InterfaceError` error is raised. 

        .. versionchanged:: 0.7.0
            :meth:`Cursor:execute()` now accepts operations with no parameters. This happens for ``CREATE TABLE`` DDL statements.

        .. versionchanged:: 0.5.0
            Calling this method on a closed cursor raises the :exc:`Error`.

        Examples:
            Create a new page as child of an exisisting database:

            >>> operation = {'endpoint': 'pages', 'request': 'create'}
            >>> parameters = {
            >>>     'payload': {
            >>>         'properties': {
            >>>             'id': {'number': ':id'},       
            >>>             'name': {'title': [{'text': {'content': ':name'}}]},
            >>>             'grade': {'rich_text': [{'text': {'content': ':grade'}}]}
            >>>         },
            >>>         'parent': parent
            >>>     },
            >>>     'params': {                           # params contains the bindings
            >>>         'id': 1,
            >>>         'name': 'Isaac Newton',
            >>>         'grade': 'B'
            >>>     }
            >>> }
            >>> cursor = Cursor()
            >>> cursor.execute(operation, parameters).fetchall()
            >>> assert cursor.rowcount == 0  # 0 remaining rows after fetchall()

        Args:
            operation (dict): A dictionary containing the Notion API request to be executed.
            parameters (DBAPIExecuteParameters): A dictionary containing the payload for the Notion API request

        Raises:
            Error: If the cursor is closed.
            InterfaceError: ``"properties"`` object not specified in parameters.
            InterfaceError: ``"parent"`` object not specified in parameters.
       
        Returns:
            Self: This :class:`Cursor` instance.
        """
        
        if self._closed:
            raise Error(
                'This cursor is closed. '
                'Cannot fetch rows or execute operations on a closed cursor'
            )

        object_ = {}
        try:
            object_ = self._client(operation['endpoint'], operation['request'], operation['payload'])
        except KeyError as ke:
            raise InterfaceError(f"Missing required key in operation dict: {ke.args[0]}")
        
        except NotionError as ne:
            raise InterfaceError(
                f'NotionError("{self._client.__class__.__name__}"): {ne}'
            ) from ne
        
        self._parse_result_set(object_)         # initialize result set with parsed rows, if any
        return self
    
    def executemany(self, operation: dict, parameters: Sequence[dict]) -> Self:
        """Prepare a database operation (query or command) and then execute it against all parameter sequences or 
        mappings found in the sequence seq_of_parameters.

        Note:
            This method is not implemented yet.
            Calling it raises :exc:`NotImplementedError`.

        Args:
            operation (dict): A dictionary containing the Notion API request to be executed.
            parameters (Sequence[dict]): A sequence of dictionaries containint the parameters to be executed multiple times.

        Returns:
            Self: This :class:`Cursor` instance.
        """
        raise NotImplementedError

    def close(self) -> None:
        """Close the cursor now.

        The cursor will be unusable from this point forward; an Error exception 
        will be raised if any operation is attempted with the cursor.

        .. versionadded:: 0.5.0
        """
        # set internal cursor state and close it
        self._description = None
        self._result_set = None
        self._closed = True
                
class CompositeCursor(Cursor):
    """Transaction-aware DBAPI cursor
    
    This is how the new Cursor class will work in tandem with :class:`Connection`.

    Note:
        Unfortunately, the DBAPI 2.0 does not forsee an execute() method for the Connection class.
        This leads to a suboptimal separation of concerns: The Connection clas should be responsible to manage the
        transaction and to execute operations, while Cursor should only be concerned with providing access to
        the results. In the lack of an execute() method at connection level, the Cursor class needs to have a reference 
        to the connection, so it can start a new transaction on the first call to its execute() method.

    .. versionchanged:: 0.7.0
    
    """
    def __init__(self, dbapi_connection: Connection):
        self._dbapi_connection = dbapi_connection
        super().__init__(self._dbapi_connection._client)

    def execute(self, operation: dict, parameters: Optional[DBAPIExecuteParameters] = None) -> Self:
        """Execute the operation within the currently opened transaction.

        This method is similar to :meth:`BaseCursor.execute()` with the additional feature of
        executing the operation within the currently opened transaction. 
        This means that it does not execute immediately the operation, but it add the operation
        to the operations list of the opened transaction.
        Execution is deferred to the point in time when the :meth:`Connection.commit()` is called.

        Args:
            operation (dict): A dictionary containing the Notion API request to be executed.
            parameters (DBAPIExecuteParameters): A dictionary containing the payload for the Notion API request

        Raises:
            OperationalError: If it fails to add the operation to the transaction.
            InternalError: If the operation is not supported or not recognized.

        Returns:
            _type_: This cursor instance.

        .. versionadded:: 0.7.0
        """
        # begin a new transaction if the connection is not in transaction state
        if not self._dbapi_connection._in_transaction():
            self._dbapi_connection._begin_transaction()

        # IMPORTANT => DO NOT CALL the parent's method implementation!
        # Otherwise the operation will be executed immediately and outside the transaction context.
        # Add the operation and parameters to the transaction operations list
        payload = {}
        if parameters and parameters.get('params', {}):
            # bind the params and construct the final payload
            payload = self._bind_parameters(parameters)
        
        else:
            # no binding necessary, extract the payload
            payload = parameters.get('payload')

        self._dbapi_connection._execute_in_transaction(operation, payload)

        return self

IsolationLevel = Literal[
    """Isolation level supported by the API."""

    "SERIALIZABLE",
    "REPEATABLE READ",
    "READ COMMITTED",
    "READ UNCOMMITTED",
    "AUTOCOMMIT",
]



class Connection:
    """Provide database base connection functionalty according to the DBAPI 2.0 specification (PEP 249).
    
    Warning:
        This class implements the "AUTOCOMMIT" (non-transactional) isolation level only. This means that its transaction-related methods like :meth:`Connection.commit()`
        and :meth:`Connection.rollback()` are not implemented.

    .. versionadded:: 0.7.0

    """

    def __init__(self, client: AbstractNotionClient, isolation_level: Optional[IsolationLevel] = 'AUTOCOMMIT'):
        self._isolation_level = isolation_level
        """The isolation level set for this database connection."""

        self._client = client
        """The dependecy-injected Notion client to interface with."""

        self._cursor: Cursor = None
        """Classic DBAPI cursor to execute operations and fetch rows."""

    @property
    def autocommit(self) -> bool:
        """Read-only attribute to query the autocommit mode of the connection.
        
        Return ``True`` if the connection is operating in autocommit (non-transactional) mode.

        Note:
            :class:`BaseConnection` implements the autocommit mode only, so the read-only attribute always returns ``True``.
            Setting the autocommit mode by writing to the attribute is not supported as it is deprecated according to PEP 249.
        
        """
        return self._isolation_level == 'AUTOCOMMIT'

    def cursor(self, composite: Optional[bool] = False) -> Union[Cursor, CompositeCursor]:
        """Procure a new cursor object using the connection.

        Args:
            composite (bool, optional): ``False`` supported only. The provided value is ignored. 

        Returns:
            Union[Cursor, CompositeCursor]: Always a non-composite cursor regardless of the supplied argument value.

        .. versionchanged:: 0.7.0

        """
 
        # IMPORTANT: The DBAPI connection must always procure a new cursor because this has a per-operation lifecyle
        return Cursor(self._client)
    
    
    def _begin_transaction(self) -> None:
        """Begin a new transaction."""

    def _in_transaction(self) -> bool:
        """True if the connection has already initiated a transaction.
        
        This method is used by the cursor to determin whether to begin a new transaction or not.
        """
        return True

    def _execute_in_transaction(self, operation: dict, parameters: Optional[DBAPIExecuteParameters] = None) -> None:
        """Execute the operation in the context of the opened transaction.
        
        Args:
            operation (dict): A dictionary containing the Notion API request to be executed.
            parameters (DBAPIExecuteParameters): A dictionary containing the payload for the Notion API request

        Raises:
            OperationalError: If it fails to add the operation to the transaction.
            InternalError: If the operation is not supported or not recognized.

        .. versionadded:: 0.7.0
        
        """
        self.cursor().execute(operation, parameters)

class TxnConnection(Connection):
    """Provide database base connection functionalty according to the DBAPI 2.0 specification (PEP 249).

    Warning:
        This class is still proof-of-concept stage. It needs to be initialized with a Flask testing client (:class:`FlaskClient`).
        **DO NOT USE YET!**

    .. versionadded:: 0.7.0

    """
    def __init__(self, proxy_client: FlaskClient, client: AbstractNotionClient):
        super().__init__(client)
        self._proxy_client = proxy_client
        
        self._tx_id: str = None 
        
        self._cursor: Cursor = None
        """Classic DBAPI cursor to execute operations and fetch rows."""
        
        self._comp_cursor: CompositeCursor = None
        """Composite cursor holding all cursors created out of committed changes in the transaction."""

        self._cursors: List[Cursor] = []

    def cursor(self, composite: Optional[bool] = False) -> Union[Cursor, CompositeCursor]:
        """Procure a new cursor object using the connection.

        Args:
            composite (bool, optional): If ``True`` procure a :class:`normlite.notiondbapi.dbapi2.CompositeCursor`, else 
            a `normlite.notiondbapi.dbapi2.Cursor` instance holding the last result set returned by the last
            committed statement. Defaults to ``False``.

        Returns:
            Union[Cursor, CompositeCursor]: Either a cursor or a composite depending on the argument value.

        .. versionchanged:: 0.7.0

        """
        if composite:
            self._cursor = CompositeCursor(self._cursors)
        else:
            self._cursor = self._cursors[-1]  # last_only = True => only return the last result set
        
        return self._cursor
    
    def _begin_transaction(self) -> None:
        """Begin a new transaction."""
        
        # Assumption: The caller implements correctly the internal API.
        # Call _begin_transaction() only if _in_transaction() returns False
        # => No checks consistency to avoid multiple txn here!
        response = self._proxy_client.post('/transactions')
        if response.status_code != 200:
            raise InterfaceError(
                f'Unable to start a new transaction in the proxy server. '
                f'Reason: {response.get_json()['error']}'
            )
        
        self._tx_id = response.get_json()['transaction_id']

    def _in_transaction(self) -> bool:
        """True if the connection has already initiated a transaction.
        
        This method is used by the cursor to determin whether to begin a new transaction or not.
        """
        return self._tx_id
    
    def _execute_in_transaction(self, operation: dict, parameters: Optional[DBAPIExecuteParameters] = None) -> None:
        """Execute the operation in the context of the opened transaction.
        
        Args:
            operation (dict): A dictionary containing the Notion API request to be executed.
            parameters (DBAPIExecuteParameters): A dictionary containing the payload for the Notion API request

        Raises:
            OperationalError: If it fails to add the operation to the transaction.
            InternalError: If the operation is not supported or not recognized.

        .. versionadded:: 0.7.0
        
        """
             
        if operation['endpoint'] == 'pages' and operation['request'] == 'create':
            # add insert operation
            response = self._proxy_client.post(
                f"/transactions/{self._tx_id}/insert", 
                json=parameters
            )

            if response.status_code != 202:
                raise OperationalError(f'Failed to add insert operation to transaction. Reason: {response.get_json()['error']}')
            
        else:
            raise InternalError(f'Unsupported or bad operation: {operation}')
                
    def commit(self) -> None:
        """Commit any pending transaction to the database.

        Note:
            If the database supports an auto-commit feature, this must be initially off. 
            An interface method may be provided to turn it back on.

        .. versionadded:: 0.7.0

        """ 
        response = self._proxy_client.post(
            f'/transactions/{self._tx_id}/commit'
        )

        if response.status_code != 200:
            raise DatabaseError(
                f'Failed to commit transaction: {self._tx_id}. '
                f'Reason: {response.get_json()['error']}'
            )
        
        # create cursors
        self._create_cursors(response.get_json()['data'])

    def _create_cursors(self, result_sets: Sequence[dict]) -> None:
        """Helper to populate the _cursors attribute holding the cursors to access all rows returned in the transaction.
        
        .. versionadded:: 0.7.0

        """
        for result_set in result_sets:
            cursor = Cursor(self)
            cursor._parse_result_set(result_set)
            self._cursors.append(cursor)

class CompositeCursor(Cursor):
    """Extend a DBAPI cursor to manage multiple child cursors, one per result set returned
    from a multi-statement transaction commit.
    """
    def __init__(self, cursors: Sequence[Cursor]):
        self._cursors = list(cursors)
        self._current_index = 0
        self._current_cursor = self._cursors[self._current_index]

    def nextset(self) -> bool:
        """Advance to the next result set if available.
        
        This method makes the cursor skip to the next available set, discarding any remaining rows from the current set. 
        It returns ``False`` if there are no more sets or returns ``True`` and subsequent calls to the cursor.fetch*() methods 
        returns rows from the next result set. 
        """
        if self._current_index + 1 < len(self._cursors):
            # close the current cursor, advance and forget
            self._current_cursor.close()
            self._current_index += 1
            self._current_cursor = self._cursors[self._current_index]
            return True
        return False
    
    @property
    def rowcount(self) -> int:
        """Return the row count of the current cursor."""
        return self._current_cursor.rowcount
    
    @property
    def lastrowid(self) -> int:
        """Return the last row id of the current cursor."""
        return self._current_cursor.lastrowid
    
    @property
    def description(self) -> tuple:
        """Return the description of the current cursor."""
        return self._current_cursor.description
    
    @property
    def paramstyle(self) -> DBAPIParamStyle:
        """Return the row parameter style of the current cursor."""
        return self._current_cursor.paramstyle
    
    def fetchone(self) -> Optional[tuple]:
        """Fetch the next row of the current cursor's result set."""
        return self._current_cursor.fetchone()
    
    def fetchall(self) -> List[tuple]:
        """Fetch all rows of the current cursor's result set. """
        return self._current_cursor.fetchall()




