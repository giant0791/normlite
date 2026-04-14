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
from operator import itemgetter
import pdb
from typing import Any, Dict, Iterator, List, Literal, NoReturn, Optional, Self, Sequence, Tuple, TypeAlias, Union
from itertools import islice
import uuid
from flask.testing import FlaskClient


from normlite._constants import SpecialColumns
from normlite.notion_sdk.client import AbstractNotionClient, NotionError
from normlite.notiondbapi.resultset import ResultSet

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

class ProgrammingError(DatabaseError):
    """Exception raised for errors that are related to the database’s operation and not necessarily under 
    the control of the programmer.

    Example situations are an unexpected disconnect occurs, the data source name is not found, 
    a transaction could not be processed, a memory allocation error occurred during processing, etc. 

    .. versionadded:: 0.8.0

    """

_desc_mapper: dict[str, str] = {
    SpecialColumns.NO_ID: "id",
    SpecialColumns.NO_ARCHIVED: "archived",
    SpecialColumns.NO_IN_TRASH: "in_trash",
    SpecialColumns.NO_CREATED_TIME: "created_time",
    SpecialColumns.NO_TITLE: "title",
}

class _Description:
    """Internal DBAPI representation of cursor.description.
    
    .. versionadded:: 0.9.0
    """

    __slots__ = ("_entries",)

    def __init__(self, entries: tuple[tuple, ...]):
        self._entries = entries

    def as_sequence(self) -> tuple[tuple, ...]:
        return self._entries

class Cursor:
    """Provide database base cursor functionalty according to the DBAPI 2.0 specification (PEP 249).
    
    Note:
        the :class:`Cursor` does not support transaction awareness. Use :class:`CompositeCursor` for fully
        DBAPI 2.0 compliant cursor.

    .. versionchanged: 0.9.0
        This version supports execution of bulk operations, see :meth:`Cursor.executemany`.

    .. versionadded:: 0.7.0

    """
    def __init__(self, client: AbstractNotionClient):
        self._client = client
        """The client implementing the Notion API."""

        self._result_sets: list[ResultSet] = []
        """The result sets returned by bulk operations.
        
        .. versionadded:: 0.9.0
        """
        
        self._result_index: int = 0
        """The current result set to be consumed in the result sets sequence.
        
        .. versionadded:: 0.9.0
        """
        
        self._result_set = None
        """The result set returned by the last :meth:`.execute()`. It is set by :meth:`._parse_result_set()`
        
        ..version-removed:: 0.9.0
            Outdated internal attribute. This will be removed in the next version.
        """
        
        self._paramstyle: DBAPIParamStyle = 'named'
        """The default parameter style applied."""

        self._description: Sequence[tuple] = None
        """Provide information describing one result column.
        
        This attribute is set by the :meth:`normlite.engine.context.ExecutionContext.pre_exec` via the :meth:`_inject_description` 
        if the operation to be executed is a DML constrcut. For DML statements the table involved is the authoritative source of truth
        for the schema (i.e. description).

        .. versionchanged:: 0.9.0
            In this version, the :attr:`_description` is now used to buffer the description until the
            :meth:`_parse_result_set` is called. This method now uses the new :class:`normlite.notiondbapi.resultset.ResultSet`
            to do the parsing of the Notion returned object. The new class for the result set takes care to 
            either return the injected description, in the DML case, or to construct the description from the retrieved database, 
            in the DDL case. 

        """

        self._closed = False
        """Whether this cursor is closed. Always ``False`` after initialitation."""
        
        self.arraysize = 1
        """The number of rows to fetch at a time with :meth:`fetchmany`. 
        
        It defaults to 1 meaning to fetch a single row at a time.
        
        .. versionadded:: 0.9.0
        """

    @property
    def _current_result_set(self) -> Optional[ResultSet]:
        """Provide access to current result set."""
        if not self._result_sets:
            return None
        return self._result_sets[self._result_index]    

    @property
    def description(self) -> Optional[Sequence[tuple]]:
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
        rs = self._current_result_set
        if rs is not None:
            return rs.description
    
        return None
    
    @property
    def closed(self) -> bool:
        """``True`` if the cursor is closed.
        
        .. admonition:: DBAPI extension
            :class: note

            The closed attribute is a normlite extension to the DBAPI 2.0.

        .. versionadded:: 0.9.0
            
        """
        return self._closed
    
    @property
    def rowcount(self) -> int:
        """This read-only attribute specifies the number of rows that 
           the last :meth:`.execute` / :meth:`executemany` produced.

        .. important::
            This version supports non paginated results. Thus, the :attr:`rowcount` is _always_
            equal to the rows contained in the current result set.
            In future versions supporting paginated results, the :attr:`rowcount` will be -1 
            _until all pages have been retrieved_. Once this condition is fulfilled, :attr:`rowcount`
            will provide the sum of all retrieved pages.
            For example, let's assume the query being executed returns 100 rows. Notion returns batches
            of 10 pages only. So, the :attr:`rowcount` will stay equal to -1 until all 10 batches have
            been retrieved.

        Returns:
            int: Number of rows. `-1` if in case no :meth:`.execute()` has been performed 
                 on the cursor or the rowcount of the last operation cannot be 
                 determined by the interface.

        .. versionchanged:: 0.9.0
            This version adds support for row counting in case of multiple result sets
        """
        if not self._result_sets:
            return -1
        
        # sum of all result sets in the cursor
        return sum(len(rs) for rs in self._result_sets)
        
    @property
    def lastrowid(self) -> Optional[int]:
        """This read-only attribute provides the rowid of the last modified row.

        Most Notion API calls return an object with an id, which is used as rowid. 
        If the operation does not set a rowid, this attribute is set to ``None``.
        In case of bulk execution, it returns the rowid of the last modified row
        belonging to the current result set. 

        .. admonition:: DBAPI extension
            :class: note

            The semantics of :attr:`lastrowid` is a normlite extension to the DBAPI 2.0.

            ``normlite`` considers inserted/updated/deleted rows as modified rows.
            This means that :attr:`.lastrowid` returns non ``None`` values after INSERT/UPDATE/DELETE statements.

            ``normlite`` also defines semantics of ::attr::`.lastrowid` in case the last executed 
            statement modified more than one row, e.g. when using multi-row INSERT/UPDATE/DELETE statements.
            
            :attr:`.lastrowid` returns a 128-bit integer representation of the object id, which can be 
            used to directly access Notion objects.

        Example:
            >>> object_id = str(uuid.UUID(int=cursor.lastrowid))
            >>> print(object_id)
            680dee41-b447-451d-9d36-c6eaff13fb46

        Returns:
            Optional[int]: A 128-bit integer representing the UUID object id or `None`. 

        .. versionchanged:: 0.9.0
            This version derives the last inserted rowid from the new attribute
            :attr:`normlite.notiondbapi.resultset.ResultSet.last_inserted_rowids`.
            Additionally, it now supports multiple result sets.
        """
        rs = self._current_result_set
        if rs is None:
            return None       
        
        # extract the object UUID of the last row as 128-bit integer
        last_inserted_rowids = rs.last_inserted_rowids
        if last_inserted_rowids is None:
            # no rows modified
            return None
        
        return uuid.UUID(last_inserted_rowids[-1][0]).int
    
    @property
    def lastrowid_as_string(self) -> Optional[str]:
        rs = self._current_result_set
        if rs is None:
            return None       
        
        # extract the object UUID of the last row as 128-bit integer
        last_inserted_rowids = rs.last_inserted_rowids
        if last_inserted_rowids is None:
            # no rows modified
            return None
        
        return last_inserted_rowids[-1][0]
    
    @property
    def _last_inserted_row_ids(self) -> Optional[list[tuple]]:
        """Return all row ids of the last inserted rows as flattened out list.
        
        .. versionadded:: 0.9.0
        """
        all_ids = []
        for rs in self._result_sets:
            if rs.last_inserted_rowids is not None:
                all_ids.extend(rs.last_inserted_rowids)
        
        return all_ids
        
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
    
    def _inject_description(
        self, 
        schema_entries: tuple[tuple, ...],
    ) -> None:
        self._description = schema_entries

    def _check_closed(self) -> NoReturn:
        if self._closed:
            raise ProgrammingError(
                'This cursor is closed. '
                'Cannot fetch rows or execute operations on a closed cursor'
            )

    def _append_result_set(self, returned_obj: Dict[str, Any]) -> None:
        """Parse the JSON object returned by the command execution and add it to the current result set.

        .. versionchanged:: 0.9.0
            This method now fully supports multiple result sets and it has been renamed from 
            `_parse_result_set()`.
        """
        self._check_closed()
        rs = ResultSet.from_json(
            self._description,
            returned_obj
        )
        self._result_sets.append(rs)

    def _reset_results(self) -> None:
        """Reset helper."""
        self._result_sets = []
        self._result_index = 0

    def __iter__(self) -> Iterator[tuple]:
        """Make cursors compatible with the iteration protocol.

        Raises:
            ProgrammingError: If the previous call to :meth:`.execute()` did not produce any result set
                or no call was issued yet.

        Yields:
            Iterator[Iterable[tuple]]: The next row in the result set.

        .. versionchanged:: 0.9.0
            This version uses the new redesigned iterator based :class:`normlite.notiondbapi.resultset.ResultSet`
            class.
            Additionally, it supports bulk executions.
            
        .. versionchanged:: 0.5.0
            Calling this method on a closed cursor raises the :exc:`Error`.

        """
        self._check_closed()
        rs = self._current_result_set
        if rs is None:
            raise ProgrammingError(
                'Cursor result set is empty. '
                'The previous call to .execute*() did not produce any result set '
                'or no call was issued yet '
                'or you are attempting to fetch from an empty result set. '
                'Hint: .rowcount is 0 if .execute*() did not produce any result set '
                'or -1 if no call was issued yet.' 
            )

        return self

    def fetchone(self) -> Optional[tuple]:
        """Fetch the next row of a query result set.

        This method returns the next row or ``None`` when no more data is available.

        Note:    
            The current implementation guarantees that a call to this method will only move 
            the associated cursor forward by one row.

        Raises:
            Error: If the cusors is closed.
            ProgrammingError: If the previous call to :meth:`.execute()` did not produce any result set
                or no call was issued yet.

        Returns:
            Optional[tuple]: The next row as single tuple, or an empty tuple when no more data is available.

        .. versionchanged:: 0.9.0
            This version uses the refactored result set. It raises :exc:`ProgrammingError` as sqlite3.
            Additionally, it supports bulk executions.

        .. versionchanged:: 0.5.0
            Calling this method on a closed cursor raises the :exc:`Error`.
        """
        self._check_closed()
        rs = self._current_result_set
        if rs is None:
            raise ProgrammingError(
                'Cursor result set is empty. '
                'The previous call to .execute*() did not produce any result set '
                'or no call was issued yet '
                'or you are attempting to fetch from an empty result set. '
                'Hint: .rowcount is 0 if .execute*() did not produce any result set '
                'or -1 if no call was issued yet.' 
            )

        try:
            return next(rs)
        except StopIteration:
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

        .. versionchanged:: 0.9.0
            This version uses the refactored result set. It raises :exc:`ProgrammingError` as sqlite3.
            Additionally, it supports bulk executions.   

        .. versionchanged:: 0.5.0
            Calling this method on a closed cursor raises the :exc:`Error`.

        Raises:
            Error: If the cursor is closed.
            ProgrammingError: If the previous call to :meth:`.execute()` did not produce any result set
                or no call was issued yet.

        Returns:
            List[tuple]: The list containing all the remaining queried rows. ``[]`` if no rows are available.
        """

        self._check_closed()

        rs = self._current_result_set
        if rs is None:
            raise ProgrammingError(
                'Cursor result set is empty. '
                'The previous call to .execute*() did not produce any result set '
                'or no call was issued yet '
                'or you are attempting to fetch from an empty result set. '
                'Hint: .rowcount is 0 if .execute*() did not produce any result set '
                'or -1 if no call was issued yet.' 
            )

        return list(rs)
    
    def fetchmany(self, size: Optional[int]=None) -> list[tuple]:
        """Fetch the next set of rows of a query result, returning a sequence of sequences (e.g. a list of tuples). 
        
        An empty sequence is returned when no more rows are available.
        The number of rows to fetch per call is specified by the parameter. 
        If it is not given, the cursor’s :attr:`arraysize` determines the number of rows to be fetched. 
        The method tries to fetch as many rows as indicated by the size parameter. 
        If this is not possible due to the specified number of rows not being available, 
        fewer rows may be returned.

        Args:
            size (int, optional): Number of rows to be fetched. Defaults to ``None``.

        Raises:
            ProgrammingError: If the previous call to :meth:`.execute()` did not produce any result set
                or no call was issued yet.

        Returns:
            list[tuple]: The list containing all the remaining queried rows. ``[]`` if no rows are available.

        .. versionadded:: 0.9.0
            This version provides a highly optimized implementation fully leveraging the iterator
            design of the class :class:`normlite.notiondbapi.resultset.ResultSet`.
            The core optimization delegating the fetch loop to :func:`itertools.islice`, which uses the Python runtime's 
            highly optimized C code and reduces the overhead of the Python interpreter for every row fetched. 
            Additionally, it supports bulk executions.
        """
        self._check_closed()

        rs = self._current_result_set
        if rs is None:
            raise ProgrammingError(
                'Cursor result set is empty. '
                'The previous call to .execute*() did not produce any result set '
                'or no call was issued yet '
                'or you are attempting to fetch from an empty result set. '
                'Hint: .rowcount is 0 if .execute*() did not produce any result set '
                'or -1 if no call was issued yet.' 
            )

        n = self.arraysize if size is None else size

        return list(islice(rs, n))
    
    def execute(self, operation: dict, parameters: DBAPIExecuteParameters) -> Self:
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
            object_ = self._client(
                operation['endpoint'],
                operation['request'],
                parameters.get('path_params'),
                parameters.get('query_params'),
                parameters.get('payload'),
            )
        except KeyError as ke:
            # Programming error in the DBAPI usage
            raise InterfaceError(
                f"Missing required key in operation or parameters: {ke.args[0]}"
            ) from ke

        except NotionError as ne:
            # --- Database object does not exist -----------------------------
            if ne.code == "object_not_found":
                raise ProgrammingError(
                    f'NotionError("Object not found: {ne}'
                ) from ne

            # --- Authentication / authorization -----------------------------
            if ne.code in {"unauthorized", "forbidden"}:
                raise OperationalError(
                    f'Authentication/authorization failure: {ne}'
                ) from ne

            # --- Rate limiting / availability -------------------------------
            if ne.code in {"rate_limited", "service_unavailable"}:
                raise OperationalError(
                    f'Backend temporarily unavailable: {ne}'
                ) from ne

            # --- Malformed request / client misuse --------------------------
            if ne.code in {"invalid_request", "validation_error"}:
                raise InterfaceError(
                    f'Invalid request sent to backend: {ne}'
                ) from ne

            # --- Fallback: backend rejected operation -----------------------
            raise DatabaseError(
                f'Unhandled NotionError("{self._client.__class__.__name__}"): {ne}'
            ) from ne
        
        self._reset_results()
        self._append_result_set(object_)
        return self
    
    def executemany(self, operation: dict, parameters: Sequence[DBAPIExecuteParameters]) -> Self:
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

        .. versionadded:: 0.9.0
        """
        self._check_closed()
        self._reset_results()

        for param_set in parameters:

            try:
                object_ = self._client(
                    operation['endpoint'],
                    operation['request'],
                    param_set.get('path_params'),
                    param_set.get('query_params'),
                    param_set.get('payload'),
                )
            except KeyError as ke:
                # Programming error in the DBAPI usage
                raise InterfaceError(
                    f"Missing required key in operation or parameters: {ke.args[0]}"
                ) from ke

            except NotionError as ne:
                # --- Database object does not exist -----------------------------
                if ne.code == "object_not_found":
                    raise ProgrammingError(
                        f'NotionError("Object not found: {ne}'
                    ) from ne

                # --- Authentication / authorization -----------------------------
                if ne.code in {"unauthorized", "forbidden"}:
                    raise OperationalError(
                        f'Authentication/authorization failure: {ne}'
                    ) from ne

                # --- Rate limiting / availability -------------------------------
                if ne.code in {"rate_limited", "service_unavailable"}:
                    raise OperationalError(
                        f'Backend temporarily unavailable: {ne}'
                    ) from ne

                # --- Malformed request / client misuse --------------------------
                if ne.code in {"invalid_request", "validation_error"}:
                    raise InterfaceError(
                        f'Invalid request sent to backend: {ne}'
                    ) from ne

                # --- Fallback: backend rejected operation -----------------------
                raise DatabaseError(
                    f'Unhandled NotionError("{self._client.__class__.__name__}"): {ne}'
                ) from ne
            
            self._append_result_set(object_)
        
        return self
        
    def nextset(self) -> bool:
        """Advance to the next available result set.
        
        .. versionadded:: 0.9.0
        """

        if self._result_index + 1 >= len(self._result_sets):
            return False

        self._result_index += 1
        return True

    def _iter_all(self) -> Iterator[tuple]:
        """Iterate over all rows across all result sets.
        
        This is a private API used by :class:`normlite.engine.cursor.CursorResult` to return all rows.

        .. versionadded:: 0.9.0
        """
        self._check_closed()

        if not self._result_sets:
            raise ProgrammingError(
                "Cursor result set is empty. No execute*() call was issued."
            )

        for rs in self._result_sets:
            yield from rs
    
    def close(self) -> None:
        """Close the cursor now.

        The cursor will be unusable from this point forward; an Error exception 
        will be raised if any operation is attempted with the cursor.

        .. versionadded:: 0.5.0
        """
        # set internal cursor state and close it
        self._description = None
        self._reset_results()
        self._closed = True
                
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

    def cursor(self) -> Cursor:
        """Procure a new cursor object using the connection.

        Returns:
            Cursor: The new cursor object using this connection.

        .. versionchanged:: 0.9.0
            All code related to composite cursor is removed.    

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

