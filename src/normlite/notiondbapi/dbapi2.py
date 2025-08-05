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

import pdb
from typing import Any, Dict, Iterator, List, Literal, Optional, Self, Sequence, Tuple, TypeAlias
import uuid

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

class Cursor:
    """Provide database cursor functionalty according to the DBAPI 2.0 specification (PEP 249)."""
    def __init__(self, client: AbstractNotionClient):
        self._client = client
        """The client implementing the Notion API."""

        self._result_set = None
        """The result set returned by the last :meth:`.execute()`. It is set by :meth:`._parse_result_set()`"""
        
        self._paramstyle: DBAPIParamStyle = 'named'
        """The default parameter style applied."""

        self._description: tuple = None
        """Provide information describing one result column."""

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
            self._description = page.accept(desc_visitor)
         
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
        
    def __iter__(self) -> Iterable[tuple]:
        """Make cursors compatible to the iteration protocol.

        Note:
            This method is not tested yet. Don't use it yet.

        Yields:
            Iterator[Iterable[tuple]]: The next row in the result set.
        """
        while self._result_set:
            next_row = self._result_set.pop(0)
            yield next_row

    def fetchone(self) -> Optional[tuple]:
        """Fetch the next row of a query result set.

        Note:    
            The current implementation guarantees that a call to this method will only move 
            the associated cursor forward by one row.

        Raises:
            InterfaceError: If the previous call to :meth:`.execute()` did not produce any result set
                            or no call was issued yet.

        Returns:
            Optional[tuple]: The next row as single tuple, or an empty tuple when no more data is available.
        """
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

        Raises:
            InterfaceError: If the previous call to :meth:`.execute()` did not produce any result set
                            or no call was issued yet.

        Returns:
            List[tuple]: The list containing all the remaining queried rows. ``[]`` if no rows are available.
        """

        if self._result_set is None:
            # the previous call to .execute*() did not produce any result set 
            # or no call was issued yet.
            raise InterfaceError(
                'Cursor result set is empty. '
                'The previous call to .execute*() did not produce any result set'
                'or no call was issued yet '
                'or you are attempting to fetch from an empty result set. '
                'Hint: .rowcount is 0 if .execute*() did not produce any result set '
                'or 1 if no call was issued yet.' 
            )

        # assume result set is empty
        results = []
        if len(self._result_set) > 0:
            results = list(self)
            
            # Important: This ensures that any subsequent call returns an empty list
            self._result_set = []          

        return results
    
    def execute(self, operation: Dict[str, Any], parameters: DBAPIExecuteParameters) -> Self:
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
            result set. Always call this method prior to :meth:`Cursor.fetchone()` and :meth:`Cursor.fetchall()`,
            otherwise an :exc:`InterfaceError` error is raised. 

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
            InterfaceError: ``"properties"`` object not specified in parameters
            InterfaceError: ``"parent"`` object not specified in parameters
       
        Returns:
            Self: This :class:`Cursor` instance.
        """
        
        object_ = {}
        payload = {}
        if parameters.get('params', {}):
            # Not all operations require paramter binding (e.g. databases.create)
            payload = self._bind_parameters(parameters)
        
        else:
            payload = parameters.get('payload')

        try:
            object_ = self._client(
                operation['endpoint'],
                operation['request'],
                payload
            )
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
    
    def _bind_parameters(self, parameters: DBAPIExecuteParameters) -> dict:
        """Helper for binding values to the payload."""
        
        payload = parameters.get('payload', {})
        params = parameters.get('params', {})
        if not payload or not params:
            raise InterfaceError(f'Missing "payload" or "params" object in parameters: {parameters}')
        
        properties: Dict[str, Any] = payload.get('properties', {})
        if not properties:
            raise InterfaceError('f"Missing "properties" object in payload: {payload}')
        
        
        for property_name in properties.keys():
            property_object = properties.get(property_name)
            try:
                # get the value from the current binding corresponding to the property name
                value = params[property_name]
            except KeyError as ke:
                raise InterfaceError(f"Missing binding parameter for property '{ke.args[0]}'") 
            
            for key in property_object.keys(): 
                if key == 'number':
                    # the property value to be bound is a number
                    if not isinstance(value, int):
                        raise ValueError(
                            f"Expected integer value for number property: '{property_name}', "
                            f"received: '{value}'"
                        )
                    property_object[key] = value
                
                elif key in ['title', 'rich_text']:
                    # the property value to be bound is a title or richt text
                    if not isinstance(value, str):
                        raise ValueError(
                            f"Expected string value for '{key}' property: '{property_name}', "
                            f"received: '{value}'"
                        )
                    property_object[key] = [dict(text=dict(content=value))]
                
                else:
                    raise TypeError(
                        f"Unknown or unsupported type '{key}' "
                        f"supplied in property '{property_name}'"
                    )
                
        return payload
                


             
                




