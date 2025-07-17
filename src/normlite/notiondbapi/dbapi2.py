from typing import Any, Dict, List, Optional, Self, Tuple
import uuid

from normlite.notion_sdk.client import AbstractNotionClient, NotionError

class Error(Exception):
    """Base class of all other error exceptions.

    It can be used this to catch all errors with one single `except` statement. 
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
    """Implement the `Cursor` class according to the DBAPI 2.0 specification."""
    def __init__(self, client: AbstractNotionClient):
        self._client = client
        self._result_set = {}  # set by execute method
        self._rowcount = -1
        self._lastrowid = 'UNDEF'

    @property
    def rowcount(self) -> int:
        """This read-only attribute specifies the number of rows that the 
            last `.execute*()` produced.

        Returns:
            int: Number of rows. `-1` if in case no `.execute*()` has been performed 
                 on the cursor or the rowcount of the last operation cannot be 
                 determined by the interface.
        """
        return self._rowcount
    
    @property
    def lastrowid(self) -> Optional[int]:
        """This read-only attribute provides the rowid of the last modified row.

        Most Notion API calls return an object with an id, which is used as rowid. 
        If the operation does not set a rowid, this attribute is set to `None`.

        The semantics of `.lastrowid` are undefined in case the last executed statement 
        modified more than one row, e.g. when using `INSERT` with `.executemany()`.
        `.lastworid` return a 128-bit integer representation of the object id, which can be 
        used to access Notion objects.

        Example:
        >>> object_id = str(uuid.UUID(cursor.lastrowid)))
        >>> print(object_id)
        680dee41-b447-451d-9d36-c6eaff13fb46

        Returns:
            Optional[int]: A 128-bit integer representing the UUID object id or `None`. 
        """
        if self._lastrowid == 'UNDEF':
            return None
        
        return uuid.UUID(self._lastrowid).int

    def _parse_object(self, obj: Dict[str, Any]) -> List[Tuple[str, str, Any]]:
        object_ = self._result_set['object']
        if not object_ in ['page', 'database']:
            raise InternalError(
                f'Unexpected object: {object_}. '
                'Only "page" or "database" objects supported.'
            )
    
        # Single database or page object returned by one of the following API calls: 
        # databases.retrieve or pages.retrieve.
        row = [
            ('id', 'object_id', self._result_set['id']),
            ('object', 'object_type', object_),
            # database objects only have a 'title' key
            ('title', 'title', obj['title'] if obj['title'] else '')  
        ]

        properties = obj.get("properties", {})
        for column_name, column_data in properties.items():
            db_type = column_data.get("type")
            # Extract value based on type
            if db_type == "number":
                value = column_data.get("number", "")
            elif db_type == "rich-text":
                items = column_data.get("richt_text", [])
                value = items[0]["text"]["content"] if items else ""
            elif db_type == "title":
                items = column_data.get("title", [])
                value = items[0]["text"]["content"] if items else ""
            else:
                value = ""

            row.append((column_name, db_type, str(value)))

        return row
        


    def fetchall(self) -> List[List[Tuple[str, str, Any]]]:

        results = []

        if self._result_set['object'] == 'list':
            # Multiple objects returned by the API call: databases.query.
            # Parsing requires constructing a list of rows
            results = self._result_set.get("results", [])

        else:
            # Single page or database object returned by the API calls: 
            # pages.create, pages.query, databases.create, databases.query with filters
            results = [self._result_set]

        if len(results) == 0:
            # no results returned by the operation execution
            return []

        # Parse the objects in the result set
        rows = []

        for page_or_database in results:
            row = self._parse_object(page_or_database)
            rows.append(row)

        self._rowcount = len(rows)
        self._lastrowid = rows[-1][0][-1]  # id object is always the first, the UUID id the last   
        return rows
    
    def execute(self, operation: Dict[str, Any], parameters: Dict[str, Any]) -> Self:
        """Prepare and execute a database operation (query or command).

        The `execute()` methods implements a return interface to enable concatenating 
        calls on `Cursor` methods.

        Examples:
            Create a new page as child of an exisisting database:

            >>> operation = {'endpoint': 'pages', 'request': 'create'}
            >>> parameters = {
            >>>    'parent': {
            >>>         'type': 'database_id', 
            >>>         'database_id': 'd9824bdc-8445-4327-be8b-5b47500af6ce'
            >>>     },
            >>>     'properties': {
            >>>         'id': {'number': 1},
            >>>        'name': {'title': {'text': {'content': 'Isaac Newton'}}},
            >>>        'grade': {'rich_text': {'text': {'content': 'B'}}}
            >>>    }
            >>> }
            >>> cursor = Cursor()
            >>> assert cursor.execute(operation, parameters).fetchall() == []

        Args:
            operation (Dict[str, Any]): A dictionary containing the Notion API request to be executed
            parameters (Dict[str, Any]): A dictionary containing the payload for the Notion API request

        Raises:
            InterfaceError: "properties" object not specified in parameters
            InterfaceError: "parent" object not specified in parameters
       
        Returns:
            Self: This `Cursor` instance
        """
        
        object_ = {}
        try:
            object_ = self._client(
                operation['endpoint'],
                operation['request'],
                parameters
            )
        except KeyError as ke:
            raise InterfaceError(f"Missing required key in operation dict: {ke.args[0]}")
        
        except NotionError as ne:
            raise InterfaceError(
                f'NotionError("{self._client.__class__.__name__}"): {ne}'
            ) from ne
        
        self._result_set = object_ 
        return self
