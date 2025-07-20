from typing import Any, Dict, List, Literal, Optional, Self, Tuple
import uuid

from normlite.notion_sdk.client import AbstractNotionClient, NotionError

DBAPIParamStyle = Literal[
    'qmark',     
    'numeric',   
    'named',     
    'format',
    'pyformat'
]
"""Public type for param style used by the cursor."""

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
        self._paramstyle: DBAPIParamStyle = 'named'

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

    def _parse_object(self, obj: Dict[str, Any]) -> List[Tuple[str, str, Any]]:
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
            [('object', 'otype', 'value'), ('id', 'oid', 'bc1211ca-e3f1-4939-ae34-5260b16f627c'),
            ('title', 'text', 'students'), ('id', 'number', ''), ('name', 'title', ''),
            ('grade', 'rich_text', '')]

            >>> # parse page object returned from pages.create  

        Args:
            obj (Dict[str, Any]): _description_

        Raises:
            InternalError: _description_
            InterfaceError: _description_
            InterfaceError: _description_
            InterfaceError: _description_

        Returns:
            List[Tuple[str, str, Any]]: _description_
        """
        object_ = obj['object']
        if not object_ in ['page', 'database']:
            raise InternalError(
                f'Unexpected object: {object_}. '
                'Only "page" or "database" objects supported.'
            )
    
        oid = obj.get('id', None)
        if not oid:
            raise InterfaceError(f'Missing object id in: {obj}')
        
        # Single database or page object returned by one of the following API calls: 
        # databases.retrieve or pages.retrieve.
        title = obj.get('title', None)
        row = [
            ('id', 'object_id', oid),
            ('object', 'object_type', object_),
            # database objects only have a 'title' key
            ('title', 'title', title if title else '')  
        ]

        properties = obj.get("properties", {})
        for column_name, column_data in properties.items():
            db_type = column_data.get("type")
            # Extract value based on type
            if db_type == "number":
                value = column_data.get("number", "")
            elif db_type == "rich_text":
                items = column_data.get("richt_text", None)
                if not items and object_ == 'page':
                    # pages must have a "rich_text" key
                    # databases have not "rich_text" key
                    raise InterfaceError(f'Expected "rich_text" key in property value object: {column_data}')
                    
                value = items[0]["text"]["content"] if items else ""
            elif db_type == "title":
                items = column_data.get('title', None)
                if not items and object_ == 'page':
                    # pages must have a "title" key
                    # databases have not "title" key
                    raise InterfaceError(f'Expected "title" key in property value object: {column_data}')

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

        Parameters may be provided as a mapping and will be bound to variables in the operation.
        In Notion, the use of placeholders for variable is redundant, since the properties to 
        which the values are to be bound are known by name.
        In the current implementation, the parameter style implemented in the cursor is:
        `named`.
        The `execute()` methods implements a return interface to enable concatenating 
        calls on `Cursor` methods.

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
            >>> _ = cursor.fetchall()
            >>> assert cursor.rowcount == 1  # the object created

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
        
        self._result_set = object_ 
        return self
    
    def _bind_parameters(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
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
                


             
                




