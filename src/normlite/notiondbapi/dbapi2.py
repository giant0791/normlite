from typing import Any, Dict, List, Self, Tuple

class Error(Exception):
    """Base class of all other error exceptions.

    It can be used this to catch all errors with one single `except` statement. 
    """
    pass

class InterfaceError(Error):
    """Exception raised for errors that are related to the database interface.

    For example, it is raised when an operation is not supported by the Notion API.
    """

class Cursor:
    """Implement the `Cursor` class according to the DBAPI 2.0 specification."""
    def __init__(self):
        self._result_set = {}  # set externally or by another method
        self._rowcount = -1

    @property
    def rowcount(self) -> int:
        return self._rowcount

    def fetchall(self) -> List[Tuple[str, str, Any]]:
        results = self._result_set.get("results", [])
        self._rowcount = len(results)
        if self._rowcount == 0:
            # no results returned by the operation execution
            return []

        # Init rows list containing the extracted Notion objects found in "results" 
        rows = []

        for page in results:
            row = []
            properties = page.get("properties", {})
            for column_name, column_data in properties.items():
                db_type = column_data.get("type")

                # Extract value based on type
                if db_type == "number":
                    value = column_data.get("number", "")
                elif db_type == "rich-text":
                    items = column_data.get("richt-text", [])
                    value = items[0]["text"]["content"] if items else ""
                elif db_type == "title":
                    items = column_data.get("title", [])
                    value = items[0]["text"]["content"] if items else ""
                else:
                    value = ""

                row.append((column_name, db_type, str(value)))

            rows.append(row)

        return rows
    
    def execute(self, operation: Dict[str, Any], parameters: Dict[str, Any]) -> Self:
        """Prepare and execute a database operation (query or command).

        The `execute()` methods implements a return interface to enable concatenating 
        calls on `Cursor` methods.

        Examples of usage for creating a new page
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
        
        # Fake the execution
        endpoint = operation.get('endpoint', '')
        request = operation.get('request', '')
        if  endpoint != 'pages':
            raise InterfaceError(f'API endpoint "{endpoint}" unknown or not supported.')
        
        if request != 'create':
            raise InterfaceError(f'API request "{request}" unknown or not supported.')

        if endpoint == 'pages' and request == 'create':
            # Construct payload for notion_sdk call
            parent = parameters.get('parent', 'no_parent')
            properties = parameters.get('properties', 'no_properties')
            if properties == 'no_properties':
                raise InterfaceError(
                    f'"properties" object not specified in parameters for: {endpoint}.{request}. '
                    'Please specify the properties for this new entity.'
                )

            if  parent == 'no_parent':
                raise InterfaceError(
                    f'"parent" object not specified in parameters for: {endpoint}.{request}. '
                    'Please specify the parent object this entity belongs to.'
                )
            try:
                # call here the notion_dk client
                payload = {
                    'properties': parameters
                }
                pass
            except:
                pass

            # Creating a new page (inserting a new row) does not return any result set
            self._result_set = {}
            self._rowcount = 0
 
        return self
