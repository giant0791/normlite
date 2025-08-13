from __future__ import annotations
from abc import ABC, abstractmethod
import pdb
from typing import Optional
from unittest.mock import patch
from uuid import UUID

import pytest
from normlite.cursor import CursorResult
from normlite.engine import create_engine
from normlite.notiondbapi.dbapi2 import DBAPIExecuteParameters, Cursor
from normlite.sql.sql import CreateTable, SqlNode, SqlToJsonVisitor, text

class Executable(ABC):
    """Provide the base class for proxy objects to execute SQL nodes.

    All SQL nodes can be devided into:
        * pure fetchers (fetch the where-clause matching rows): ``SELECT``
        * pure updaters(update the database state): ``CREATE TABLE``
        * fetchers-updaters(first fetch and then update): ``DELETE``, ``UPDATE``, 
          ``ALTER TABLE``, ``DROP TABLE`` (fetch database id)  
    """
    def __init__(
            self, 
            connection: Connection, 
            stmt: SqlNode,
            parameters: Optional[DBAPIExecuteParameters] = None
    ):
        self.connection = connection
        self.stmt = stmt

        self._parameters = dict(params=parameters) if parameters else dict(params={})
        """The compiled parameters (incl. bindings)."""

        self._parameters.update(dict(payload={}))

        self._operation = dict()
        """The compiled operation."""

        self._result: Cursor = None
        """The result of the operation."""

    @property
    def operation(self) -> dict:
        """Provide the compiled operation.

        This read-only attribute holds the result of the compilation for the operation part.
        It delivers the compiled JSON code after the :meth:`compile()` has been called.

        The returned mapping has the keys ``"endpoint"`` and ``"request"``.
        
        Example::

            create_table.compile()
            print(create_table.operation)    # {"endpoint": "databases", "request": "create"}

        Returns:
            dict: The compiled JSON code or ``{}``, if :meth:`compile()` has not previously been called.  
        """
        return self._operation
    
    @property
    def parameters(self) -> dict:
        """Provide the compiled parameters (incl. bindings).

        This read-only attribute holds the result of the compilation for the parameter part.
        It delivers the compiled JSON code after the :meth:`compile()` has been called.

        The returned mapping has the keys ``"payload"`` and ``"params"`` (the parameter bidings mapping).
        See also :meth:`notiondbapi.dbapi2.Cursor.execute()`.

        Example::

            create_table.compile()
            print(create_table.parameters)
            {
                "payload": {
                    "properties": {
                        "id": {
                            "number": ":id"
                        }
                }, 
                "parameters": {}     
            }

            insert.compile()
            print(insert.parameters)
                'payload': {
                    'properties': {
                        'id': {'number': ':id'},       
                        'name': {'title': [{'text': {'content': ':name'}}]},
                        'grade': {'rich_text': [{'text': {'content': ':grade'}}]}
                    },
                    'parent': parent
                },
                'params': {                   # bindings provided by the parameters argument in ``__init__()``
                    'id': 1,                            
                    'name': 'Isaac Newton',
                    'grade': 'B'
                 }
            }


        Returns:
            dict: The compiled JSON code or ``{}``, if :meth:`compile()` has not previously been called.  
        """
        return self._parameters

    @abstractmethod
    def compile(self) -> None:
        raise NotImplementedError
    
    @abstractmethod
    def _fetch_impl(self) -> None:
        """Hook to implement the fetch part of the execution.

        Pure fetchers like ``SELECT`` must set the result attribute in their implementation.
       """
        raise NotImplementedError
    
    @abstractmethod
    def _update_impl(self) -> None:
        """Hook to implement the update part of the execution.

        Pure updaters like ``CREATE TABLE`` and updaters such as ``DELETE`` set the result attribute
        in their implementation.
       """
        raise NotImplementedError
    
    def execute(self) -> None:
        """Execute the SQL node.
        """
        self.compile()
        self._fetch_impl()
        self._update_impl()

    def get_result(self) -> CursorResult:
        """Provide the execution result

        Returns:
            CursorResult: The proxy object to the execution result. Returns ``None`` if
            :meth:`execute()` was not called.
        """
        return self._result


class CreateTable(Executable):
    def __init__(
            self, 
            connection: Connection, 
            stmt: SqlNode,
            parameters: Optional[DBAPIExecuteParameters] = None
    ):
        if not isinstance(stmt, CreateTable):
            raise TypeError(f'Unexpected SQL node type: {stmt.__class__.__name__}')
        
        super().__init__(connection, stmt, parameters)

    def compile(self):
        """Compile the node to an executable JSON object.

        Subclasses use this method to create the dictionary representing the operation.
        The following example shows the generated JSON code for the SQL statement ``CREATE TABLE``:

        .. code-block:: python
        
            print(create_table.operation)
            {
                "endpoint": "databases",
                "request": "create",
            }

            print(create_table.parameters)
            {
                "payload": {
                    "title": [
                        {
                            "type": "text",
                            "text": {"content": "students"}
                        }
                    ],
                    "properties": {
                        "studentid": {"number": {}},
                        "name": {"title": {}},
                        "grade": {"rich_text": {}}
                    }
                },
                "parameters": {}
            }
        
        """
        visitor = SqlToJsonVisitor()
        self._operation['endpoint'] = 'databases'
        self._operation['request'] = 'create'
        self._parameters['payload'].update(self.stmt.accept(visitor))

    def _fetch_impl(self) -> None:
        ...

    def _update_impl(self) -> None:
        # execute the operation by calling the underlying DBAPI 2 cursor
        self.connection.dbapi_cursor.execute(self.stmt.operation, self.stmt.parameters)

        # create the cursor result proxy object
        # TODO: construct the CursorResultMetaData from dbapi_cursor.description
        self._result = CursorResult(self.connection.dbapi_cursor, [('object',)('id',)])


class Connection():
    def __init__(self, dbapi_cursor: Cursor):
        self.dbapi_cursor = dbapi_cursor

    def execute(self, stmt: SqlNode, parameters: Optional[DBAPIExecuteParameters] = None) -> CursorResult:
        """Provide entry-point for SQL node execution.  

        Args:
            stmt (SqlNode): The SQL statement to be executed as complete AST.   
            parameters (Optional[dict], optional): The values to be bound as parameters. Defaults to None.

        Returns:
            CursorResult: The result of the execution.
        """
        exec = Executable(self, stmt, parameters)
        exec.execute()
        return exec.get_result()

def test_compile_create_table(dbapi_cursor: Cursor):
    expected_params = {
        'params': {}, 
        'payload': {
            'title': [{'type': 'text', 'text': {'content': 'students'}}], 
            'properties': {
                'id': {'number': {}}, 
                'name': {'title': {}}, 
                'grade': {'rich_text': {}}
            }
        }
    }
    stmt = text("create table students (id int, name title_varchar(255), grade varchar(1))")
    conn = Connection(dbapi_cursor)
    exec = Executable(conn, stmt)
    exec.compile()

    assert exec.operation == {"endpoint": "databases", "request": "create"}
    assert exec.parameters == expected_params

def test_compile_insert(dbapi_cursor: Cursor):
    pass


@patch("normlite.notion_sdk.client.uuid.uuid4")
@pytest.mark.skip
def test_create_table_executable(mock_uuid4, dbapi_cursor: Cursor):
    # mock the uuid4 function for reproduceable id
    mock_uuid4.return_value = UUID("33333333-3333-3333-3333-333333333333")
    expected = (UUID("33333333-3333-3333-3333-333333333333").int,)

    stmt = text("create table students (id int, name title_varchar(255), grade varchar(1))")
    conn = Connection(dbapi_cursor)
    exec = Executable(conn, stmt)
    pdb.set_trace()
    exec.execute()
    
    """
    result: CursorResult = exec.get_result()
    row = result.first()
    assert isinstance(row, tuple)
    assert len(row) == 1
    assert row == expected
    """

@pytest.mark.skip('Not ready yet')
def test_connection_execute():
    """This integration test puts all the pieces together:
    1. Use the high-level Connection class API to execute SQL statements
    2. Use the high-level CursorResult class API to fetch the rows resulting from the execution

    TODO:
    1. Add implementation for executing the Insert SQL node
    2. Add implementation for Select SQL node
    3. Add implementation for the Connection.execute() method
    4. Refactor CursorResult to reflect the new API implemented in DBAPI 2.0 Cursor (flattened sequence of tuples)
    5. Add Row.__str__() method if not implemented yet
    
    """
    expected_rows = [
        "Row('id': 1, 'name': 'Isaac Newton', 'grade': 'B')",
        "Row('id': 2, 'name': 'Galileo Galilei', 'grade': 'A')",
    ]

    # Get the connection to the intergration
    conn = Connection()

    # create a table
    conn.execute(text("create table students (id int, name title_varchar(255), grade varchar(1))"))
    
    # insert rows
    conn.execute(     
        text("insert into students (id, name, grade) values (:id, :name, :grade)"),
        [
            {"id": 1, "name": "Isaac Newton", "grade": "B"},
            {"id": 2, "name": "Galileo Galilei", "grade": "A"},
        ]
    )

    # fetch the inserted rows
    result = conn.execute(text('select id, name, grade from students'))
    rows = result.fetchall()

    for rowidx, row in enumerate(rows):
        assert str(row) == expected_rows[rowidx]

@pytest.mark.skip('Engine.connect() not implemented yet.')
def test_connection_from_engine():
    """Add the Connection factory to Engine based on the URI provided."""
    
    expected_rows = [
        "Row('id': 1, 'name': 'Isaac Newton', 'grade': 'B')",
        "Row('id': 2, 'name': 'Galileo Galilei', 'grade': 'A')",
    ]

    # create a proxy object to an in-memory Notion integration
    engine = create_engine("normlite:///:memory:")

    # Get the connection to the intergration
    conn = engine.connect()

    # create a table
    conn.execute(text("create table students (id int, name title_varchar(255), grade varchar(1))"))
    
    # insert rows
    conn.execute(     
        text("insert into students (id, name, grade) values (:id, :name, :grade)"),
        [
            {"id": 1, "name": "Isaac Newton", "grade": "B"},
            {"id": 2, "name": "Galileo Galilei", "grade": "A"},
        ]
    )

    # fetch the inserted rows
    result = conn.execute(text('select id, name, grade from students'))
    rows = result.fetchall()

    for rowidx, row in enumerate(rows):
        assert str(row) == expected_rows[rowidx]
