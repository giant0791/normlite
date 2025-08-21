normlite.notiondbapi.dbapi2
===========================

.. py:module:: normlite.notiondbapi.dbapi2








Module Contents
---------------

.. py:data:: DBAPIParamStyle

   Public type for param style used by the cursor.

.. py:type:: DBAPIExecuteParameters
   :canonical: dict


   Type for parameters passed to for SQL statement execution.

.. py:exception:: Error

   Bases: :py:obj:`Exception`


   Base class of all other error exceptions.

   It can be used this to catch all errors with one single ``except`` statement.


.. py:exception:: InterfaceError

   Bases: :py:obj:`Error`


   Exception raised for errors that are related to the database interface.

   For example, it is raised when an operation is not supported by the Notion API.


.. py:exception:: DatabaseError

   Bases: :py:obj:`Error`


   Exception raised for errors that are related to the database.



.. py:exception:: InternalError

   Bases: :py:obj:`Error`


   Exception raised when the database encounters an internal error.

   For example, the cursor is not valid anymore, an unexpected object has been returned by
   the underlying database driver (i.e. the Notion API).


.. py:exception:: OperationalError

   Bases: :py:obj:`DatabaseError`


   Exception raised for errors that are related to the database’s operation and not necessarily under
   the control of the programmer.

   Example situations are an unexpected disconnect occurs, the data source name is not found,
   a transaction could not be processed, a memory allocation error occurred during processing, etc.

   .. versionadded:: 0.7.0



.. py:class:: BaseCursor(client: normlite.notion_sdk.client.AbstractNotionClient)

   Provide database base cursor functionalty according to the DBAPI 2.0 specification (PEP 249).

   .. note::

      the :class:`BaseCursor` does not support transaction awareness. Use :class:`Cursor` for fully
      DBAPI 2.0 compliant cursor.

   .. versionadded:: 0.7.0



   .. py:attribute:: _client

      The client implementing the Notion API.


   .. py:attribute:: _result_set
      :value: None


      The result set returned by the last :meth:`.execute()`. It is set by :meth:`._parse_result_set()`


   .. py:attribute:: _paramstyle
      :type:  DBAPIParamStyle
      :value: 'named'


      The default parameter style applied.


   .. py:attribute:: _description
      :type:  tuple
      :value: None


      Provide information describing one result column.


   .. py:attribute:: _closed
      :value: False


      Whether this cursor is closed. Always ``False`` after initialitation.


   .. py:property:: description
      :type: tuple


      Provide the cursor description.

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


   .. py:property:: rowcount
      :type: int


      



      This read-only attribute specifies the number of rows that
         the last :meth:`.execute()` produced.

      :returns:

                Number of rows. `-1` if in case no :meth:`.execute()` has been performed
                     on the cursor or the rowcount of the last operation cannot be
                     determined by the interface.
      :rtype: int


   .. py:property:: lastrowid
      :type: Optional[int]


      This read-only attribute provides the rowid of the last modified row.

      Most Notion API calls return an object with an id, which is used as rowid.
      If the operation does not set a rowid, this attribute is set to ``None``.

      .. note::

         ``normlite`` considers both inserted and updated rows as modified rows.
         This means that :attr:`.lastrowid` returns non ``None`` values after either
         an ``INSERT`` or ``UPDATE`` statement.
         
         ``normlite`` also defines semantics of ::attr::`.lastrowid` in case the last executed
         statement modified more than one row, e.g. when using ``INSERT`` with :meth:`.executemany()` or
         ``UPDATE`` and its ``SELECT`` clause returns multiple rows.
         
         :attr:`.lastrowid` returns a 128-bit integer representation of the object id, which can be
         used to driectly access Notion objects.

      .. rubric:: Example

      >>> object_id = str(uuid.UUID(int=cursor.lastrowid))
      >>> print(object_id)
      680dee41-b447-451d-9d36-c6eaff13fb46

      :returns: A 128-bit integer representing the UUID object id or `None`.
      :rtype: Optional[int]


   .. py:property:: paramstyle
      :type: DBAPIParamStyle


      String constant stating the type of parameter marker formatting expected by the interface.


      Supported values are:
          ``named``: Named mark style, e.g. ``WHERE name=:name``

      Note
          It always returns ``named``.

      :returns: The paramstyle currently in use.
      :rtype: DBAPIParamStyle


   .. py:method:: _parse_object(obj: Dict[str, Any]) -> Tuple[tuple]

      Parse a Notion database or page from a list object into a list of tuples.

      .. rubric:: Examples

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

      :param obj: _description_
      :type obj: Dict[str, Any]

      :raises ValueError: If ``'object'`` is neither ``'page'`` or ``'database'``.

      :returns: The tuple representing the successfully parsed row.
      :rtype: Tuple[str, str, Any]



   .. py:method:: _parse_result_set(returned_obj: Dict[str, Any]) -> None

      Parse the JSON object returned by the command execution into the cursor's result set.




   .. py:method:: __iter__() -> Iterator[tuple]

      Make cursors compatible with the iteration protocol.

      .. versionchanged:: 0.5.0
          Calling this method on a closed cursor raises the :exc:`Error`.

      :raises Error: If the cusors is closed.

      :Yields: *Iterator[Iterable[tuple]]* -- The next row in the result set.



   .. py:method:: fetchone() -> Optional[tuple]

      Fetch the next row of a query result set.

      This method returns the next row or ``None`` when no more data is available.

      .. note::

         The current implementation guarantees that a call to this method will only move
         the associated cursor forward by one row.

      .. versionchanged:: 0.5.0
          Calling this method on a closed cursor raises the :exc:`Error`.

      :raises Error: If the cusors is closed.
      :raises InterfaceError: If the previous call to :meth:`.execute()` did not produce any result set
          or no call was issued yet.

      :returns: The next row as single tuple, or an empty tuple when no more data is available.
      :rtype: Optional[tuple]



   .. py:method:: fetchall() -> List[tuple]

      Fetch all rows of this query result.

      This method returns all the remaining rows contained in this query result as a sequence of sequences
      (e.g. a list of tuples).
      Please refer to :mod:`notiondbapi` for a detailed description of how Notion JSON objects are
      parsed and cross-compiled into Python ``tuple`` objects.

      .. important::

         After a call to the :meth:`.fetchall()` the result set is exausted (empty). Any subsequent call
         to this method returns an empty sequence.

      .. versionchanged:: 0.5.0
          Calling this method on a closed cursor raises the :exc:`Error`.

      :raises Error: If the cursor is closed.
      :raises InterfaceError: If the previous call to :meth:`.execute()` did not produce any result set
          or no call was issued yet.

      :returns: The list containing all the remaining queried rows. ``[]`` if no rows are available.
      :rtype: List[tuple]



   .. py:method:: execute(operation: dict, parameters: DBAPIExecuteParameters) -> Self

      Prepare and execute a database operation (query or command).

      Parameters may be provided as a mapping and will be bound to variables in the operation.
      In Notion, the use of placeholders for variable is redundant, since the properties to
      which the values are to be bound are known by name.
      In the current implementation, the parameter style implemented in the cursor is:
      `named`.
      The :meth:`execute()` methods implements a return interface to enable concatenating
      calls on :class:`Cursor` methods.

      .. important::

         :meth:`.execute()` stores the executed command result(s) in the internal
         result set. Always call this method prior to :meth:`Cursor.fetchone()` and :meth:`Cursor.fetchall()`,
         otherwise an :exc:`InterfaceError` error is raised.

      .. versionchanged:: 0.5.0
          Calling this method on a closed cursor raises the :exc:`Error`.

      .. rubric:: Examples

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

      :param operation: A dictionary containing the Notion API request to be executed.
      :type operation: dict
      :param parameters: A dictionary containing the payload for the Notion API request
      :type parameters: DBAPIExecuteParameters

      :raises Error: If the cursor is closed.
      :raises InterfaceError: ``"properties"`` object not specified in parameters.
      :raises InterfaceError: ``"parent"`` object not specified in parameters.

      :returns: This :class:`Cursor` instance.
      :rtype: Self



   .. py:method:: executemany(operation: dict, parameters: Sequence[dict]) -> Self
      :abstractmethod:


      Prepare a database operation (query or command) and then execute it against all parameter sequences or
      mappings found in the sequence seq_of_parameters.

      .. note::

         This method is not implemented yet.
         Calling it raises :exc:`NotImplementedError`.

      :param operation: A dictionary containing the Notion API request to be executed.
      :type operation: dict
      :param parameters: A sequence of dictionaries containint the parameters to be executed multiple times.
      :type parameters: Sequence[dict]

      :returns: This :class:`Cursor` instance.
      :rtype: Self



   .. py:method:: close() -> None

      Close the cursor now.

      The cursor will be unusable from this point forward; an Error exception
      will be raised if any operation is attempted with the cursor.

      .. versionadded:: 0.5.0



   .. py:method:: _bind_parameters(parameters: DBAPIExecuteParameters) -> dict

      Helper for binding values to the payload.



.. py:class:: Cursor(dbapi_connection: Connection)

   Bases: :py:obj:`BaseCursor`


   Transaction-aware DBAPI cursor

   This is how the new Cursor class will work in tandem with :class:`Connection`.

   .. note::

      Unfortunately, the DBAPI 2.0 does not forsee an execute() method for the Connection class.
      This leads to a suboptimal separation of concerns: The Connection clas should be responsible to manage the
      transaction and to execute operations, while Cursor should only be concerned with providing access to
      the results. In the lack of an execute() method at connection level, the Cursor class needs to have a reference
      to the connection, so it can start a new transaction on the first call to its execute() method.

   .. versionchanged:: 0.7.0



   .. py:attribute:: _dbapi_connection


   .. py:method:: execute(operation: dict, parameters: DBAPIExecuteParameters) -> Self

      Execute the operation within the currently opened transaction.

      This method is similar to :meth:`BaseCursor.execute()` with the additional feature of
      executing the operation within the currently opened transaction.
      This means that it does not execute immediately the operation, but it add the operation
      to the operations list of the opened transaction.
      Execution is deferred to the point in time when the :meth:`Connection.commit()` is called.

      :param operation: A dictionary containing the Notion API request to be executed.
      :type operation: dict
      :param parameters: A dictionary containing the payload for the Notion API request
      :type parameters: DBAPIExecuteParameters

      :raises OperationalError: If it fails to add the operation to the transaction.
      :raises InternalError: If the operation is not supported or not recognized.

      :returns: This cursor instance.
      :rtype: _type_

      .. versionadded:: 0.7.0



.. py:class:: Connection(proxy_client: flask.testing.FlaskClient, client: normlite.notion_sdk.client.AbstractNotionClient)

   Provide database base connection functionalty according to the DBAPI 2.0 specification (PEP 249).

   .. warning::

      This class is still proof-of-concept stage. It needs to be initialized with a Flask testing client (:class:`FlaskClient`).
      **DO NOT USE YET!**

   .. versionadded:: 0.7.0



   .. py:attribute:: _proxy_client


   .. py:attribute:: _client


   .. py:attribute:: _tx_id
      :type:  str
      :value: None



   .. py:attribute:: _cursor
      :type:  Cursor
      :value: None


      Classic DBAPI cursor to execute operations and fetch rows.


   .. py:attribute:: _comp_cursor
      :type:  CompositeCursor
      :value: None


      Composite cursor holding all cursors created out of committed changes in the transaction.


   .. py:attribute:: _cursors
      :type:  List[Cursor]
      :value: []



   .. py:method:: cursor(composite=False) -> Union[Cursor, CompositeCursor]

      Procure a new cursor object using the connection.

      :param composite: If ``True`` procure a :class:`normlite.notiondbapi.dbapi2.CompositeCursor`, else
      :type composite: bool, optional
      :param a `normlite.notiondbapi.dbapi2.Cursor` instance holding the last result set returned by the last:
      :param committed statement. Defaults to ``False``.:

      :returns: Either a cursor or a composite depending on the argument value.
      :rtype: Union[Cursor, CompositeCursor]

      .. versionchanged:: 0.7.0




   .. py:method:: _begin_transaction() -> None

      Begin a new transaction.



   .. py:method:: _in_transaction() -> bool

      True if the connection has already initiated a transaction.

      This method is used by the cursor to determin whether to begin a new transaction or not.



   .. py:method:: _execute_in_transaction(operation: dict, parameters: DBAPIExecuteParameters) -> None

      Execute the operation in the context of the opened transaction.

      :param operation: A dictionary containing the Notion API request to be executed.
      :type operation: dict
      :param parameters: A dictionary containing the payload for the Notion API request
      :type parameters: DBAPIExecuteParameters

      :raises OperationalError: If it fails to add the operation to the transaction.
      :raises InternalError: If the operation is not supported or not recognized.

      .. versionadded:: 0.7.0




   .. py:method:: commit() -> None

      Commit any pending transaction to the database.

      .. note::

         If the database supports an auto-commit feature, this must be initially off.
         An interface method may be provided to turn it back on.

      .. versionadded:: 0.7.0




   .. py:method:: _create_cursors(result_sets: Sequence[dict]) -> None

      Helper to populate the _cursors attribute holding the cursors to access all rows returned in the transaction.

      .. versionadded:: 0.7.0




.. py:class:: CompositeCursor(cursors: Sequence[Cursor])

   Bases: :py:obj:`Cursor`


   Extend a DBAPI cursor to manage multiple child cursors, one per result set returned
   from a multi-statement transaction commit.


   .. py:attribute:: _cursors


   .. py:attribute:: _current_index
      :value: 0



   .. py:attribute:: _current_cursor


   .. py:method:: nextset() -> bool

      Advance to the next result set if available.

      This method makes the cursor skip to the next available set, discarding any remaining rows from the current set.
      It returns ``False`` if there are no more sets or returns ``True`` and subsequent calls to the cursor.fetch*() methods
      returns rows from the next result set.



   .. py:property:: rowcount
      :type: int


      Return the row count of the current cursor.


   .. py:property:: lastrowid
      :type: int


      Return the last row id of the current cursor.


   .. py:property:: description
      :type: tuple


      Return the description of the current cursor.


   .. py:property:: paramstyle
      :type: DBAPIParamStyle


      Return the row parameter style of the current cursor.


   .. py:method:: fetchone() -> Optional[tuple]

      Fetch the next row of the current cursor's result set.



   .. py:method:: fetchall() -> List[tuple]

      Fetch all rows of the current cursor's result set.



