normlite.cursor
===============

.. py:module:: normlite.cursor








Module Contents
---------------

.. py:exception:: ResourceClosedError

   Bases: :py:obj:`normlite.exceptions.NormliteError`


   The cursor cannot deliver rows.

   This exception is raised when a cursor (resource) cannot return rows because it is
   either closed (i.e. exhausted) or it represents the result from an SQL statement that
   does not return values (e.g. ``INSERT``).

   .. versionadded:: 0.5.0



.. py:class:: _CursorColMapRecType

   Bases: :py:obj:`NamedTuple`


   Helper record data structure to store column metadata.

   This class provides a description record to enable value and type conversions between DBAPI 2.0 rows and
   higher level class:`Row` objects.

   .. versionadded:: 0.5.0



   .. py:attribute:: column_name
      :type:  str

      The name of the column.


   .. py:attribute:: index
      :type:  int

      The column position in the description (first column --> index = 0).


   .. py:attribute:: column_type
      :type:  str

      Currently, a string denoting the column type for conversion in the Python type system.


.. py:data:: _ColMapType

.. py:class:: _NoCursorResultMetadata

   .. py:attribute:: returns_row
      :type:  ClassVar[bool]
      :value: False



   .. py:method:: _raise_error() -> NoReturn


   .. py:property:: keys
      :type: Sequence[str]



   .. py:property:: key_to_index
      :type: Mapping[str, int]



   .. py:property:: index_for_key
      :type: Mapping[int, str]



.. py:data:: _NO_CURSOR_RESULT_METADATA

.. py:class:: CursorResultMetaData(desc: Sequence[tuple])

   Bases: :py:obj:`_NoCursorResultMetadata`


   Provide helper metadata structures to access row data from low level
   :class:`normlite.notionbdapi.dbapi2.Cursor` DBAPI 2.0.

   .. versionadded:: 0.5.0



   .. py:attribute:: returns_row
      :value: True


      The associated cursor returns rows (e.g. ``SELECT`` statement).


   .. py:attribute:: _colmap
      :type:  _ColMapType

      Mapping between column name and its description record :class:`_CursorColMapRecType`.


   .. py:attribute:: _key_to_index
      :type:  Mapping[str, int]

      Mapping between column name and its positional index.


   .. py:attribute:: _index_for_key
      :type:  Mapping[int, str]

      Mapping between column positional index and its name.


   .. py:attribute:: _keys
      :type:  Sequence[str]

      A sequence containing all the column names.


   .. py:property:: keys
      :type: Sequence[str]


      Provide all the column names for the described row.


   .. py:property:: key_to_index
      :type: Mapping[str, int]


      Provide the mapping between column name and its positional index.


   .. py:property:: index_for_key
      :type: Mapping[int, str]


      Provid the mapping beween the positional index of a column and its name.


.. py:class:: BaseCursorResult(cursor: normlite.notiondbapi.dbapi2.Cursor)

   Provide pythonic high level interface to result sets from SQL statements.

   This class is an adapter to the DBAPI cursor (see :class:`normlite.notiondbapi.dbapi2.Cursor`)
   representing state from the DBAPI cursor. It provides a high level API to
   access returned database rows as :class:`Row` objects.

   .. note::

      If a closed DBAPI cursor is passed to the init method, this cursor result automatically
      transitions to the closed state.

   .. versionchanged:: 0.5.0
       The fetcher methods now check that the cursor metadata returns row prior to execution.
       This ensures that no calls to ``None`` objects are issued.

   .. versionchanged:: 0.7.0   This class has been renamed to reflect the base API and behaviour
       of results. Now, the base implementation of a cursor result can be composed in the derived
       subclass :class:`CursorResult`. Additionally, the :meth:`close()` is now available to close
       the underlying DBAPI cursor. Therefore, all methods returning rows now check whether the cursor
       is closed and raise the exc:`ResourceClosedError`. The attribute :attr:`CursorResult.return_rows`
       of closed cursor result always return ``False``.



   .. py:attribute:: _cursor

      The underlying DBAPI cursor.


   .. py:attribute:: _closed
      :value: False


      ``True`` if this cursor result is closed.


   .. py:property:: returns_rows
      :type: bool


      ``True`` if this :class:`CursorResult` returns zero or more rows.

      This attribute signals whether it is legal to call the methods: :meth:`CursorResult.fetchone()`,
      :meth:`fetchall()`, and :meth:`fetchmany()`.

      The truthness of this attribute is strictly in sync with whether the underlying DBAPI cursor
      had a :attr:`normlite.notiondbapi.dbapi2.Cursor.description`, which always indicates the presence
      of result columns.

      .. note::

         A cursor that returns zero rows (e.g. an empty sequence from :meth:`CursorResult.all()`)
         has still a :attr:`normlite.notiondbapi.dbapi2.Cursor.description`, if a row-returning
         statement was executed.

      .. versionadded:: 0.5.0

      :returns: ``True`` if this cursor result returns zero or more rows.
      :rtype: bool


   .. py:method:: __iter__() -> Iterator[Row]

      Provide an iterator for this cursor result.

      .. versionadded:: 0.5.0

      .. versionchanged:: 0.7.0   Raise :exc:`ClosedResourceError` if it was previously closed.

      :raises ClosedResourceError: If it was previously closed.

      :Yields: *Iterator[Row]* -- The row iterator.



   .. py:method:: one() -> Row

      Return exactly one row or raise an exception.

      .. versionadded:: 0.5.0

      .. versionchanged:: 0.7.0   Raise :exc:`ClosedResourceError` if it was previously closed.

      :raises NoResultFound: If no row was found when one was required.
      :raises MultipleResultsFound: If multiple rows were found when exactly one was required.
      :raises ClosedResourceError: If it was previously closed.

      :returns: The one row required.
      :rtype: Row



   .. py:method:: all() -> Sequence[Row]

      Return all rows in a sequence.

      This method closes the result set after invocation. Subsequent calls will return an empty sequence.

      .. versionadded:: 0.5.0

      .. versionchanged:: 0.7.0   Raise :exc:`ClosedResourceError` if it was previously closed.

      :raises ClosedResourceError: If it was previously closed.

      :returns: All rows in a sequence.
      :rtype: Sequence[Row]



   .. py:method:: first() -> Optional[Row]

      Return the first row or ``None`` if no row is present.

      .. note:: This method closes the result set and discards remaining rows.

      .. versionadded:: 0.5.0

      .. versionchanged:: 0.7.0   Raise :exc:`ClosedResourceError` if it was previously closed.

      :raises ClosedResourceError: If it was previously closed.

      :returns: The first row in the result set or ``None`` if no row is present.
      :rtype: Optional[Row]



   .. py:method:: fetchone() -> Optional[Row]

      Fetch the next row.

      When all rows are exhausted, returns ``None``.

      .. versionadded:: 0.5.0

      :returns: The row object in the result.
      :rtype: Optional[Row]



   .. py:method:: fetchall() -> Sequence[Row]

      Synonim for :class:`CursorResult.all()` method.

      .. versionchanged:: 0.5.0
          This method has been refactored as a wrapper around :meth:`CursorResult.all()`.
          This ensures consistent behavior across synomin methods.

      :returns: The sequence of row objects. Empty sequence if the cursor result is closed.
      :rtype: Sequence[Row]



   .. py:method:: fetchmany() -> Sequence[Row]
      :abstractmethod:


      Fetch many rows.

      When all rows are exhausted, returns an empty sequence.

      .. versionadded:: 0.5.0

      :raises NotImplementedError: Method not implemented yet.

      :returns: All rows or an empty sequence when exhausted.
      :rtype: Sequence[Row]



   .. py:method:: close() -> None


   .. py:method:: _check_if_closed() -> None

      Raise ResourceClosedError if this cursor result is closed.



.. py:class:: CursorResult(dbapi_cursor: normlite.notiondbapi.dbapi2.CompositeCursor)

   Bases: :py:obj:`BaseCursorResult`


   Prototype for new and refactored CursorResult class with composite cursor feature.

   .. versionadded:: 0.7.0



   .. py:attribute:: _dbapi_cursor


   .. py:attribute:: _current_result


   .. py:method:: next_result() -> bool

      Advance to the next cursor, if available.



   .. py:method:: one() -> Row

      Return exactly one row or raise an exception.

      .. versionadded:: 0.5.0

      .. versionchanged:: 0.7.0   Raise :exc:`ClosedResourceError` if it was previously closed.

      :raises NoResultFound: If no row was found when one was required.
      :raises MultipleResultsFound: If multiple rows were found when exactly one was required.
      :raises ClosedResourceError: If it was previously closed.

      :returns: The one row required.
      :rtype: Row



   .. py:method:: all() -> Sequence[Row]

      Return all rows in a sequence.

      This method closes the result set after invocation. Subsequent calls will return an empty sequence.

      .. versionadded:: 0.5.0

      .. versionchanged:: 0.7.0   Raise :exc:`ClosedResourceError` if it was previously closed.

      :raises ClosedResourceError: If it was previously closed.

      :returns: All rows in a sequence.
      :rtype: Sequence[Row]



.. py:class:: Row(metadata: CursorResultMetaData, row_data: tuple)

   Provide pythonic high level interface to a single SQL database row.

   .. versionchanged:: 0.5.0
       :class:`Row` has been significantly extended to provide iteratable capabilities
       and a mapping-sytle object to access the values of the columns returned in the row.



   .. py:attribute:: _metadata

      The metadata object to process raw rows.


   .. py:attribute:: _values

      Thew column values.


   .. py:method:: __getitem__(key_or_index: Union[str, int]) -> Any

      Provide keyed and indexed access to the row values.

      Providing this method enables row object to be iterated::

          >>> for value in row:
          ...     print(f"{value = }")
          value = '680dee41-b447-451d-9d36-c6eaff13fb45'
          value = False
          value = False
          value = 12345
          value = 'B'
          value = 'Isaac Newton'

      .. versionchanged:: 0.5.0
          Now, it supports both keyes and indexed access. Error handling is more
          robust and consistent.

      :param key_or_index: The value's key (column name) or index (column positional).
      :type key_or_index: Union[str, int]

      :raises IndexError: If index is out or range.
      :raises KeyError: If row has no column named ``key_or_index``.
      :raises TypeError: If the provided index is neither ``str`` (column name) or ``int`` (column index).

      :returns: The value for the column the key or index has been provided.
      :rtype: Any



   .. py:method:: mapping() -> dict

      Provide the mapping object for this row.

      .. versionadded:: 0.5.0




   .. py:method:: __repr__()


.. py:class:: RowMapping(row: Row)

   Bases: :py:obj:`Mapping`\ [\ :py:obj:`str`\ , :py:obj:`Any`\ ]


   Helper to construct mapping objects for rows.

   :class:`RowMapping` provides a dedicated mapping implementation for column name, column value pairs.

   .. versionadded:: 0.5.0



   .. py:attribute:: _row

      The underlying row object.


   .. py:attribute:: _mapping

      The mapping object created from the row object.


   .. py:method:: __getitem__(key)


   .. py:method:: __iter__()


   .. py:method:: __len__()


   .. py:method:: __eq__(other: Any) -> bool


   .. py:method:: keys()

      D.keys() -> a set-like object providing a view on D's keys



   .. py:method:: values()

      D.values() -> an object providing a view on D's values



   .. py:method:: items()

      D.items() -> a set-like object providing a view on D's items



