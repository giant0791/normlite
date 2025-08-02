normlite.cursor
===============

.. py:module:: normlite.cursor




Module Contents
---------------

.. py:class:: _CursorMetaData(desc: Sequence[tuple])

   Provide helper metadata structures to access raw data from low level `Cursor` DBAPI 2.0.


   .. py:attribute:: index_to_key


   .. py:attribute:: key_to_index


.. py:class:: CursorResult(cursor: normlite.notiondbapi.dbapi2.Cursor, metadata: _CursorMetaData)

   Provide pythonic high level interface to result sets from SQL statements.


   .. py:attribute:: _cursor


   .. py:attribute:: _metadata


   .. py:method:: fetchall() -> List[Row]


.. py:class:: Row(metadata: _CursorMetaData, row_data: List[Tuple[str, str, str]])

   Provide pythonic high level interface to a single SQL database row.


   .. py:attribute:: _metadata


   .. py:attribute:: _values


   .. py:method:: __getitem__(key: str) -> str


   .. py:method:: __repr__()


