normlite.sql.dml
================

.. py:module:: normlite.sql.dml






Module Contents
---------------

.. py:class:: Insert

   Bases: :py:obj:`normlite.sql.sql.SqlNode`


   Provide the SQL ``INSERT`` node to create a new row in the specified table.

   This class provide a generative implementation of the SQL ``INSERT`` node to be executed on a
   :class:`normlite.connection.Connection`.

   Usage:
       >>> # create a new insert statement for the table students
       >>> stmt = Insert(students)
       ...
       >>> # create a new insert statement and specify the ``RETURNING`` clause.
       >>> stmt = Insert(students).returning(students.c.id, students.c.name)
       ...
       >>> # specify the values to be inserted as keyword arguments
       >>> stmt.values(id=123456, name='Isaac Newton', grade='B')
       ...
       >>> # specify the values to be inserted as dictionary
       >>> stmt.values({'id': 123456, 'name':'Isaac Newton', 'grade': 'B'})
       ...
       >>> # specify the values to be inserted as tuple
       >>> stmt.values((123456, 'Isaac Newton', 'B'))

   .. important::

      The :class:`Insert` has always by default the Notion specific columns as ``RETURNING`` clause.
      That is, the :class:`normlite.CursorResult` methods of an insert statement always returns rows with the
      columns corresponding to the Notion specific ones.
      You specify the :meth:`Insert.returning()` to get additional columns in the rows returned by the
      :class:`normlite.CursorResult` methods.

   .. note::

      The :class:`Insert`  can also be constructed without specifying the values.
      In this case, the parameters passed in the :meth:`normlite.connection.Connection.execute()` are bound
      as ``VALUES`` clause parameters at execution time.

   .. versionchanged:: 0.7.0 The old construct has been completely redesigned and refactored.
       Now, the new class provides all features of the SQL ``INSERT`` statement.



   .. py:attribute:: _values
      :type:  types.MappingProxyType
      :value: None


      The immutable mapping holding the values.


   .. py:attribute:: _table
      :type:  normlite.sql.schema.Table
      :value: None


      The table object to insert a new row to.


   .. py:attribute:: _returning
      :value: ('_no_id', '_no_archived')


      The tuple holding the


   .. py:method:: accept(visitor)

      Not supperted yet.



   .. py:method:: _set_table(table: normlite.sql.schema.Table) -> None


   .. py:method:: values(*args: Union[dict, Sequence[Any]], **kwargs: Any) -> Self

      Provide the ``VALUES`` clause to specify the values to be inserted in the new row.

      :raises ArgumentError: If both positional and keyword arguments are passes, or
          if not enough values are supplied for all columns, or if values are passed
          with a class that is neither a dictionary not a tuple.

      :returns: This instance for generative usage.
      :rtype: Self



   .. py:method:: returning(*cols: normlite.sql.schema.Column) -> Self

      Provide the ``RETURNING`` clause to specify the column to be returned.

      :raises ArgumentError: If a specified column does not belong to the table this insert statement
          is applied to.

      :returns: This instance for generative usage.
      :rtype: Self



   .. py:method:: _process_dict_values(dict_arg: dict) -> types.MappingProxyType


.. py:class:: SQLCompiler

   Provide the central compiler for all SQL executables.


   .. py:attribute:: _stmt_map


   .. py:method:: compile_insert(ins_stmt: Insert) -> dict


.. py:class:: OldInsert(table: normlite.sql.sql.CreateTable)

   Bases: :py:obj:`normlite.sql.base.Executable`


   Provide an insert statement to add rows to the associated table.

   This class respresents an SQL ``INSERT`` statement. Every insert statement is associated
   to the table it adds rows to.

   .. warning::

      This is going to be removed. Don't use!
      Use :class:`Insert` instead.


   .. py:attribute:: _table

      The table subject of the insert.


   .. py:attribute:: _values
      :type:  dict

      The mapping column name, column value.


   .. py:attribute:: _operation
      :type:  dict

      The dictionary containing the compiled operation.


   .. py:attribute:: _parameters
      :type:  dict

      The dictionary containing the compiled parameters.


   .. py:method:: prepare() -> None

      Prepare this executable for execution.

      This method is used to populate the internal structures needed for execution.
      That is compiling and constructing the operation and parameters dictionaries.



   .. py:method:: bindparams(parameters: Optional[dict]) -> None

      Bind (assign) the parameters to the insert values clause.



   .. py:method:: operation()


   .. py:method:: parameters()


.. py:function:: old_insert(table: normlite.sql.sql.CreateTable) -> OldInsert

   Construct an insert statement.

   This class constructs an SQL ``INSERT`` statement capable of inserting rows
   to this table.

   :returns: A new insert statement for this table.
   :rtype: OldInsert

   .. warning::

      This is going to be removed. Don't use!
      Use :func:`insert()` instead.


.. py:function:: insert(table: normlite.sql.schema.Table) -> Insert

   Construct an insert statement.

   This class constructs an SQL ``INSERT`` statement capable of inserting rows
   to this table.

   .. versionchanged:: 0.7.0
       Now, it uses the :class:`normlite.sql.schema.Table` as table object.

   :returns: A new insert statement for this table.
   :rtype: Insert


