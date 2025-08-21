normlite.sql.dml
================

.. py:module:: normlite.sql.dml






Module Contents
---------------

.. py:class:: SQLCompiler

   Provide the central compiler for all SQL executables.


   .. py:attribute:: _stmt_map


   .. py:method:: compile_insert(ins_stmt: Insert) -> dict


.. py:class:: Insert(table: normlite.sql.sql.CreateTable)

   Bases: :py:obj:`normlite.sql.base.Executable`


   Provide an insert statement to add rows to the associated table.

   This class respresents an SQL ``INSERT`` statement. Every insert statement is associated
   to the table it adds rows to.

   .. versionadded:: 0.7.0



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


.. py:function:: insert(table: normlite.sql.sql.CreateTable) -> Insert

   Construct an insert statement.

   This class constructs an SQL ``INSERT`` statement capable of inserting rows
   to this table.

   :returns: A new insert statement for this table.
   :rtype: Insert


