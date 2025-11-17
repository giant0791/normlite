normlite.connection
===================

.. py:module:: normlite.connection




Module Contents
---------------

.. py:class:: Connection

   Provide high level API to a connection to Notion databases.

   This class delegates the low level implementation of its methods to the DBAPI counterpart
   :class:`dbapi2.Connection`.

   .. versionadded:: 0.7.0



   .. py:method:: execute(stmt: normlite.sql.base.Executable, parameters: Optional[dict]) -> normlite.cursor.CursorResult

      Execute an SQL statement.

      This method executes both DML and DDL statements in an enclosing (implicit) transaction.
      When it is called for the first time, it sets up the enclosing transaction.
      All subsequent calls to this method add the statements to the enclosing transaction.
      Use either :meth:`commit()` to commit the changes permanently or :meth:`rollback()` to
      rollback.

      .. note::

         **Non-mutating** statements like ``SELECT`` returns their result immediately after the
         :meth:`Connection.execute()` returns. All **mutating** statements like ``INSERT``
         (see :class:`normlite.sql.dml.Insert`),``UPDATE`` or ``DELETE`` return an
         **empty** result immediately.

      .. important::

         The cursor result object associated with the last :meth:`Connection.execute()` contains
         a list of all result sets of the statements executed within the enclosing transaction.

      :param stmt: The statement to execute.
      :type stmt: SqlNode
      :param parameters: An optional dictionary containing the parameters to be
                         bound to the SQL statement.
      :type parameters: Optional[dict]

      :returns: The result of the statement execution as cursor result.
      :rtype: CursorResult

      .. versionadded:: 0.7.0




   .. py:method:: commit() -> None


   .. py:method:: rollback() -> None


