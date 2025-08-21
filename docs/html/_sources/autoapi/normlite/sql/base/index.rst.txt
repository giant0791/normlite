normlite.sql.base
=================

.. py:module:: normlite.sql.base




Module Contents
---------------

.. py:class:: Executable

   Bases: :py:obj:`Protocol`


   Provide the interface for all executable SQL statements.


   .. py:method:: prepare() -> None

      Prepare this executable for execution.

      This method is used to populate the internal structures needed for execution.
      That is compiling and constructing the operation and parameters dictionaries.



   .. py:method:: bindparams(parameters: Optional[dict]) -> None


   .. py:method:: operation() -> dict


   .. py:method:: parameters() -> dict


