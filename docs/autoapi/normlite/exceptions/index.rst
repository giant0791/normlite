normlite.exceptions
===================

.. py:module:: normlite.exceptions

.. autoapi-nested-parse::

   Provide ``normlite`` specific exceptions.

   .. versionadded:: 0.5.0





Module Contents
---------------

.. py:exception:: NormliteError

   Bases: :py:obj:`Exception`


   Base exception class for all ``normlite`` exceptions.

   .. versionadded: 0.5.0


.. py:exception:: NoResultFound

   Bases: :py:obj:`NormliteError`


   Raised when exactly one result row is expected, but none was found.

   .. versionadded: 0.5.0


.. py:exception:: MultipleResultsFound

   Bases: :py:obj:`NormliteError`


   Raised if multiple rows were found when exactly one was required.

   .. versionadded: 0.5.0


.. py:exception:: DuplicateColumnError

   Bases: :py:obj:`NormliteError`


   Raised when an already existing column is added to a table.

   .. versionadded:: 0.7.0


.. py:exception:: ArgumentError

   Bases: :py:obj:`NormliteError`


   Raised when an erroneous argument is passed.

   .. versionadded:: 0.7.0


.. py:exception:: InvalidRequestError

   Bases: :py:obj:`NormliteError`


   Raised when a ``normlite`` method or function is cannot perform as requested.

   .. versionadded:: 0.7.0


