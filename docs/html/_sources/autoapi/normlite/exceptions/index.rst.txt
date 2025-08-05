normlite.exceptions
===================

.. py:module:: normlite.exceptions




Module Contents
---------------

.. py:exception:: NormliteError

   Bases: :py:obj:`Exception`


   Base exception class for all ``normlite`` exceptions.

   .. versionadded: 0.5.0


.. py:exception:: NoResultFound

   Bases: :py:obj:`NormliteError`


   Raised when exaclty one result row is expected, but none was found.

   .. versionadded: 0.5.0


.. py:exception:: MultipleResultsFound

   Bases: :py:obj:`NormliteError`


   Raised if multiple rows were found when exactly one was required.

   .. versionadded: 0.5.0


