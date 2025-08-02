normlite.transaction
====================

.. py:module:: normlite.transaction

.. autoapi-nested-parse::

   Provide transaction management for Notion integrations.

   This module adds transaction management to Notion integrations. It enables multiple clients to
   concurrently perform changes while maintaining atomicity, consistency, isolation and durability.
   It achieves this by implementing shared and exclusive locking mechanisms and isolation levels.

   .. warning:: This module is at a very early stage of proof-of-concept. Do not use it yet!









Module Contents
---------------

.. py:data:: LockMode

.. py:exception:: AcquireLockFailed

   Bases: :py:obj:`Exception`


   Raised when an attempt to acquire a lock for a given resource fails


.. py:class:: LockManager

   Track which transactions hold locks on which resources and enforce access rules.



   .. py:attribute:: locks
      :type:  Dict[str, List[Tuple[str, LockMode]]]


   .. py:method:: acquire(oid: str, tid: str, mode: LockMode) -> None


.. py:class:: Transaction

   Provide a context to holds a list of change operations, their lock requirements,
   and rollback logic.

   .. warning:: Not yet implemented.


   .. py:method:: add_change(resource_id: str, mode: LockMode, operation: Callable[[], None], rollback_op: Callable[[], None], commit_op: Callable[[], None]) -> bool
      :abstractmethod:



   .. py:method:: commit() -> None
      :abstractmethod:



   .. py:method:: rollback() -> None
      :abstractmethod:



.. py:class:: TransactionManager

   Procure and coordinate transactions, lock acquisition, and lifecycle (start/commit/rollback).

   .. warning:: Not yet implemented.


   .. py:attribute:: lock_manager

      The lock manager.


   .. py:attribute:: active_txs
      :type:  Dict[str, Transaction]

      The active transactions the manager orchestrates.


   .. py:method:: begin() -> Transaction
      :abstractmethod:


      Procure a new transaction.

      .. note:: The transaction manager is repsonsible to assign to each created transaction a unique id.

      .. warning:: Not implemented yet.

      :returns: A new transaction.
      :rtype: Transaction



