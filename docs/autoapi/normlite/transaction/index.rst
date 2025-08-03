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

   Type for lock modes.

.. py:class:: TransitionState(*args, **kwds)

   Bases: :py:obj:`enum.Enum`


   Provide a transition state ``enum`` to describe the possible states.


   .. py:attribute:: ACTIVE

      After being created with :meth:`TransactionManager.begi()`, the transaction goes to ``ACTIVE`` state.


   .. py:attribute:: COMMITTED

      After successful :meth:`Transaction.commit()`, the transaction goes to ``COMMITED`` state.


   .. py:attribute:: ROLLED_BACK

      After successful :meth:`Transaction.rollback()`, the transaction goes to ``ROLLED_BACK`` state.


.. py:class:: Operation

   Bases: :py:obj:`Protocol`


   Interface for change requests that process data in the context of a transaction.

   Change requests follow a well-defined protocol to accomplish their task of modifying data in a consistent way.
   Each operation must define:

       * :meth:`stage()`: These are the pre-commit activities to validate data prior to committing them.

       * :meth:`do_commit()`: All activities to commit data to the database.

       * :meth:`do_rollback()`: All activities to revert changes committed prior to this failed change.


   .. py:attribute:: transaction
      :type:  Transaction
      :value: None



   .. py:method:: add_to(txn: Transaction) -> None

      Add this operation to a trasaction context.



   .. py:method:: stage() -> None

      Stage and validate the data to be committed.



   .. py:method:: do_commit() -> None

      Perform the commit activities associated with this operation.



   .. py:method:: do_rollback() -> None

      Perform the rollback activities associated to this operation.



.. py:exception:: AcquireLockFailed

   Bases: :py:obj:`Exception`


   Raised when an attempt to acquire a lock for a given resource fails.


.. py:class:: LockManager

   Track which transactions hold locks on which resources and enforce access rules.



   .. py:attribute:: locks
      :type:  Dict[str, List[Tuple[str, LockMode]]]


   .. py:method:: acquire(oid: str, tid: str, mode: LockMode) -> None

      Try to acquire a lock for the requested resource and for the requesting transaction.

      :param oid: The object id to be locked.
      :type oid: str
      :param tid: The id of the transaction requisting the lock.
      :type tid: str
      :param mode: The lock mode.
      :type mode: LockMode

      :raises AcquireLockFailed: If the lock request cannot be served.



   .. py:method:: release(tx_id: str) -> None

      Release all locks held by the requesting transaction.

      :param tid: The id of the requesting transaction.
      :type tid: str



.. py:class:: Transaction(tid: str, lock_manager: LockManager)

   Provide a context to holds a list of change operations, their lock requirements,
   and rollback logic.

   .. warning:: Initial implementation. UNSTABLE.


   .. py:attribute:: tid

      The unique transaction identifier (UUIDv4).


   .. py:attribute:: lock_manager

      The lock manager to ensure locking mechanisms.


   .. py:attribute:: operations
      :type:  List[Tuple[str, LockMode, Operation]]
      :value: []


      The list of operations to be committed/rollbacked in the context of this transaction.


   .. py:attribute:: state

      The transaction state.


   .. py:method:: add_change(resource_id: str, mode: LockMode, operation: Operation) -> None

      Add a change request in the context of this transactions.

      Transactions track the list of changes they have to perform.

      :param resource_id: The resource id subject of the change.
      :type resource_id: str
      :param mode: The requested locking mode.
      :type mode: LockMode
      :param operation: The operation to carry out the requested change.
      :type operation: Operation



   .. py:method:: commit() -> None
      :abstractmethod:



   .. py:method:: rollback() -> None
      :abstractmethod:



.. py:class:: TransactionManager

   Procure and coordinate transactions, lock acquisition, and lifecycle (start/commit/rollback).

   .. warning:: Initial implementation, UNSTABLE.


   .. py:attribute:: lock_manager

      The lock manager.


   .. py:attribute:: active_txs
      :type:  Dict[str, Transaction]

      The active transactions the manager orchestrates.


   .. py:method:: begin() -> Transaction

      Procure a new transaction.

      .. note:: The transaction manager is repsonsible to assign to each created transaction a unique id.

      .. warning:: Not implemented yet.

      :returns: A new transaction.
      :rtype: Transaction



