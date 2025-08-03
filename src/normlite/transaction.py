# normlite/transaction.py
# Copyright (C) 2025 Gianmarco Antonini
#
# This module is part of normlite and is released under the GNU Affero General Public License.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Provide transaction management for Notion integrations.

This module adds transaction management to Notion integrations. It enables multiple clients to 
concurrently perform changes while maintaining atomicity, consistency, isolation and durability.
It achieves this by implementing shared and exclusive locking mechanisms and isolation levels. 

Warning:
    This module is at a very early stage of proof-of-concept. Do not use it yet!
"""

from __future__ import annotations
from collections import defaultdict
from enum import Enum, auto
import pdb
from typing import Dict, List, Literal, Protocol, Tuple
import uuid

LockMode = Literal["read", "write"]
"""Type for lock modes."""

class TransitionState(Enum):
    """Provide a transition state ``enum`` to describe the possible states."""
    
    ACTIVE = auto()
    """After being created with :meth:`TransactionManager.begi()`, the transaction goes to ``ACTIVE`` state."""

    COMMITTED = auto()
    """After successful :meth:`Transaction.commit()`, the transaction goes to ``COMMITED`` state."""
    
    ROLLED_BACK = auto()
    """After successful :meth:`Transaction.rollback()`, the transaction goes to ``ROLLED_BACK`` state."""

class Operation(Protocol):
    """Interface for change requests that process data in the context of a transaction.
    
    Change requests follow a well-defined protocol to accomplish their task of modifying data in a consistent way.
    Each operation must define:

        * :meth:`stage()`: These are the pre-commit activities to validate data prior to committing them. 
        
        * :meth:`do_commit()`: All activities to commit data to the database.

        * :meth:`do_rollback()`: All activities to revert changes committed prior to this failed change.
    """
    
    def __init__(self):
        self.transaction: Transaction = None
    
    def add_to(self, txn: Transaction) -> None:
        """Add this operation to a trasaction context."""
        self.transaction = txn
    
    def stage(self) -> None:
        """Stage and validate the data to be committed."""
        
    def do_commit(self) -> None:
        """Perform the commit activities associated with this operation."""
        
    def do_rollback(self) -> None:
        """Perform the rollback activities associated to this operation."""

class AcquireLockFailed(Exception):
    """Raised when an attempt to acquire a lock for a given resource fails."""
    pass

class LockManager:
    """Track which transactions hold locks on which resources and enforce access rules.
    """
    def __init__(self):
        # oid -> list of (tid, mode)
        self.locks: Dict[str, List[Tuple[str, LockMode]]] = defaultdict(list)

    def acquire(self, oid: str, tid: str, mode: LockMode) -> None:
        """Try to acquire a lock for the requested resource and for the requesting transaction.

        Args:
            oid (str): The object id to be locked.
            tid (str): The id of the transaction requisting the lock.
            mode (LockMode): The lock mode.

        Raises:
            AcquireLockFailed: If the lock request cannot be served.
        """
        current = self.locks[oid]

        if not current:
            # no locks held on the requested resource, lock it
            self.locks[oid].append((tid, mode))
            return
        
        if mode == 'read':
            # tid is attempting to acquire a read lock
            if all(m == 'read' for _, m in current):
                # all locks being held for the requested oid are read locks, 
                # add the new txn to the list
                self.locks[oid].append((tid, mode))
                return
            
            # there is one txn holding a write lock, attempt to acquire lock failed
            raise AcquireLockFailed(
                f'Attempt to acquire read lock on locked resource: {oid}, current loc: {current[0][1]} '
                f'(transaction: {current[0][0]}).'
            )
        
        if mode == 'write':
            # tid is attempting to acquire a write lock
            if len(current) == 1 and current[0][0] == tid:
                # IMPORTANT: 
                # Only the txn holding the read-lock on the requested resource is allowed to
                # upgrade it to an exclusive write-lock.
                self.locks[oid] = [(tid, 'write')]
                return
            
            # another txn is holding the exclusive write lock, attempt faild
            raise AcquireLockFailed(
                f'Attempt to acquire write lock on locked resource: {oid}, current loc: {current[0][1]} '
                f'(transaction: {current[0][0]}).'
            )

    def release(self, tx_id: str) -> None:
        """Release all locks held by the requesting transaction.

        Args:
            tid (str): The id of the requesting transaction.
        """
        for resource_id in list(self.locks):
            # reconstruct the list of locks by removing the requesting transaction from all resources
            self.locks[resource_id] = [
                (tid, mode) for tid, mode in self.locks[resource_id] if tid != tx_id
            ]
            if not self.locks[resource_id]:
                # no transaction holding locks for resourc_id, delete the correponding element
                del self.locks[resource_id]

class Transaction:
    """Provide a context to holds a list of change operations, their lock requirements, 
    and rollback logic.

    Warning:
        Initial implementation. UNSTABLE.
    """

    def __init__(self, tid: str, lock_manager: LockManager):
        self.tid = tid
        """The unique transaction identifier (UUIDv4)."""
        
        self.lock_manager = lock_manager
        """The lock manager to ensure locking mechanisms."""

        # Each operation: (resource_id, mode, operation, rollback_op, commit_op)
        self.operations: List[Tuple[str, LockMode, Operation]] = []
        """The list of operations to be committed/rollbacked in the context of this transaction."""

        self.state = TransitionState.ACTIVE
        """The transaction state."""

    def add_change(
        self,
        resource_id: str,
        mode: LockMode,
        operation: Operation,
    ) -> None:
        """Add a change request in the context of this transactions.

        Transactions track the list of changes they have to perform.   

        Args:
            resource_id (str): The resource id subject of the change.
            mode (LockMode): The requested locking mode.
            operation (Operation): The operation to carry out the requested change.
        """

        # acquire the requeste lock on the resource to be processed
        self.lock_manager.acquire(self.tid, resource_id, mode)

        # lock acquired as requested (no AcquireLockFailed execption raised)
        operation.add_to(self)
        self.operations.append((resource_id, mode, operation))    
    
    def commit(self) -> None:
        raise NotImplementedError

    def rollback(self) -> None:
        raise NotImplementedError

class TransactionManager:
    """Procure and coordinate transactions, lock acquisition, and lifecycle (start/commit/rollback).
    
    Warning:
        Initial implementation, UNSTABLE.
    """

    def __init__(self):
        self.lock_manager = LockManager()
        """The lock manager."""

        self.active_txs: Dict[str, Transaction] = {}
        """The active transactions the manager orchestrates."""

    def begin(self) -> Transaction:
        """Procure a new transaction.

        Note:
            The transaction manager is repsonsible to assign to each created transaction a unique id.

        Warning:
            Not implemented yet.

        Returns:
            Transaction: A new transaction.
        """
        tid = str(uuid.uuid4())
        tx = Transaction(tid, self.lock_manager)
        self.active_txs[tid] = tx
        return tx
        

    