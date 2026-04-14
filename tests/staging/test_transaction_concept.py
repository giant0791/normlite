# tests/test_transaction_concept.py
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

"""
# Quick proof-of-concept for a transaction manager.

## Why we need a transaction manager?
Notion does not support ACID transactional operations to its objects.
It just ensures atomicity.
A full SQL-like front-end to Notion needs a transactional model to ensure ACID.

## What are the key abstractions and interactions?

### Initialize a connection to a Notion integration
normlite clients do not interact directly with Notion integrations.
They send their requests to a proxy: TransactionManagerServer.

A TransactionManagerServer is a Flask REST API application that executes Notion API calls 
*on behalf* of normlite clients.

normlite clients interact with the TransactionManagerClient. This acts as proxy to the 
TransactionManagerServer behind the scenes (see test_engine_with_txn_client() for a detailed example
of seamless integration of the txn client into the engine connection model).

### Commit as you go
Updates to databases are made persistent by commiting a transaction.
"commit as you go" is a commit-style to commit data changes using the Connection.commit() method 
*inside* the block where we have the connection object:

>>> with engine.connect() as conn:
...    conn.execute(text("CREATE TABLE some_table (x int, y int)"))
...    conn.execute(
...        text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
...        [{"x": 1, "y": 1}, {"x": 2, "y": 4}],
...    )
...    conn.commit()

"commit as you go" requires the `Connection` class to access the transaction, so that it can implement
the `commit()` method (see test_connection_provide_commit() test for how the `Connection` class 
implements its `commit()` API method by delegating it to the transaction manager previously 
acquired by the engine).

### Commit changes to the database
At a very high level view, each `Connection.execute()` call adds *change requests* to the txn.
When the `Connection.commit()` method is called, the following interactions take place:

1. `TransactionManagerClient.commit()` issues the REST API request:
    `PATCH transactions/{txn_id}/commit`.

2. `TransactionManagerServer.commit()` gets a call dispatched through Flask and starts executing 
each change request contained in its list of change requests.

3. `ChangeRequest.execute()` is called by the txn manager. This method calls the Notion API client 
to perform the REST API call to effect the change in Notion

4. `InternalIntegrationClient.__call__(endpoint, request, payload)` is called by 
the `ChangeRequest.execute()` method. This method interacts via the Notion API with the integration.
It takes care that the requested payload is processed accordingly.

### Notes on rolling back a transaction in case of errors.
First, not all change requests are equal:
* CREATE, INSERT: create new Notion entities (a database or a page respectively)
* SELECT: no changes performed, it's only a query
* ALTER, UPDATE: modify an existing entities (page a database or a page respectively)
* DROP, DELETE: destroy existing entities permanently

The most critical change request is the ALTER/UPDATE, because it requires a write lock on the
table/row to prevent others to make their changes.

**IMPORTANT**: When the txn manager executes a change request like ALTER/UPDATE, it must first ensure
that the entity obiect id (oid) is not locked. 
If it is not locked, then it set a write lock, which means no one else can either read or write the locked entity.
If it is locked, then is must abort the transaction.

### Begin a transaction block
The Engine class can create a 

"""
from __future__ import annotations
from collections import defaultdict
import pdb
from typing import Dict, List, Literal, Tuple
from uuid import UUID
import pytest
from normlite.engine import create_engine
from normlite.txmodel.transaction import AcquireLockFailed, Operation, Transaction, TransactionManager, TransitionState


NOTION_TOKEN = 'secret_token'
NOTION_VERSION = '2022-06-28'

class NoOp(Operation):

    def stage(self) -> None:
        pass

    def do_commit(self):
        pass

    def do_rollback(self):
        pass

def create_txn_manager(uri: str):
    """Provide a transaction manager client factory."""
    pass

@pytest.fixture
def txn_manager() -> TransactionManager:
    return TransactionManager()

@pytest.fixture
def payload() -> dict:
    return {
        'object': 'page',
        'id': '00000000-0000-0000-0000-000000000000'
    }    

@pytest.mark.skip()
def test_engine_with_txn_client():
    txn_manager = create_txn_manager('normlite+txnmgr://127.0.0.1:8000')
    engine = create_engine(
        f'normlite+auth://internal?token={NOTION_TOKEN}&version={NOTION_VERSION}',
        txn_manager=txn_manager         # create_engine() provides dialect specific keyword args
    )

    assert engine.txn_manager == txn_manager    # the Engine object stores the txn manager as its member

@pytest.mark.skip()
def test_connection_provide_commit():
    # We are inside the Connection.commit() method
    # Connection.__init__() needs a txn manager
    txn_manager = create_txn_manager('normlite+txnmgr://127.0.0.1:8000')
    txn_manager.commit()

@pytest.mark.skip()
def test_txn_execute_change_req():
    # t1 is the transaction expected to successfully acquire the write lock.
    # changes is a list of change requests where the txn holds the change requests from the client
    # for now a simple mapping: key = f'{endpoint}.{request}' value = payload
    t1 = Transaction()
    t1.changes['pages.update'] = payload

    # t2 is the transaction expected to fail to acquire the write lock.
    t2 = Transaction()
    t2.changes['pages.update'] = payload
    
    # when a new transaction is added to the txn manager, it gets a unique id
    tid1 = '11111111-1111-1111-1111-111111111111'
    tid2 = '22222222-2222-2222-2222-222222222222'

    # transactions is a list of transactions where the txn manager holds the current open transactions
    # for now it is a simple mapping: key = tid, value = Transaction object
    txn_manager = TransactionManager()
    txn_manager.transactions[tid1] = t1
    txn_manager.transactions[tid2] = t2


def test_txn_manager_lock_entity(txn_manager: TransactionManager, payload: dict):
    # this payload is refers to the same object contended by two transactions
    # Note: here the details of the update is irrelevant. For this test, it is
    # important the two contending transactions want to change the same object (same oid)
    lock_manager = txn_manager.lock_manager

    # when a new transaction is added to the txn manager, it gets a unique id
    tid1 = '11111111-1111-1111-1111-111111111111'

    # On starting execution of a change request, the txn tries to acquire the lock
    lock_manager.acquire(payload['id'], tid1, 'write')
    assert lock_manager.locks['00000000-0000-0000-0000-000000000000'] == [
        ('11111111-1111-1111-1111-111111111111', 'write',)
    ]

def test_multiple_txns_share_lock(txn_manager: TransactionManager, payload: dict):
    lock_manager = txn_manager.lock_manager
    tids = [
        UUID("11111111-1111-1111-1111-111111111111"),  
        UUID("22222222-2222-2222-2222-222222222222"),  
        UUID("33333333-3333-3333-3333-333333333333"),  
        UUID("44444444-4444-4444-4444-444444444444"),  
    ]    
    
    for tid in tids:
        lock_manager.acquire(payload['id'], tid, 'read')
    
    assert len(lock_manager.locks[payload['id']]) == 4

def test_only_one_txn_holds_exclusive_lock(txn_manager: TransactionManager, payload: dict):
    lock_manager = txn_manager.lock_manager
    roid = payload['id']
    tid1 = UUID("11111111-1111-1111-1111-111111111111")
    lock_manager.acquire(roid, tid1, 'read')
    lock_manager.acquire(roid, tid1, 'write')
    
    assert lock_manager.locks[roid] == [(tid1, 'write')]

def test_attempt_to_acquire_write_lock_on_read_locked_resource(txn_manager: TransactionManager, payload: dict):
    lock_manager = txn_manager.lock_manager
    roid = payload['id']
    tid1 = UUID("11111111-1111-1111-1111-111111111111")
    lock_manager.acquire(roid, tid1, 'read')
    tid2 = UUID("11111111-2222-1111-1133-111111111111")

    with pytest.raises(
        AcquireLockFailed,
        match=f'Attempt to acquire write lock on locked resource: {roid}, current loc: read'
    ):
        lock_manager.acquire(roid, tid2, 'write')

def test_attempt_to_acquire_write_lock_on_write_locked_resource(txn_manager: TransactionManager, payload: dict):
    lock_manager = txn_manager.lock_manager
    roid = payload['id']
    tid1 = UUID("11111111-1111-1111-1111-111111111111")
    lock_manager.acquire(roid, tid1, 'write')
    tid2 = UUID("11111111-2222-1111-1133-111111111111")

    with pytest.raises(
        AcquireLockFailed,
        match=f'Attempt to acquire write lock on locked resource: {roid}, current loc: write'
    ):
        lock_manager.acquire(roid, tid2, 'write')

def test_attempt_to_acquire_read_lock_on_write_locked_resource(txn_manager: TransactionManager, payload: dict):
    lock_manager = txn_manager.lock_manager
    roid = payload['id']
    tid1 = UUID("11111111-1111-1111-1111-111111111111")
    lock_manager.acquire(roid, tid1, 'write')
    tid2 = UUID("11111111-2222-1111-1133-111111111111")

    with pytest.raises(
        AcquireLockFailed,
        match=f'Attempt to acquire read lock on locked resource: {roid}, current loc: write'
    ):
        lock_manager.acquire(roid, tid2, 'read')



def test_txn_is_active_after_begin(txn_manager: TransactionManager):
    t1: Transaction = txn_manager.begin()
    t2: Transaction = txn_manager.begin()

    assert t1.state == TransitionState.ACTIVE
    assert t2.state == TransitionState.ACTIVE


def test_txn_tid_is_unique(txn_manager: TransactionManager):
    t1: Transaction = txn_manager.begin()
    t2: Transaction = txn_manager.begin()

    assert t1.tid != t2.tid

def test_release_lock_for_txn(txn_manager: TransactionManager, payload: dict):
    lock_manager = txn_manager.lock_manager
    roid = payload['id']
    tids = [
        UUID("11111111-1111-1111-1111-111111111111"),  
        UUID("22222222-2222-2222-2222-222222222222"),  
        UUID("33333333-3333-3333-3333-333333333333"),  
        UUID("44444444-4444-4444-4444-444444444444"),  
    ]    
    
    for tid in tids:
        lock_manager.acquire(roid, tid, 'read')

    lock_manager.release(UUID("44444444-4444-4444-4444-444444444444"))

    assert UUID("44444444-4444-4444-4444-444444444444") not in lock_manager.locks[roid]

def test_no_locks_after_all_txn_releasetxn_manager(txn_manager: TransactionManager, payload: dict):
    lock_manager = txn_manager.lock_manager
    roid = payload['id']
    tids = [
        UUID("11111111-1111-1111-1111-111111111111"),  
        UUID("22222222-2222-2222-2222-222222222222"),  
        UUID("33333333-3333-3333-3333-333333333333"),  
        UUID("44444444-4444-4444-4444-444444444444"),  
    ]    
    
    for tid in tids:
        lock_manager.acquire(roid, tid, 'read')

    for tid in reversed(tids):
        lock_manager.release(tid)

    assert not lock_manager.locks[roid]
