# tests/test_proxy.py
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
import pdb
import pytest
from normlite.proxy.client import create_proxy_client
from normlite.proxy.server import create_app
from normlite.proxy.state import notion, transaction_manager
from normlite.txmodel.operations import Operation, StagedInsert
from normlite.txmodel.transaction import TransactionState

@pytest.fixture
def test_client():
    """Create Flask test client with an in-memory Notion client."""
    app = create_app()
    app.config["TESTING"] = True

    with app.test_client() as client:
        yield create_proxy_client(flask_client=client)

@pytest.fixture(scope='function')
def store():
    yield notion._store
    notion._store['store'].clear()      # always clear the store after usage in a test

def test_begin_txn(test_client):
    response = test_client.begin()
    assert response.status_code == 200
    tx_id = response.json()["transaction_id"]
    state = response.json()['state']
    assert tx_id in transaction_manager.active_txs
    assert state == TransactionState.ACTIVE.name

def test_insert_to_txn(test_client):
    # 1. Start a new transaction
    response = test_client.begin()
    assert response.status_code == 200
    tx_id = response.json()["transaction_id"]

    # 2. Insert a page using the Notion-compatible payload
    payload = {
        "parent": { "database_id": "db-123" },
        "properties": {
            "Name": {
                "title": [{"text": {"content": "Test Insert"}}]
            }
        }
    }
    response = test_client.insert(tx_id, payload)
    assert response.status_code == 202
    assert response.json()['state'] == TransactionState.ACTIVE.name

    # 3. check that a staged insert op has been added to the active txn
    tx = transaction_manager.active_txs[tx_id]
    assert len(tx.operations) == 1
    assert isinstance(tx.operations[0][2], StagedInsert)

    # IMPORTANT: rollback the transaction to release the lock
    tx.rollback()
    assert len(tx.results) == 0


def test_insert_commit_flow(test_client, store):
    # 1. Start a new transaction
    response = test_client.begin()
    assert response.status_code == 200
    tx_id = response.json()["transaction_id"]

    # 2. Insert a page using the Notion-compatible payload
    payload = {
        "parent": { "database_id": "db-123" },
        "properties": {
            "Name": {
                "title": [{"text": {"content": "Test Insert"}}]
            }
        }
    }
    response = test_client.insert(tx_id, payload)
    assert response.status_code == 202
    assert response.json()['state'] == TransactionState.ACTIVE.name

    # 3. Commit the transaction
    response = test_client.commit(tx_id)
    assert response.status_code == 200
    assert response.json()["state"] == TransactionState.COMMITTED.name
    assert len(response.json()["data"]) == 1

    # 4. Validate that the resource was inserted via mock object notion
    inserted = [p for p in store['store'] if p["properties"]["Name"]["title"][0]["text"]["content"] == "Test Insert"]
    assert len(inserted) == 1

def test_insert_stage_fails(test_client, store):
    # 1. Start a new transaction
    response = test_client.begin()
    assert response.status_code == 200
    tx_id = response.json()["transaction_id"]

    # 3. Insert an invalid page (missing required fields)
    bad_payload = {
        "parent": { "database_id": "db-123" },
        # Intentionally missing "properties" object
    }
    response = test_client.insert(tx_id, bad_payload)
    assert response.status_code == 202      # code = 202 means: accepted but operation not started yet or not completed
    assert response.json()['state'] == TransactionState.ACTIVE.name

    # 3. Commit the transaction: it is expected to fail
    response = test_client.commit(tx_id)
    assert response.status_code == 500
    assert response.json()['state'] == TransactionState.FAILED.name
    assert "error" in response.json()
    assert response.json()['error']['code'] == 'commit_failed'
    assert 'Missing "properties"' in response.json()['error']['message']

    # 4. Expect the store to be empty as previous commit failed
    assert len(store['store']) == 0

def test_insert_commit_fails(test_client, store):
    # 1. Fake insert operation with failing do_commit()
    class CommitFailingInsert(Operation):
        def stage(self):
            pass

        def do_commit(self):
            raise Exception('Mocked: Connection error.')
        
        def do_rollback(self):
            pass

        def get_result(self):
            return {}               # nothing to return 

    # 2. Start a new transaction
    response = test_client.begin()
    assert response.status_code == 200
    tx_id = response.json()["transaction_id"]

    # 3. Insert a page using the Notion-compatible payload
    payload = {
        "parent": { "database_id": "db-123" },
        "properties": {
            "Name": {
                "title": [{"text": {"content": "Test Insert"}}]
            }
        }
    }
    response = test_client.insert(tx_id, payload)
    assert response.status_code == 202
    assert response.json()['state'] == TransactionState.ACTIVE.name

    # 4. Monkey-patch the transaction and add the commit failing insert
    # This is executed after the first insert, so expect to have one remaining Notion object with archived == True
    transaction_manager.active_txs.get(tx_id).add_change(
        'bc1211ca-e3f1-4939-ae34-5260b16f627c',
        'write',
        CommitFailingInsert()
    )

    # 5. Commit the transaction: it is expected to fail, but the automatic rollback works
    # status_code == 500
    # state == TransactionState.ABORTED
    response = test_client.commit(tx_id)
    assert response.status_code == 500
    assert response.json()['state'] == TransactionState.ABORTED.name
    assert "error" in response.json()
    assert response.json()['error']['code'] == 'commit_failed'
    assert 'Mocked: Connection error' in response.json()['error']['message']

    # 6. Expect exactly 1 element with archived == True
    #pdb.set_trace()
    tx_data = response.json()['data']
    assert len(tx_data) == 2                        # 1 for the successfull insert (archived = False) and 1 for the rollbacked insert (archived = True) 
    object_id1 = tx_data[0]['id']
    object_id2 = tx_data[1]['id']
    assert object_id1 == object_id2                 # it's the same object but with different attribute archived
    archived1 = tx_data[0]['archived']
    archived2 = not tx_data[1]['archived']
    #pdb.set_trace()
    assert archived1 == archived2
    assert len(store['store']) == 1
    assert store['store'][0]['id'] == object_id1
    assert store['store'][0]['archived'] == (not archived2)

