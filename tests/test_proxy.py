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
from normlite.proxy.server import create_app
from normlite.proxy.state import notion, transaction_manager
from normlite.txmodel.operations import Operation
from normlite.txmodel.transaction import TransactionState

@pytest.fixture
def test_client():
    """Create Flask test client with an in-memory Notion client."""
    app = create_app()
    app.config["TESTING"] = True

    with app.test_client() as client:
        yield client

def test_insert_commit_flow(test_client):
    # 1. Start a new transaction
    response = test_client.post("/transactions")
    assert response.status_code == 200
    tx_id = response.get_json()["transaction_id"]

    # 2. Insert a page using the Notion-compatible payload
    payload = {
        "parent": { "database_id": "db-123" },
        "properties": {
            "Name": {
                "title": [{"text": {"content": "Test Insert"}}]
            }
        }
    }
    response = test_client.post(f"/transactions/{tx_id}/insert", json=payload)
    assert response.status_code == 202
    assert response.get_json()['state'] == TransactionState.ACTIVE.name

    # 3. Commit the transaction
    response = test_client.post(f"/transactions/{tx_id}/commit")
    assert response.status_code == 200
    assert response.get_json()["state"] == TransactionState.COMMITTED.name
    assert len(response.get_json()["data"]) == 1

    # 4. Optional: Validate that the resource was inserted (via mock state)
    inserted = [p for p in notion.__class__._store['store'] if p["properties"]["Name"]["title"][0]["text"]["content"] == "Test Insert"]
    assert len(inserted) == 1

def test_insert_stage_fails(test_client):
    # 1. Start a new transaction
    response = test_client.post("/transactions")
    tx_id = response.get_json()["transaction_id"]

    # 2. Insert an invalid page (missing required fields)
    bad_payload = {
        "parent": { "database_id": "db-123" },
        # Intentionally missing "properties" object
    }
    response = test_client.post(f"/transactions/{tx_id}/insert", json=bad_payload)
    assert response.status_code == 202      # code = 202 means: accepted but operation not started yet or not completed
    assert response.get_json()['state'] == TransactionState.ACTIVE.name

    # 3. Commit the transaction: it is expected to fail
    response = test_client.post(f"/transactions/{tx_id}/commit")
    assert response.status_code == 500
    assert response.get_json()['state'] == TransactionState.FAILED.name
    assert "error" in response.get_json()

def test_insert_commit_fails(test_client):
    # fake insert operation with failing do_commit()
    class CommitFailingInsert(Operation):
        def stage(self):
            pass

        def do_commit(self):
            raise Exception('Unsaved data.')
        
        def do_rollback(self):
            pass

    # 1. Start a new transaction
    response = test_client.post("/transactions")
    tx_id = response.get_json()["transaction_id"]

    # 2. Monkey-patch the transaction and add the commit failing insert
    transaction_manager.active_txs.get(tx_id).add_change(
        'bc1211ca-e3f1-4939-ae34-5260b16f627c',
        'write',
        CommitFailingInsert()
    )

    # 3. Commit the transaction: it is expected to fail
    response = test_client.post(f"/transactions/{tx_id}/commit")
    assert response.status_code == 500
    assert response.get_json()['state'] == TransactionState.FAILED.name
    assert "error" in response.get_json()

    # 4. rollback the changes
    response = test_client.post(f"/transactions/{tx_id}/rollback")
    assert response.status_code == 200
    assert response.get_json()["state"] == TransactionState.ABORTED.name

def test_insert_commit_and_rollback_flow(test_client):
    # 1. Start a new transaction
    response = test_client.post("/transactions")
    assert response.status_code == 200
    tx_id = response.get_json()["transaction_id"]

    # 2. Insert a page using the Notion-compatible payload
    payload = {
        "parent": { "database_id": "db-123" },
        "properties": {
            "Name": {
                "title": [{"text": {"content": "Test Insert"}}]
            }
        }
    }
    response = test_client.post(f"/transactions/{tx_id}/insert", json=payload)
    assert response.status_code == 202
    assert response.get_json()['state'] == TransactionState.ACTIVE.name

    # 3. Commit the transaction
    response = test_client.post(f"/transactions/{tx_id}/commit")
    assert response.status_code == 200
    data = response.get_json()["data"]
    assert response.get_json()["state"] == TransactionState.COMMITTED.name
    assert len(data) == 1
    assert notion._get(data[0]['id']) == data[0]

    # 4. Rollback the committed transaction
    response = test_client.post(f"/transactions/{tx_id}/rollback")
    assert response.status_code == 200
    data = response.get_json()["data"]
    assert response.get_json()["state"] == TransactionState.ABORTED.name
    assert len(data) == 1
    assert notion._get(data[0]['id'])['archived'] == data[0]['archived'] 
