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
import pytest
from normlite.proxy.server import create_app
from normlite.proxy.state import transactions
from normlite.notion_sdk.client import InMemoryNotionClient

@pytest.fixture
def test_client(monkeypatch):
    """Create Flask test client with an in-memory Notion client."""
    app = create_app()
    app.config["TESTING"] = True

    # Monkeypatch the proxy's Notion client for the test
    from normlite.proxy import state
    state.notion = InMemoryNotionClient()  # inject test-safe mock client

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

    # 3. Commit the transaction
    response = test_client.post(f"/transactions/{tx_id}/commit")
    assert response.status_code == 200
    assert response.get_json()["status"] == "committed"

    # 4. Optional: Validate that the resource was inserted (via mock state)
    notion_client = transactions[tx_id].staged_ops[0].notion
    inserted = [p for p in notion_client.pages if p["properties"]["Name"]["title"][0]["text"]["content"] == "Test Insert"]
    assert len(inserted) == 1

def test_insert_fails_and_rolls_back(test_client):
    # 1. Start a new transaction
    response = test_client.post("/transactions")
    tx_id = response.get_json()["transaction_id"]

    # 2. Try to insert an invalid page (missing required fields)
    bad_payload = {
        "parent": { "database_id": "db-123" },
        "properties": {
            # Intentionally missing Name
            "Status": { "select": { "name": "Done" } }
        }
    }
    response = test_client.post(f"/transactions/{tx_id}/insert", json=bad_payload)
    assert response.status_code == 400
    assert "error" in response.get_json()

    # 3. Rollback
    response = test_client.post(f"/transactions/{tx_id}/rollback")
    assert response.status_code == 200
    assert response.get_json()["status"] == "rolled_back"
