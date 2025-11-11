import json
from pathlib import Path
import pdb
from typing import Any, Generator
import uuid
from flask.testing import FlaskClient
import pytest
from normlite.notion_sdk.client import AbstractNotionClient, InMemoryNotionClient
from normlite.notiondbapi.dbapi2 import Connection, Cursor
from normlite.proxy.server import create_app
from normlite.proxy.state import transaction_manager, notion

@pytest.fixture(scope="session")
def api_key() -> str:
    # This is a fake key, read the real one from env variable
    return 'ntn_abc123def456ghi789jkl012mno345pqr'

@pytest.fixture(scope="session")
def ischema_page_id() -> str:
    # This is a fake page id for the info schema page, read the real one from an env variable.
    # Remember that as of today, the Notion API only supports creating pages into 
    # **existing** pages.
    return '680dee41-b447-451d-9d36-c6eaff13fb46'

@pytest.fixture(scope="session")
def client() -> AbstractNotionClient:
    return InMemoryNotionClient()

# Load the fixture file once
@pytest.fixture(scope="module")
def json_fixtures():
    fixture_path = Path(__file__).parent / "fixtures.json"
    with fixture_path.open() as f:
        return json.load(f)
    
@pytest.fixture(scope='function')
def proxy_client() -> Generator[FlaskClient, Any, None]:
    """Create Flask test client with an in-memory Notion client."""
    app = create_app()
    app.config["TESTING"] = True

    with app.test_client() as client:
        yield client

        # tear down: empties the active txns and Notion store
        transaction_manager.active_txs = {}
        notion._create_store()


@pytest.fixture(scope="function")
def dbapi_connection(proxy_client: FlaskClient, client: AbstractNotionClient) -> Connection:
    return Connection(proxy_client, client)


# The following fixture must be scope=function otherwise the attribute
# _result_set gets overwritten when executing all tests together
@pytest.fixture(scope="function")
def dbapi_cursor(client: AbstractNotionClient) -> Cursor:
    return Cursor(client)
