
from __future__ import annotations
import pdb
from typing import Any, Generator
from flask.testing import FlaskClient
import pytest

from normlite.proxy.server import create_app
from normlite.proxy.state import transaction_manager, notion
from normlite.notiondbapi.dbapi2 import Connection

@pytest.fixture
def proxy_client() -> Generator[FlaskClient, Any, None]:
    """Create Flask test client with an in-memory Notion client."""
    app = create_app()
    app.config["TESTING"] = True

    with app.test_client() as client:
        yield client

@pytest.fixture
def insert_op() -> dict:
    return {'endpoint': 'pages', 'request': 'create'}

@pytest.fixture
def insert_params() -> dict:
    parent = {'type': 'database_id', 'database_id': 'd9824bdc-8445-4327-be8b-5b47500af6ce'}
    return {
        'payload': {
        'properties': {
            'id': {'number': ':id'},       
            'name': {'title': [{'text': {'content': ':name'}}]},
            'grade': {'rich_text': [{'text': {'content': ':grade'}}]}
            },
            'parent': parent
        },
        'params': {                           # params contains the bindings
            'id': 1,
            'name': 'Galileo Galilei',
            'grade': 'A'
        }
    }

def test_cursor_exec_can_begin_new_implicit_txn(
        proxy_client: FlaskClient,
        insert_op: dict,
        insert_params: dict
    ):

    # 1. create new connection with the proxy.state.notion client 
    # this is important because you can inspect the same Notion in-memory store, which 
    # the proxy server is using
    conn = Connection(proxy_client, notion)
    cur = conn.cursor()

    # no transaction pending at this stage
    assert not conn._in_transaction()

    # 2. execute an op on the cursor: This is the **first** call
    cur.execute(insert_op, insert_params)

    # now the connection shall be in transaction state
    assert conn._in_transaction()    

@pytest.mark.skip('Transaction management in Connectio not ready yet.')
def test_connection_create_implicit_txn_on_exec(
        proxy_client: FlaskClient, 
        insert_op: dict,
        insert_params: dict
    ):
    dbapi_conn = Connection(proxy_client)

    # 1. execute the insert operation on the DBAPI cursor
    # Prior to execution no transactions are active
    cur = dbapi_conn.cursor()
    assert len(transaction_manager.active_txs.keys()) == 1
    
    # 2. execute the operation
    # the execute() opens an implicit transaction
    cur.execute(insert_op, insert_params)
    assert len(transaction_manager.active_txs.keys()) == 1

    # 3. fetch all rows
    # prior to commit, the cursor result set is empty.
    raw_rows = cur.fetchall()



    