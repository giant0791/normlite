
from __future__ import annotations
import pdb
import uuid
from flask.testing import FlaskClient
import pytest

from normlite.cursor import _NoCursorResultMetadata, CursorResult, ResourceClosedError
from normlite.proxy.state import transaction_manager, notion
from normlite.notiondbapi.dbapi2 import Connection, Cursor, DatabaseError, InterfaceError

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

@pytest.fixture
def insert_op2() -> dict:
    return {'endpoint': 'pages', 'request': 'create'}

@pytest.fixture
def insert_params2() -> dict:
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
            'id': 2,
            'name': 'Ada Lovelace',
            'grade': 'A'
        }
    }

@pytest.fixture
def select_op() -> dict:
    return {'endpoint': 'databases', 'request': 'query'}

@pytest.fixture
def select_params() -> dict:
    """Implements a SELECT * FROM students."""
    return {
        'payload': {
            'database_id': 'd9824bdc-8445-4327-be8b-5b47500af6ce',
            'filter': {}    
        },
        'params': {}
    }

def test_dbapi_cursor_fetchall(dbapi_cursor: Cursor):
    rows = dbapi_cursor.fetchall()
    expected_rows = [
        (
            '680dee41-b447-451d-9d36-c6eaff13fb45',
            False,
            False,
            12345,
            'B',
            'Isaac Newton'
            ),
        (
            '680dee41-b447-451d-9d36-c6eaff13fb46',
            True,
            True,
            67890,  
            'A', 
            'Galileo Galilei'
        ),
    ]

    assert expected_rows == rows

def test_dbapi_cursor_no_parent(
        dbapi_connection: Connection, 
        insert_op: dict,
        insert_params: dict        
    ):
    insert_params['payload'].pop('parent')

    # error occurs in the POST /transaction<id>/insert
    with pytest.raises(
        DatabaseError, 
        match='Missing "parent"'):
        dbapi_connection.cursor().execute(insert_op, insert_params)

def test_dbapi_cursor_no_properties(
        dbapi_connection: Connection, 
        insert_op: dict,
        insert_params: dict        
    ):
    insert_params['payload'].pop('properties')

    # error occurs in the POST /transaction<id>/insert
    with pytest.raises(
        InterfaceError, 
        match='Missing "properties"'):
        dbapi_connection.cursor().execute(insert_op, insert_params)

def test_dbapi_cursor_concat_calls(
        dbapi_connection: Connection, 
        insert_op: dict,
        insert_params: dict        
    ):
    with pytest.raises(InterfaceError, match='Cursor result set is empty.'):
        results = dbapi_connection.cursor().execute(
            insert_op, 
            insert_params
        ).fetchall()

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


def test_dbapi_cursor_resultset_avail_on_commit(
        dbapi_connection: Connection, 
        insert_op: dict,
        insert_params: dict
    ):
    # monkey-patch the notion client from proxy/state.py
    dbapi_connection._client = notion
    conn = dbapi_connection
    
    # 1. create new connection with the proxy.state.notion client 
    # this is important because you can inspect the same Notion in-memory store, which 
    # the proxy server is using

    cur = conn.cursor()

    # Prior to execution no transactions are active
    # no transaction pending at this stage
    assert not conn._in_transaction()

    # 2. execute an op on the cursor: This is the **first** call
    cur.execute(insert_op, insert_params)

    # the payload in the insert operation has been bound
    active_tx = transaction_manager.active_txs[conn._tx_id]
    op = active_tx.operations[-1][2]
    payload = op.page_payload
    properties = payload['properties']
    assert properties['id']['number'] == 1
    assert properties['name']['title'][0]['text']['content'] == 'Galileo Galilei'
    assert properties['grade']['rich_text'][0]['text']['content'] == 'A'
    
    # the execute() opens an implicit transaction
    assert len(transaction_manager.active_txs.keys()) == 1
    assert conn._in_transaction()
    assert cur.rowcount == -1    

    # 3. fetch all rows
    # prior and after the commit, the cursor result set is empty.
    # Data committed in the transaction are available in the composite cursor:
    # comp_cur = conn.cursor(composite=True)
    conn.commit()
    assert cur.rowcount == -1

def test_cursorresult_avail_on_commit(
        dbapi_connection: Connection, 
        insert_op: dict,
        insert_params: dict
    ):
    # monkey-patch the notion client from proxy/state.py
    dbapi_connection._client = notion
    conn = dbapi_connection
    
    # 1. create new connection with the proxy.state.notion client 
    # this is important because you can inspect the same Notion in-memory store, which 
    # the proxy server is using
    cur = conn.cursor()

    # Prior to execution no transactions are active
    # no transaction pending at this stage
    assert not conn._in_transaction()

    # 2. execute an op on the cursor: This is the **first** call
    cur.execute(insert_op, insert_params)

    # the payload in the insert operation has been bound
    active_tx = transaction_manager.active_txs[conn._tx_id]
    op = active_tx.operations[-1][2]
    payload = op.page_payload
    properties = payload['properties']
    assert properties['id']['number'] == 1
    assert properties['name']['title'][0]['text']['content'] == 'Galileo Galilei'
    assert properties['grade']['rich_text'][0]['text']['content'] == 'A'
    
    # the execute() opens an implicit transaction
    assert len(transaction_manager.active_txs.keys()) == 1
    assert conn._in_transaction()
    assert cur.rowcount == -1    

    # 3. construct the cursor result and expect exactly one row
    # Data committed in the transaction are available in the composite cursor:
    # comp_cur = conn.cursor(composite=True)
    conn.commit()
    result = CursorResult(cur)
    assert not result.returns_rows
    assert isinstance(result._metadata, _NoCursorResultMetadata)

    # 4. Any attempt to access the cursor result metadata raises
    # ResourceClosedError
    with pytest.raises(ResourceClosedError, match='This result object does not return rows.'):
        keys = result._metadata.keys

def test_connection_procure_composite_cursor(
        dbapi_connection: Connection, 
        insert_op: dict,
        insert_params: dict,
        insert_op2: dict,
        insert_params2: dict,        
):
    # monkey-patch the notion client from proxy/state.py
    dbapi_connection._client = notion
    conn = dbapi_connection

    # 1. create a new connection
    cur = conn.cursor() # composite=False as default

    # 2. execute two .execute()
    cur.execute(insert_op, insert_params)
    cur.execute(insert_op2, insert_params2)

    # 3. commit all changes
    conn.commit()

    # What is the best interface to access the results from 1st and 2nd .execute()?
    # now you have CompositeCursor
    # get the composite cursor
    comp_cur = conn.cursor(composite=True)

    assert comp_cur.rowcount == 1
    id = str(uuid.UUID(int=comp_cur.lastrowid))
    row = comp_cur.fetchone()
    assert row[0] == id

    # move on to the next result set
    assert comp_cur.nextset()

    # now fetchone() applies to the second result set
    assert comp_cur.rowcount == 1
    id = str(uuid.UUID(int=comp_cur.lastrowid))
    row = comp_cur.fetchone()
    assert row[0] == id

@pytest.mark.skip('Deferred: This requires StagedSelect and new route.')
def test_multi_cursor_results_in_tx(
        dbapi_connection: Connection, 
        insert_op: dict,
        insert_params: dict,
        insert_op2: dict,
        insert_params2: dict,
        select_op: dict,
        select_params: dict
):
    # monkey-patch the notion client from proxy/state.py
    dbapi_connection._client = notion
    conn = dbapi_connection
    
    # 1. create new connection with the proxy.state.notion client 
    # this is important because you can inspect the same Notion in-memory store, which 
    # the proxy server is using
    cur = conn.cursor()

    # 2. execute non mutanting statement
    # the store has no pages in the students database
    cur.execute(select_op, select_params)
    result = CursorResult(cur)
    assert not result.returns_rows

    # 3. executes all inserts
    # the store still have 0 pages, no changes committed so far
    cur.execute(insert_op, insert_params)
    cur.execute(insert_op2, insert_params2)
    cur.execute(select_op, select_params)
    result = CursorResult(cur)
    assert not result.returns_rows

    # 4. commit changes
    # the stores now has 2 new pages after the inserts
    conn.commit()
    result = CursorResult(cur)
    assert result.returns_rows
    rows = result.all()
    assert rows[0]['name'] == 'Galileo Galilei'
    assert rows[1]['name'] == 'Ada Lovelace'


