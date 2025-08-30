from collections import namedtuple
import pytest

from normlite.engine import Engine, NotionAuthURI, NotionURI, _parse_uri, create_engine, NotionSimulatedURI
from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.sql.schema import Table

"""
Quick mapping between Notion and the database world.

Database store: This a dedicated Notion workspace as the entrypoint where all databases and metadata will be stored.
Think of it as a file system residing on the cloud.

information_schema: Top Notion page in the workspace where all table metadata are stored.

tables: Notion database for storing all table metadata in the store. 
The tables schema is as follows:
  - table_name: name of a database table
  - table_catalog: name of the database to which the table belongs. 
    A table catalog is a Notion page at the top of the workspace
  - table_id: Notion object id of the Notion database corresponding to the table 

Database: A database is a Notion top page. All tables belonging to this database are 
Notion databases contained in this page. 

Create a new database (new Notion page)
1. Search for a Notion page with the database name
2. No page is found, so create a new page

Create a new table (new Notion database)
1. Create a new Notion database under the page respresenting the current database
2. Update the tables Notion database under the page information_schema
"""

Environment = namedtuple('Environment', ['NOTION_TOKEN', 'NOTION_VERSION'])

@pytest.fixture
def int_env() -> Environment:
    return Environment('ntn_abc123def456ghi789jkl012mno345pqr', '2022-06-28')

def test_parse_uri_for_inmemory_integration():
    uri: NotionURI = _parse_uri('normlite:///:memory:')
    assert isinstance(uri, NotionSimulatedURI)
    assert uri.kind == 'simulated'
    assert uri.mode == 'memory'
    assert uri.path is None

def test_parse_uri_for_file_integration():
    uri: NotionURI = _parse_uri('normlite:///path/to/my/test-integration.db')
    assert isinstance(uri, NotionSimulatedURI)
    assert uri.kind == 'simulated'
    assert uri.mode == 'file'
    assert uri.path == '/path/to/my/test-integration.db'    # leading '/' is crucial, this is an absolute path
    assert uri.file == 'test-integration.db'   

def test_parse_uri_for_internal_integration(int_env: Environment):
    uri: NotionURI = _parse_uri(
        f'normlite+auth://internal?token={int_env.NOTION_TOKEN}&version={int_env.NOTION_VERSION}'
    )
    assert isinstance(uri, NotionAuthURI)
    assert uri.kind == 'internal'
    assert uri.token == int_env.NOTION_TOKEN
    assert uri.version == int_env.NOTION_VERSION
    assert uri.client_id is None
    assert uri.client_secret is None
    assert uri.auth_url is None

def test_create_in_memory_engine():
    engine: Engine = create_engine(
        'normlite:///:memory:',
        _mock_ws_id = '12345678-0000-0000-1111-123456789012',
        _mock_db_parent_id = '87654321-4444-4444-4444-210987654321',
        _mock_ischema_page_id = 'abababab-3333-3333-3333-abcdefghilmn',
        _mock_tables_id = '66666666-6666-6666-6666-666666666666'
        )

    assert engine._database == 'memory'
    assert engine._ws_id == '12345678-0000-0000-1111-123456789012'
    assert engine._db_parent_id == engine._client._get_by_title(engine._database, 'page').get('id')

def test_engine_inspect_has_table():
    engine: Engine = create_engine(
        'normlite:///:memory:',
        _mock_ws_id = '12345678-0000-0000-1111-123456789012',
        _mock_db_parent_id = '87654321-4444-4444-4444-210987654321',
        _mock_ischema_page_id = 'abababab-3333-3333-3333-abcdefghilmn',
        _mock_tables_id = '66666666-6666-6666-6666-666666666666'
        )
    
    # create a row in tables for the table 'students' to be looked up
    payload = {
        'parent': {
            'type': 'database_id',
            'database_id': engine._tables_id
        },
        'properties': {
            'table_name': {'title': [{'text': {'content': 'students'}}]},
            'table_schema': {'rich_text': [{'text': {'content': ''}}]},
            'table_catalog': {'rich_text': [{'text': {'content': 'memory'}}]},
            'table_id': {'rich_text': [{'text': {'content': '66666666-6666-6666-6666-666666666699'}}]}
        } 

    }
    students_ischema_row = engine._client.pages_create(payload)
    
    inspector = engine.inspect()
    # TODO: Decide whether tables should also have a row in the itself.
    # Why is this important?
    # inspector.has_table() could be implemented as a lookup in tables, and if tables is not
    # in there, it will not be found.
    assert inspector.has_table('tables')

def test_engine_inspector_reflect_table():
    engine: Engine = create_engine('normlite:///:memory:')
    inspector = engine.inspect()
    tables = Table('tables')
    tables =  inspector.reflect_table('tables')

    












