from collections import namedtuple
import pdb
import pytest

from normlite.engine.base import Engine, Inspector, NotionAuthURI, NotionURI, _parse_uri, create_engine, NotionSimulatedURI, Connection
from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.notion_sdk.getters import get_object_id, get_object_type, get_parent_id, get_parent_type, get_properties, get_property, get_rich_text_property_value, get_title
from normlite.notiondbapi.dbapi2 import Cursor
from normlite.sql.ddl import CreateTable
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, Number, String

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

@pytest.fixture
def engine() -> Engine:
    return create_engine(
        'normlite:///:memory:',
        _mock_ws_id = '12345678-0000-0000-1111-123456789012',
        _mock_ischema_page_id = 'abababab-3333-3333-3333-abcdefghilmn',
        _mock_tables_id = '66666666-6666-6666-6666-666666666666',
        _mock_db_page_id = '12345678-9090-0606-1111-123456789012'
    )

@pytest.fixture
def inspector(engine: Engine) -> Inspector:
    return engine.inspect()

def create_students_db(engine: Engine) -> None:
    # create a new table students in memory
    db = engine._client._add('database', {
        'parent': {
            'type': 'page_id',
            'page_id': engine._user_tables_page_id
        },
        "title": [
            {
                "type": "text",
                "text": {
                    "content": "students",
                    "link": None
                },
                "plain_text": "students",
                "href": None
            }
        ],
        'properties': {
            'student_id': {'number': {}},
            'name': {'title': {}},
            'grade': {'rich_text': {}},
            'is_active': {'checkbox': {}}
        }
    })

    # add the students to tables
    engine._client._add('page', {
        'parent': {
            'type': 'database_id',
            'database_id': engine._tables_id
        },
        'properties': {
            'table_name': {'title': [{'text': {'content': 'students'}}]},
            'table_schema': {'rich_text': [{'text': {'content': ''}}]},
            'table_catalog': {'rich_text': [{'text': {'content': 'memory'}}]},
            'table_id': {'rich_text': [{'text': {'content': db.get('id')}}]}
        }
    })

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

#---------------------------------------------------
# Bootstrap tests
#---------------------------------------------------

def test_user_tables_page_created(engine: Engine):
    existing = engine._client._get_by_title('memory', 'page')
    assert len(existing['results']) == 1

    user_tables_page = existing['results'][0]
    assert get_title(user_tables_page) == 'memory'


def test_information_schema_page_created(engine: Engine):
    existing = engine._client._get_by_title('information_schema', 'page')
    assert len(existing['results']) == 1
    
    info_schema_page = existing['results'][0]
    assert get_title(info_schema_page) == 'information_schema'

def test_tables_database_created(engine: Engine):
    existing = engine._client._get_by_title('tables', 'database')
    assert len(existing['results']) == 1

    tables = existing['results'][0]
    name = get_title(tables)
    tables_id = get_object_id(tables)
    assert name == 'tables'
    assert tables_id == engine._tables_id

def test_tables_page_created(engine: Engine):
    existing = engine._client.databases_query(
        {
            "database_id": engine._tables_id,
            "filter": {
                "property": "table_name",
                "title": {"equals": "tables"},
            },
        }
    )

    assert len(existing['results']) == 1
    tables = existing['results'][0]
    assert get_parent_id(tables) == engine._tables_id
    table_schema = get_property(tables, 'table_schema')
    table_catalog = get_property(tables, 'table_catalog')
    table_id = get_property(tables, 'table_id')
    assert  get_rich_text_property_value(table_schema) == 'information_schema'
    assert  get_rich_text_property_value(table_catalog) == 'memory'
    assert  get_rich_text_property_value(table_id) == engine._tables_id

def test_engine_connect(engine: Engine):
    with engine.connect() as connection:
        connection: Connection = engine.connect()
        assert connection.connection

        cursor: Cursor = connection.connection.cursor()
        assert isinstance(cursor._client, InMemoryNotionClient)
        assert cursor._client is engine._client

#---------------------------------------------------
# Inspection tests
#---------------------------------------------------

def test_engine_inspect_has_table_sys(engine: Engine, inspector: Inspector):
    assert inspector.has_table('tables')

def test_engine_inspect_has_table_user(engine: Engine, inspector: Inspector):
    create_students_db(engine)
    assert inspector.has_table('students')

def test_engine_inspect_has_table_dropped_user_table(engine: Engine, inspector: Inspector):
    create_students_db(engine)
    
    # mock students table gets dropped
    entry = engine._find_sys_tables_row('students', table_catalog='memory')
    sys_rowid = entry.sys_rowid
    engine._client._store[sys_rowid]['in_trash'] = True

    assert not inspector.has_table('students')

@pytest.mark.skip('reflection requires refactor')
def test_engine_inspector_reflect_sys_table(engine: Engine, inspector: Inspector):
    metadata = MetaData()
    tables: Table = Table('tables', metadata)
    #pdb.set_trace()
    inspector.reflect_table(tables)

    assert tables._database_id == engine._tables_id
    assert 'table_name' in tables.c
    assert 'table_schema' in tables.c
    assert 'table_catalog' in tables.c
    assert 'table_id' in tables.c

@pytest.mark.skip('reflection requires refactor')
def test_engine_inspector_reflect_user_table(engine: Engine, inspector: Inspector):
    create_students_db(engine)
    metadata = MetaData()
    students: Table = Table('students', metadata)
    inspector.reflect_table(students)
    assert 'student_id' in students.c
    assert isinstance(students.c.student_id.type_, Number)
    assert students.c.student_id.type_.format == 'number'
    
    assert 'name' in students.c
    assert isinstance (students.c.name.type_, String)
    assert students.c.name.type_.is_title

    assert 'grade' in students.c
    assert isinstance(students.c.grade.type_, String)
    assert not students.c.grade.type_.is_title

    assert 'is_active' in students.c
    assert isinstance(students.c.is_active.type_, Boolean)
        










