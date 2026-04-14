from collections import namedtuple
import pdb
import pytest

from normlite.engine.base import Engine, Inspector, NotionAuthURI, NotionURI, _parse_uri, NotionSimulatedURI, Connection
from normlite.engine.systemcatalog import TableState
from normlite.exceptions import ArgumentError
from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.notion_sdk.getters import get_object_id, get_parent_id, get_property, get_rich_text_property_value, get_title
from normlite.notiondbapi.dbapi2 import Cursor
from normlite.sql.reflection import ReflectedColumnInfo
from normlite.sql.schema import MetaData, Table
from normlite.sql.type_api import Boolean, Number, String
from tests.utils.assertions import assert_is_valid_uuid4, is_valid_uuid4
from tests.utils.db_helpers import create_students_db

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
def inspector(engine: Engine) -> Inspector:
    return engine.inspect()


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

def test_engine_inspect_has_table_user(engine: Engine, inspector: Inspector):
    create_students_db(engine)
    assert inspector.has_table('students')

def test_has_table_returns_false_if_table_not_in_tables(engine: Engine, inspector: Inspector):
    create_students_db(engine)
    
    assert not inspector.has_table("courses") 

def test_get_oid_returns_valid_uuid(engine: Engine, inspector: Inspector):
    create_students_db(engine)
    students = Table('students', MetaData(), autoload_with=engine)
    oid = inspector.get_oid(students)
    coid = inspector.get_oid(students.c.name)

    assert_is_valid_uuid4(oid)
    assert not is_valid_uuid4(coid)     # column oid is not uuid4

def test_engine_inspect_has_table_dropped_user_table(engine: Engine, inspector: Inspector):
    create_students_db(engine)
    students = Table('students', MetaData(), autoload_with=engine)
    students.drop(engine)
    
    assert not inspector.has_table('students')

def test_is_table_dropped_by_str(engine: Engine, inspector: Inspector):
    create_students_db(engine)

    assert not inspector.is_dropped("students")

def test_is_table_dropped_by_table(engine: Engine, inspector: Inspector):    
    create_students_db(engine)
    students = Table('students', MetaData(), autoload_with=engine)
    students.drop(engine)

    assert inspector.is_dropped(students)

def test_is_table_dropped_wrong_type_raises(engine: Engine, inspector: Inspector):
    with pytest.raises(ArgumentError):
        _ = inspector.is_dropped({"wrong": "type"})

@pytest.mark.skip("Requires table_name refactoring")
def test_engine_inspector_reflect_sys_table(engine: Engine, inspector: Inspector):
    metadata = MetaData()
    tables: Table = Table('tables', metadata)
    inspector.reflect_table(tables)

    assert tables._sys_columns["object_id"]._value == engine._tables_id
    assert 'table_name' in tables.c
    assert 'table_schema' in tables.c
    assert 'table_catalog' in tables.c
    assert 'table_id' in tables.c

def test_engine_inspector_reflect_user_table(engine: Engine, inspector: Inspector):
    create_students_db(engine)
    metadata = MetaData()
    students: Table = Table('students', metadata)
    inspector.reflect_table(students)
    assert 'id' in students.c
    assert isinstance(students.c.id.type_, Number)
    assert students.c.id.type_.format == 'number'
    
    assert 'name' in students.c
    assert isinstance (students.c.name.type_, String)
    assert students.c.name.type_.is_title

    assert 'grade' in students.c
    assert isinstance(students.c.grade.type_, String)
    assert not students.c.grade.type_.is_title

    assert 'is_active' in students.c
    assert isinstance(students.c.is_active.type_, Boolean)
        










