from collections import namedtuple
import json
import pdb
import pytest

from normlite.engine.base import Engine, Inspector, NotionAuthURI, NotionURI, _parse_uri, NotionSimulatedURI, Connection, create_engine
from normlite.engine.systemcatalog import TableState
from normlite.exceptions import ArgumentError, InvalidRequestError
from normlite.notion_sdk.client import FileBasedNotionClient, InMemoryNotionClient
from normlite.notion_sdk.getters import get_object_id, get_parent_id, get_property, get_rich_text_property_value, get_title
from normlite.notiondbapi.dbapi2 import Cursor
from normlite.sql.dml import insert, select
from normlite.sql.reflection import ReflectedColumnInfo
from normlite.sql.schema import Column, MetaData, Table
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

#@pytest.mark.skip("Requires table_name refactoring")
def test_engine_inspector_reflect_sys_table(engine: Engine, inspector: Inspector):
    metadata = MetaData()
    tables: Table = Table('tables', metadata)
    inspector.reflect_table(tables)

    assert tables._sys_columns["object_id"]._value == engine._tables_id
    assert 'table_name' not in tables.c
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
        
# -----------------------------------------------
# Engine disposal
# -----------------------------------------------

def test_connect_after_dispose_raises_invalid_request_error():
    # Arrange: a freshly constructed in-memory engine that we then dispose
    engine = create_engine("normlite:///:memory:")
    engine.dispose()

    # Act + Assert: any further attempt to obtain a Connection must be rejected
    with pytest.raises(InvalidRequestError, match="Engine has been disposed"):
        engine.connect()


def test_raw_connection_after_dispose_raises_invalid_request_error():
    # Arrange: a freshly constructed in-memory engine that we then dispose
    engine = create_engine("normlite:///:memory:")
    engine.dispose()

    # Act + Assert: the lower-level DBAPI handle must also reject use after disposal
    with pytest.raises(InvalidRequestError, match="Engine has been disposed"):
        engine.raw_connection()

def test_disposed_property_reflects_engine_lifecycle_state():
    # Arrange: a freshly constructed in-memory engine
    engine = create_engine("normlite:///:memory:")

    # Assert (pre): a brand-new engine is not disposed
    assert engine.disposed is False

    # Act: dispose the engine
    engine.dispose()

    # Assert (post): the engine now reports itself as disposed
    assert engine.disposed is True

def test_dispose_calls_underlying_client_close(monkeypatch):
    # Arrange: a freshly constructed in-memory engine
    engine = create_engine("normlite:///:memory:")

    # Instrument the engine's collaborator so we can observe the disposal-time interaction.
    # InMemoryNotionClient.close() is a no-op, so without instrumentation the cascade
    # would have no observable side effect — the acceptance criterion would be untestable.
    close_calls = []
    monkeypatch.setattr(
        engine._client,
        "close",
        lambda: close_calls.append("called"),
    )

    # Act
    engine.dispose()

    # Assert: dispose() delegated termination to the client exactly once
    assert close_calls == ["called"]

def test_dispose_is_idempotent_and_does_not_double_call_close(monkeypatch):
    # Arrange: a freshly constructed in-memory engine, with the client's close()
    # instrumented so we can count invocations
    engine = create_engine("normlite:///:memory:")
    close_calls = []
    monkeypatch.setattr(
        engine._client,
        "close",
        lambda: close_calls.append("called"),
    )

    # Act: dispose twice in succession; the second call must not raise
    engine.dispose()
    engine.dispose()

    # Assert: close() was delegated to the client exactly once across both dispose() calls,
    # and the engine remains in its terminal state
    assert close_calls == ["called"]
    assert engine.disposed is True

def test_dispose_marks_engine_terminal_even_when_client_close_raises(monkeypatch):
    # Arrange: a freshly constructed in-memory engine whose client.close()
    # will raise a real I/O error
    engine = create_engine("normlite:///:memory:")

    def failing_close():
        raise OSError("disk full")

    monkeypatch.setattr(engine._client, "close", failing_close)

    # Act + Assert (propagation): dispose() must surface the underlying I/O exception
    # unchanged — no wrapping, no swallowing
    with pytest.raises(OSError, match="disk full"):
        engine.dispose()

    # Assert (terminal-state invariant): even though close() raised, the engine has
    # committed to its disposed state. "After dispose has been called, the engine is
    # over" — whether it returned cleanly or not.
    assert engine.disposed is True

def test_with_block_disposes_engine_on_clean_exit():
    # Arrange + Act: open the engine in a with-statement and let it exit cleanly
    with create_engine("normlite:///:memory:") as engine:
        # Inside the block, the `as` target points at a live engine — not yet disposed
        assert engine.disposed is False

    # After the with-block exits, the engine is in its terminal state
    assert engine.disposed is True

def test_with_block_propagates_exception_and_still_disposes_engine():
    # Act + Assert (propagation): the RuntimeError raised inside the inner with-block
    # must escape — the context manager must not swallow it
    with pytest.raises(RuntimeError, match="boom"):
        with create_engine("normlite:///:memory:") as engine:
            raise RuntimeError("boom")

    # Assert (cleanup-on-error): even though the block exited via an exception,
    # __exit__ disposed the engine
    assert engine.disposed is True

def test_create_engine_with_file_uri_constructs_file_based_client(tmp_path):
    # Arrange: a file URI pointing into a per-test tmp directory.
    # The path does not need to pre-exist — without read_only=True, the create-new
    # path is legitimate (see slice #296).
    store_path = tmp_path / "store.json"
    file_uri = f"normlite:{store_path}"

    # Act: build the engine. root_page_id is required for file URIs (existing contract).
    engine = create_engine(file_uri)

    # Assert: the engine constructed a file-backed client...
    assert isinstance(engine._client, FileBasedNotionClient)

    # ...and the client points at the path encoded in the URI.
    assert engine._client._path == store_path

def test_create_engine_rejects_read_only_with_memory_uri():
    # Act + Assert: read_only is meaningless against a :memory: URI — there is no
    # file to read from and no file to protect from writes — so the engine must
    # reject the misuse at construction time.
    with pytest.raises(ArgumentError, match="read_only is only valid for file-based URIs"):
        create_engine("normlite:///:memory:", read_only=True)

def test_create_engine_passes_read_only_through_to_file_based_client(tmp_path):
    # Arrange: a minimal valid store on disk. With read_only=True and the default
    # auto_load=True, the client requires the file to exist (slice #296's check),
    # so we seed an empty-but-versioned store.
    store_path = tmp_path / "fixture.json"
    store_path.write_text(json.dumps({"version": 1, "objects": {}}))
    file_uri = f"normlite:{store_path}"

    # Act: build the engine with read_only=True
    engine = create_engine(file_uri, read_only=True)

    # Assert: the kwarg traveled all the way through Engine.__init__ and
    # _create_client into the FileBasedNotionClient constructor.
    assert isinstance(engine._client, FileBasedNotionClient)
    assert engine._client._read_only is True

def test_file_engine_round_trip_persists_data_and_preserves_file_under_read_only(tmp_path):
    # Arrange: a file URI pointing into tmp_path; the file does not yet exist
    store_path = tmp_path / "store.json"
    file_uri = f"normlite:{store_path}"

    # --- Phase 1: write a row through a file-backed engine ---
    with create_engine(file_uri) as engine:
        metadata = MetaData()
        students = Table(
            "students",
            metadata,
            Column("name", String(is_title=True)),
        )
        metadata.create_all(engine)
        with engine.connect() as connection:
            connection.execute(insert(students).values(name="Galileo"))

    # After the write block exits, the store has been flushed to disk
    assert store_path.exists()
    bytes_after_write = store_path.read_bytes()

    # --- Phase 2: reopen read-only, REFLECT the table (no writes), query ---
    with create_engine(file_uri, read_only=True) as engine:
        metadata = MetaData()
        students = Table("students", metadata)                 # empty — reflection fills it
        engine.inspect().reflect_table(students)
        with engine.connect() as connection:
            result = connection.execute(select(students))
            rows = result.all()

    # The inserted row survived the dispose → reopen round-trip
    assert len(rows) == 1
    assert rows[0].name == "Galileo"

    # The read-only session did not modify the file on disk
    assert store_path.read_bytes() == bytes_after_write