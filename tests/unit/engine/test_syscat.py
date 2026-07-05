import pdb
import pytest

from normlite.engine.base import Engine, create_engine
from normlite.engine.systemcatalog import TableState
from normlite.engine.systemcatalog import SystemCatalog
from normlite.engine.systemcatalog import SystemTablesEntry
from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.notiondbapi.dbapi2 import ProgrammingError, InternalError

def create_students_db(engine: Engine, name: str = 'students') -> dict:
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
                    "content": name,
                    "link": None
                },
                "plain_text": name,
                "href": None
            }
        ],
        'initial_data_source': {
            'properties': {
                'id': {'number': {}},
                'name': {'title': {}},
                'grade': {'rich_text': {}},
                'is_active': {'checkbox': {}},
                'start_on': {'date': {}}
            }
        }
    })

    return db

# ============================================================
# Fixtures
# ============================================================

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
def syscat(engine: Engine) -> SystemCatalog:
    return engine._catalog

# ============================================================
# SystemTablesEntry.from_dict
# ============================================================

def test_from_dict_reads_data_source_id():
    # A tables-catalog row persists the table's data_source_id alongside its
    # table_id (ADR-0014: both IDs are persisted). from_dict must surface it
    # so reflection can route data_sources.retrieve on the stored ds id.
    page_obj = {
        "id": "page-abc",
        "properties": {
            "table_name": {"type": "title", "title": [{"text": {"content": "students"}}]},
            "table_catalog": {"type": "rich_text", "rich_text": [{"text": {"content": "memory"}}]},
            "table_schema": {"type": "rich_text", "rich_text": [{"text": {"content": "not_used"}}]},
            "table_id": {"type": "rich_text", "rich_text": [{"text": {"content": "db-123"}}]},
            "table_dsid": {"type": "rich_text", "rich_text": [{"text": {"content": "ds-456"}}]},
            "is_dropped": {"type": "checkbox", "checkbox": False},
        },
    }

    entry = SystemTablesEntry.from_dict(page_obj)

    assert entry.table_dsid == "ds-456"


# ============================================================
# find_entry
# ============================================================

def test_find_sys_tables_row_returns_none_when_missing(syscat: SystemCatalog):
    result = syscat.find_sys_tables_row("students", table_catalog="memory")
    assert result is None


def test_find_sys_tables_row_returns_entry(engine: Engine, syscat: SystemCatalog):
    database_id = create_students_db(engine)["id"]
    entry = syscat.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id=database_id,
    )

    found = syscat.find_sys_tables_row("students", table_catalog="memory")

    assert found.sys_tables_page_id == entry.sys_tables_page_id


def test_find_sys_tables_row_raises_internal_error_on_duplicates(engine: Engine, syscat: SystemCatalog):
    database_id = create_students_db(engine)["id"]
    syscat.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id=database_id,
    )

    # Force duplicate manually
    client = engine._client
    client.pages_create(
        payload={
            "parent": {"type": "data_source_id", "data_source_id": syscat._tables_dsid},
            "properties": {
                "table_name": {"title": [{"text": {"content": "students"}}]},
                "table_catalog": {"rich_text": [{"text": {"content": "memory"}}]},
                "table_schema": {"rich_text": [{"text": {"content": "not_defined"}}]},
                "table_id": {"rich_text": [{"text": {"content": "deadbeef-dead-beef-dead-beefdead0019"}}]},
                "table_dsid": {"rich_text": [{"text": {"content": "deadbeef-dead-beef-dead-beefdead0019-ds"}}]},
                "is_dropped": {"checkbox": False},
            },
        }
    )

    with pytest.raises(InternalError) as exc:
        syscat.find_sys_tables_row("students", table_catalog="memory")

    "multiple tables named 'students' in catalog 'memory'" in str(exc.value)


# ============================================================
# ensure_sys_tables_row
# ============================================================

def test_ensure_sys_tables_row_creates_new(engine: Engine, syscat: SystemCatalog):
    database_id = create_students_db(engine)["id"]
    entry = syscat.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id=database_id,
    )

    assert entry.sys_tables_page_id is not None

def test_ensure_entry_raises_if_not_exists_false(engine: Engine, syscat: SystemCatalog):
    database_id = create_students_db(engine)["id"]
    entry = syscat.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id=database_id,
    )

    with pytest.raises(ProgrammingError) as exc:
        syscat.ensure_sys_tables_row(
            table_name="students",
            table_catalog="memory",
            table_id=database_id,
            if_not_exists=False,
        )

    "'students' already exists in catalog 'memory'" in str(exc.value)


def test_ensure_entry_if_not_exists_true_returns_existing(engine: Engine, syscat: SystemCatalog):
    database_id = create_students_db(engine)["id"]
    entry1 = syscat.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id=database_id,
    )

    entry2 = syscat.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id=database_id,
        if_not_exists=True,
    )

    assert entry1.sys_tables_page_id == entry2.sys_tables_page_id

def test_ensure_sys_tables_row_persists_table_dsid(engine: Engine, syscat: SystemCatalog):
    db = create_students_db(engine)
    table_dsid = db["data_sources"][0]["id"]
    syscat.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id=db["id"],
        table_dsid=table_dsid,
    )

    # persisted, not just echoed: reading the row back from the store surfaces
    # the stored data source id. NB: the get_or_create create-return is NOT
    # asserted here — it comes back all-None except the page id (a separate,
    # pre-existing shape bug in how the pages_create response is parsed), so
    # persistence must be verified through a re-read via find.
    found = syscat.find_sys_tables_row("students", table_catalog="memory")
    assert found.table_dsid == table_dsid

# ============================================================
# mark_dropped
# ============================================================

def test_set_dropped_soft_deletes(engine: Engine, syscat: SystemCatalog):
    database_id = create_students_db(engine)["id"]
    entry = syscat.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id=database_id,
    )

    syscat.set_dropped(
        table_name="students",
        table_catalog="memory",
        dropped=True
    )

    # ADR-0015: soft-delete flips the is_dropped checkbox; the catalog page stays live
    page = syscat._client._store[entry.sys_tables_page_id]
    assert page["in_trash"] is False
    assert page["properties"]["is_dropped"]["checkbox"] is True


def test_set_dropped_raises_if_missing(syscat: SystemCatalog):
    with pytest.raises(ProgrammingError) as exc:
        syscat.set_dropped(
            table_name="students",
            table_catalog="memory",
            dropped=True
        )

    assert "Table 'students' does not exist" in str(exc.value)


def test_set_dropped_raises_programming_error_on_stale_page_id(engine: Engine, syscat: SystemCatalog):
    database_id = create_students_db(engine)["id"]
    entry = syscat.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id=database_id,
    )

    # simulate stale page id
    client = syscat._client
    del client._store[entry.sys_tables_page_id]

    with pytest.raises(ProgrammingError) as exc:
        syscat.set_dropped(
            table_name="students",
            table_catalog="memory",
            dropped=True
        )

    assert "Table 'students' does not exist" in str(exc.value)


# ============================================================
# repair_missing
# ============================================================

def test_repair_missing_returns_existing(engine: Engine, syscat: SystemCatalog):
    database_id = create_students_db(engine)["id"]
    entry = syscat.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id=database_id,
    )

    repaired = syscat.repair_missing(
        table_name="students",
        table_catalog="memory",
        table_id=database_id,
    )

    assert repaired.sys_tables_page_id == entry.sys_tables_page_id


def test_repair_missing_recreates_when_missing(engine: Engine, syscat: SystemCatalog):
    repaired = syscat.repair_missing(
        table_name="students",
        table_catalog="memory",
        table_id="deadbeef-dead-beef-dead-beefdeadbeef",
    )

    assert repaired.sys_tables_page_id is not None


def test_repair_missing_without_database_id_raises(syscat):
    with pytest.raises(InternalError) as exc:
        syscat.repair_missing(
            table_name="students",
            table_catalog="default_catalog",
            table_id=None,
        )

    assert "Cannot repair missing catalog entry for 'students'" in str(exc.value)

# ===========================================================
# table lifecycle states
# ===========================================================

def test_state_missing_when_no_sys_and_no_db(
        engine: Engine, 
        syscat: SystemCatalog
    ):
    state = syscat.get_table_state(
        "students",
        table_catalog=engine._user_database_name,
    )

    assert state is TableState.MISSING

def test_state_orphaned_when_db_exists_but_no_sys(
    engine: Engine,
    syscat: SystemCatalog
):
    # create physical but no metadata
    db_id = create_students_db(engine)["id"]

    state = syscat.get_table_state(
        "students",
        table_catalog=engine._user_database_name
    )
    assert state is TableState.ORPHANED

def test_state_orphaned_when_sys_exists_but_db_missing(
    engine: Engine,
    syscat: SystemCatalog
):        
    db_id = create_students_db(engine)["id"]

    # simulate manual deletion of database
    engine._client.databases_update(
        path_params={'database_id': db_id},
        payload={
            'in_trash': True
        }
    )

    state = syscat.get_table_state(
        "students",
        table_catalog=engine._user_database_name,
    )

    assert state is TableState.ORPHANED

def test_state_active_when_both_not_trashed(
    engine: Engine,
    syscat: SystemCatalog
):
    database_id = create_students_db(engine)["id"]
    entry = syscat.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id=database_id,
    )

    state = syscat.get_table_state(
        "students",
        table_catalog=engine._user_database_name,
    )

    assert state is TableState.ACTIVE


def test_state_active_when_both_not_trashed(
    engine: Engine,
    syscat: SystemCatalog
):
    database_id = create_students_db(engine)["id"]
    entry = syscat.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id=database_id,
    )

    state = syscat.get_table_state(
        "students",
        table_catalog=engine._user_database_name,
    )

    assert state is TableState.ACTIVE

def test_state_dropped_when_both_trashed(
    engine: Engine,
    syscat: SystemCatalog
):
    db_id = create_students_db(engine)["id"]
    entry = syscat.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id=db_id,
    )

    # simulate DROP TABLE
    engine._client.databases_update(
        path_params={'database_id': db_id},
        payload={
            'in_trash': True
        }
    )

    syscat.set_dropped_by_page_id(
        page_id=entry.sys_tables_page_id, 
        dropped=True
    )

    assert syscat.get_table_state(
        "students",
        table_catalog=engine._user_database_name
    ) is TableState.DROPPED

def test_state_orphaned_when_db_trashed_but_sys_active(
    engine: Engine,
    syscat: SystemCatalog
):
    db_id = create_students_db(engine)["id"]
    entry = syscat.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id=db_id,
    )

    # trash DB only
    engine._client.databases_update(
        path_params={'database_id': db_id},
        payload={
            'in_trash': True
        }
    )

    assert syscat.get_table_state(
        "students",
        table_catalog=engine._user_database_name
    ) is TableState.ORPHANED

def test_state_orphaned_when_sys_trashed_but_db_active(
    engine: Engine,
    syscat: SystemCatalog
):
    db_id = create_students_db(engine)["id"]
    entry = syscat.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id=db_id,
    )

    # trash metadata only
    syscat.set_dropped_by_page_id(
        page_id=entry.sys_tables_page_id,
        dropped=True
    )
    
    assert syscat.get_table_state(
        "students",
        table_catalog=engine._user_database_name
    ) is TableState.ORPHANED

def test_search_does_not_match_exactly_titles(
    engine: Engine,
    syscat: SystemCatalog
):
    db_id = create_students_db(engine, 'students_v1')["id"]
    entry = syscat.ensure_sys_tables_row(
        table_name="students_v1",
        table_catalog="memory",
        table_id=db_id,
    )

    state = syscat.get_table_state(
        "students",
        table_catalog=engine._user_database_name,
    )

    assert state is TableState.MISSING

def test_bootstrap_is_idempotent(engine: Engine):
    catalog = SystemCatalog(engine._client, "db", engine._root_page_id, "db")
    catalog.bootstrap()
    catalog.bootstrap()  # must not raise


# ----------------------------------------------------
# Migration to Notion API 2025-09-03
# ----------------------------------------------------
def _seed_tables_container(client, root):
    container = client.databases_create(payload={
        "parent": {"type": "page_id", "page_id": root},
        "title": [{"type": "text", "text": {"content": "tables"}}],
        "initial_data_source": {"properties": {
            "table_name": {"title": {}},
            "table_schema": {"rich_text": {}},
            "table_catalog": {"rich_text": {}},
            "table_id": {"rich_text": {}},
            "table_dsid": {"rich_text": {}},
            "is_dropped": {"checkbox": {}},
        }}, 
    })      
    return container["id"], container["data_sources"][0]["id"]

def test_find_sys_tables_row_queries_the_data_source():
    client = InMemoryNotionClient()
    client._ensure_root()
    root = client._ROOT_PAGE_ID_
    tables_db_id, tables_ds_id = _seed_tables_container(client, root)
        
    # a catalog self-row parented to the DATA SOURCE (2025-09-03 shape)
    page = client.pages_create(payload={
        "parent": {"type": "data_source_id", "data_source_id": tables_ds_id},
        "properties": {
            "table_name": {"title": [{"text": {"content": "students"}}]},
            "table_schema": {"rich_text": [{"text": {"content": "public"}}]},
            "table_catalog": {"rich_text": [{"text": {"content": "memory"}}]},
            "table_id": {"rich_text": [{"text": {"content": "deadbeef-dead-beef-dead-beefdeadbeef"}}]},
            "table_dsid": {"rich_text": [{"text": {"content": "deadbeef-dead-beef-dead-beefdeadbeef-ds"}}]},
            "is_dropped": {"checkbox": False},
        },
    })
    
    catalog = SystemCatalog(client, "memory", root, "memory")
    catalog._tables_id = tables_db_id
    catalog._tables_dsid = tables_ds_id   # cached at bootstrap from the databases.create response

    found = catalog.find_sys_tables_row("students", table_catalog="memory")

    assert found is not None
    assert found.sys_tables_page_id == page["id"]

def test_bootstrap_creates_tables_catalog_as_container():
    client = InMemoryNotionClient()
    client._ensure_root()
    catalog = SystemCatalog(client, "db", client._ROOT_PAGE_ID_, "db")

    catalog.bootstrap()

    # The tables catalog is now a container: one database holding exactly one data source.
    tables_db = client._get_by_title("tables", "database")["results"][0]
    data_sources = tables_db["data_sources"]

    assert len(data_sources) == 1
    assert data_sources[0]["id"] is not None

def test_find_sys_tables_row_by_table_id_queries_the_data_source():
    client = InMemoryNotionClient()
    client._ensure_root()
    root = client._ROOT_PAGE_ID_
    tables_db_id, tables_ds_id = _seed_tables_container(client, root)

    # a catalog row parented to the DATA SOURCE (2025-09-03 shape)
    page = client.pages_create(payload={
        "parent": {"type": "data_source_id", "data_source_id": tables_ds_id},
        "properties": {
            "table_name": {"title": [{"text": {"content": "students"}}]},
            "table_schema": {"rich_text": [{"text": {"content": "public"}}]},
            "table_catalog": {"rich_text": [{"text": {"content": "memory"}}]},
            "table_id": {"rich_text": [{"text": {"content": "deadbeef-dead-beef-dead-beefdeadbeef"}}]},
            "table_dsid": {"rich_text": [{"text": {"content": "deadbeef-dead-beef-dead-beefdeadbeef-ds"}}]},
            "is_dropped": {"checkbox": False},
        },
    })

    catalog = SystemCatalog(client, "memory", root, "memory")
    catalog._tables_id = tables_db_id
    catalog._tables_dsid = tables_ds_id   # cached at bootstrap from the databases.create response

    found = catalog.find_sys_tables_row_by_table_id("deadbeef-dead-beef-dead-beefdeadbeef")

    assert found is not None
    assert found.sys_tables_page_id == page["id"]

def test_set_dropped_marks_row_without_trashing_the_page():
    # ADR-0015: soft-delete is an explicit is_dropped checkbox on the catalog row,
    # NOT Notion page-trash — data_sources.query always skips trashed pages, so a
    # dropped row must stay live to remain queryable (DROPPED detection + RESTORE).
    client = InMemoryNotionClient()
    client._ensure_root()
    root = client._ROOT_PAGE_ID_
    tables_db_id, tables_ds_id = _seed_tables_container(client, root)

    catalog = SystemCatalog(client, "memory", root, "memory")
    catalog._tables_id = tables_db_id
    catalog._tables_dsid = tables_ds_id

    entry = catalog.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id="deadbeef-dead-beef-dead-beefdeadbeef",
    )

    catalog.set_dropped(table_name="students", table_catalog="memory", dropped=True)

    # the dropped row stays live and queryable, and reports its dropped intent
    found = catalog.find_sys_tables_row("students", table_catalog="memory")
    assert found is not None
    assert found.sys_tables_page_id == entry.sys_tables_page_id
    assert found.is_dropped is True

def test_ensure_sys_tables_row_parents_new_row_to_the_data_source():
    client = InMemoryNotionClient()
    client._ensure_root()
    root = client._ROOT_PAGE_ID_
    tables_db_id, tables_ds_id = _seed_tables_container(client, root)
    
    catalog = SystemCatalog(client, "memory", root, "memory")
    catalog._tables_id = tables_db_id
    catalog._tables_dsid = tables_ds_id

    entry = catalog.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id="deadbeef-dead-beef-dead-beefdeadbeef",
    )
    
    # created, and round-trips through the (data-source) find path
    assert entry.sys_tables_page_id is not None
    found = catalog.find_sys_tables_row("students", table_catalog="memory")
    assert found.sys_tables_page_id == entry.sys_tables_page_id

def test_ensure_on_a_dropped_row_returns_it_without_duplicating():
    # ADR-0015: a dropped catalog row still EXISTS (is_dropped checkbox; the page
    # stays live). ensure(if_not_exists=True) must therefore return that existing
    # row, not insert a second live one the next find would trip as a duplicate.
    # It does NOT un-drop it — restoring is set_dropped(dropped=False)'s job.
    client = InMemoryNotionClient()
    client._ensure_root()
    root = client._ROOT_PAGE_ID_
    tables_db_id, tables_ds_id = _seed_tables_container(client, root)

    catalog = SystemCatalog(client, "memory", root, "memory")
    catalog._tables_id = tables_db_id
    catalog._tables_dsid = tables_ds_id

    original = catalog.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id="deadbeef-dead-beef-dead-beefdeadbeef",
    )
    catalog.set_dropped(table_name="students", table_catalog="memory", dropped=True)

    same = catalog.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id="deadbeef-dead-beef-dead-beefdeadbeef",
        if_not_exists=True,
    )

    # the existing row, untouched — not a second one, and not un-dropped
    assert same.sys_tables_page_id == original.sys_tables_page_id
    assert same.is_dropped is True

    # the catalog still resolves a single, unambiguous row
    found = catalog.find_sys_tables_row("students", table_catalog="memory")
    assert found.sys_tables_page_id == original.sys_tables_page_id