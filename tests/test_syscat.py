import pdb
import pytest

from normlite.engine.base import Engine, create_engine
from normlite.engine.reflection import TableState
from normlite.engine.systemcatalog import SystemCatalog
from normlite.notiondbapi.dbapi2 import ProgrammingError, InternalError

def create_students_db(engine: Engine, name: str = 'students') -> str:
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
        'properties': {
            'id': {'number': {}},
            'name': {'title': {}},
            'grade': {'rich_text': {}},
            'is_active': {'checkbox': {}},
            'start_on': {'date': {}}
        }
    })

    return db.get('id')

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
# find_entry
# ============================================================

def test_find_sys_tables_row_returns_none_when_missing(syscat: SystemCatalog):
    result = syscat.find_sys_tables_row("students", table_catalog="memory")
    assert result is None


def test_find_sys_tables_row_returns_entry(engine: Engine, syscat: SystemCatalog):
    database_id = create_students_db(engine)
    entry = syscat.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id=database_id,
    )

    found = syscat.find_sys_tables_row("students", table_catalog="memory")

    assert found.sys_tables_page_id == entry.sys_tables_page_id


def test_find_sys_tables_row_raises_internal_error_on_duplicates(engine: Engine, syscat: SystemCatalog):
    database_id = create_students_db(engine)
    syscat.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id=database_id,
    )

    # Force duplicate manually
    client = engine._client
    client.pages_create(
        payload={
            "parent": {"type": "database_id", "database_id": engine._tables_id},
            "properties": {
                "table_name": {"title": [{"text": {"content": "students"}}]},
                "table_catalog": {"rich_text": [{"text": {"content": "memory"}}]},
                "table_schema": {"rich_text": [{"text": {"content": "not_defined"}}]},
                "table_id": {"rich_text": [{"text": {"content": "deadbeef-dead-beef-dead-beefdeadbeef"}}]},
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
    database_id = create_students_db(engine)
    entry = syscat.ensure_sys_tables_row(
        table_name="students",
        table_catalog="memory",
        table_id=database_id,
    )

    assert entry.sys_tables_page_id is not None

def test_ensure_entry_raises_if_not_exists_false(engine: Engine, syscat: SystemCatalog):
    database_id = create_students_db(engine)
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
    database_id = create_students_db(engine)
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

# ============================================================
# mark_dropped
# ============================================================

def test_set_dropped_soft_deletes(engine: Engine, syscat: SystemCatalog):
    database_id = create_students_db(engine)
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

    page = syscat._client._store[entry.sys_tables_page_id]
    assert page["in_trash"] is True


def test_set_dropped_raises_if_missing(syscat: SystemCatalog):
    with pytest.raises(ProgrammingError) as exc:
        syscat.set_dropped(
            table_name="students",
            table_catalog="memory",
            dropped=True
        )

    assert "Table 'students' does not exist" in str(exc.value)


def test_set_dropped_raises_programming_error_on_stale_page_id(engine: Engine, syscat: SystemCatalog):
    database_id = create_students_db(engine)
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
    database_id = create_students_db(engine)
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
    db_id = create_students_db(engine)

    state = syscat.get_table_state(
        "students",
        table_catalog=engine._user_database_name
    )
    assert state is TableState.ORPHANED

def test_state_orphaned_when_sys_exists_but_db_missing(
    engine: Engine,
    syscat: SystemCatalog
):        
    db_id = create_students_db(engine)

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
    database_id = create_students_db(engine)
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
    database_id = create_students_db(engine)
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
    db_id = create_students_db(engine)
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
    db_id = create_students_db(engine)
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
    db_id = create_students_db(engine)
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
    db_id = create_students_db(engine, 'students_v1')
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

