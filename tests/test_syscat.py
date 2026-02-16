import pdb
import pytest

from normlite.engine.base import Engine, create_engine
from normlite.engine.systemcatalog import SystemCatalog
from normlite.notiondbapi.dbapi2 import ProgrammingError, InternalError

def create_students_db(engine: Engine) -> str:
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
