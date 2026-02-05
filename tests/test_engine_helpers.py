from __future__ import annotations
import pdb
import pytest

from normlite.engine.base import Engine, SystemTablesEntry, create_engine
from normlite.notiondbapi.dbapi2 import ProgrammingError


@pytest.fixture
def engine() -> Engine:
    return create_engine(
        'normlite:///:memory:',
        _mock_ws_id = '12345678-0000-0000-1111-123456789012',
        _mock_ischema_page_id = 'abababab-3333-3333-3333-abcdefghilmn',
        _mock_tables_id = '66666666-6666-6666-6666-666666666666',
        _mock_db_page_id = '12345678-9090-0606-1111-123456789012'
    )

def test_sys_tables_exists(engine: Engine):
    must_exist = engine._find_sys_tables_row(
        "tables",
        table_catalog="memory"
    )
    
    assert must_exist is not None

def test_sys_tables_contains_self_row(engine: Engine):
    sys_tables: SystemTablesEntry = engine._find_sys_tables_row(
        "tables",
        table_catalog="memory"
    )

    assert sys_tables.name == 'tables'
    assert sys_tables.schema == 'information_schema'
    assert sys_tables.catalog == 'memory'
    assert sys_tables.table_id == engine._tables_id
    assert not sys_tables.is_dropped

def test_engine_ensures_new_table_does_not_exist_in_sys_tables(engine: Engine):
    engine._get_or_create_sys_tables_row(
        'students',
        table_catalog=engine._user_database_name,
        table_id=engine._tables_id,
        if_exists=True
    )

    with pytest.raises(ProgrammingError, match='students'):
        engine._get_or_create_sys_tables_row(
            'students',
            table_catalog=engine._user_database_name,
            table_id=engine._tables_id,
            if_exists=True
        )
