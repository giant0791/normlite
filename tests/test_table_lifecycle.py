from typing import NoReturn, Optional

import pytest

from normlite.engine.base import Engine, create_engine
from normlite.engine.systemcatalog import TableState
from normlite.exceptions import InvalidRequestError
from normlite.notion_sdk.getters import get_object_id, get_object_type
from normlite.notiondbapi.dbapi2 import InternalError, ProgrammingError
from normlite.sql.ddl import CreateTable
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, String

@pytest.fixture
def engine() -> Engine:
    return create_engine('normlite:///:memory:')

@pytest.fixture
def students() -> Table:
    metadata = MetaData()
    return Table(
        'students',
        metadata,
        Column('id', Integer()),
        Column('name', String(is_title=True)),
        Column('grade', String()),
        Column('is_active', Boolean()),
        Column('started_on', Date()),
    )

class TableStateProxy:
    def __init__(self, name: str, catalog: str, engine: Engine):
        self._state: TableState = None
        self._name = name
        self._catalog = catalog
        self._engine = engine

    def __enter__(self):
        self._state = self._engine.get_table_state(
            table_name = self._name,
            table_catalog=self._catalog
        )
        return self

    def __exit__(self, exc_type, exc, tb):
        self._state = None

    def _raise_if_uninitialized(self) -> NoReturn:
        if self._state is None:
            raise InvalidRequestError('Table state proxy must be used as context manager.')

    @property
    def is_missing(self) -> Optional[bool]:
        self._raise_if_uninitialized()
        return self._state is TableState.MISSING

    @property
    def is_active(self) -> Optional[bool]:
        self._raise_if_uninitialized()
        return self._state is TableState.ACTIVE
    
    @property
    def is_dropped(self) -> Optional[bool]:
        self._raise_if_uninitialized()
        return self._state is TableState.DROPPED
    
    @property
    def is_orphaned(self) -> Optional[bool]:
        self._raise_if_uninitialized()
        return self._state is TableState.ORPHANED
    

def create(self: Table, bind: Engine, checkfirst: bool = False) -> None:
    catalog = bind._user_database_name
    execution_options = bind.get_execution_options()
    restore_dropped = execution_options.get("restore_dropped", False)

    with TableStateProxy(self.name, catalog, bind) as state:

        # ------------------------
        # Lifecycle resolution
        # ------------------------

        if state.is_active:
            if checkfirst:
                return
            raise ProgrammingError(
                f"Table '{self.name}' already exists in catalog '{catalog}'"
            )

        if state.is_dropped:
            if restore_dropped:
                entry = bind.restore_table(
                    self.name,
                    table_catalog=catalog,
                )

                # update write-cache
                self._sys_tables_page_id = entry.sys_tables_page_id
                return

            # explicit non-restore → raises
            raise ProgrammingError(
                f"Table '{self.name}' is dropped. "
                f"Use execution_options(restore_dropped=True) to restore it."
            )

            # fall through to physical creation

        if state.is_orphaned:
            raise InternalError(
                f"Table '{self.name}' is orphaned."
            )

    # ------------------------
    # MISSING → physical CREATE
    # ------------------------
        
    # IMPORTANT: The user tables page id **must** be set prior to executing
    # the CreateTable statement.
    self._db_parent_id = bind._user_tables_page_id
    ddl_stmt = CreateTable(self)
    with bind.connect() as connection:
        execution_options = {
            "isolation_level": "AUTO COMMIT"
        }

        _ = connection.execute(
            ddl_stmt, 
            execution_options=execution_options
        )

def test_create_table_not_existing(engine: Engine, students: Table):
    table = students
    create(table, engine, checkfirst=True)
    entry = engine.find_table_metadata('students', table_catalog=engine._user_database_name)
    assert entry is not None

    database_obj = engine._client._get_by_id(entry.table_id)
    assert  get_object_type(database_obj) == 'database'
    assert get_object_id(database_obj) == students.get_oid()

