from __future__ import annotations
import pdb
from typing import Iterable
from unittest.mock import Mock, patch
import pytest

from normlite import (
    DuplicateColumnError,
    Column, ColumnCollection, PrimaryKeyConstraint, Table,
    ArchivalFlag, Date, Integer, ObjectId, String
)
from normlite._constants import SpecialColumns
from normlite.engine.base import Engine, create_engine
from normlite.engine.reflection import TableState
from normlite.exceptions import ArgumentError, CompileError, NoSuchTableError
from normlite.notion_sdk.client import NotionError
from normlite.notion_sdk.getters import get_object_id, get_object_type
from normlite.notiondbapi.dbapi2 import InternalError, ProgrammingError
from normlite.sql.ddl import DropTable
from normlite.sql.schema import MetaData
from normlite.sql.type_api import Boolean

@pytest.fixture
def metadata() -> MetaData:
    return MetaData()

@pytest.fixture
def students(metadata: MetaData) -> Table:
    students = Table(
        'students',
        metadata,
        Column('name', String(is_title=True)),
        Column('id', Integer()),
        Column('is_active', Boolean()),
        Column('start_on', Date()),
        Column('grade',  String())
    )
    
    return students

@pytest.fixture
def engine() -> Engine:
    return create_engine(
        'normlite:///:memory:',
        _mock_ws_id = '12345678-0000-0000-1111-123456789012',
        _mock_ischema_page_id = 'abababab-3333-3333-3333-abcdefghilmn',
        _mock_tables_id = '66666666-6666-6666-6666-666666666666',
        _mock_db_page_id = '12345678-9090-0606-1111-123456789012'
    )

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
            'id': {'number': {}},
            'name': {'title': {}},
            'grade': {'rich_text': {}},
            'is_active': {'checkbox': {}},
            'start_on': {'date': {}}
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

# --------------------------------------
# Column collection tests
#---------------------------------------
def test_columncollection_getattr(students: Table):
    sc = students.columns
 
    assert isinstance(sc.start_on, Column)
    assert not sc.start_on.primary_key

def test_columncollection_getitem(students: Table):
    sc = students.columns
 
    assert isinstance(sc['id'], Column)
    assert isinstance(sc[0], Column)
    assert not sc['id'].primary_key
    assert not sc[0].primary_key
    assert len(sc[0:3]) == 3

def test_columncollection_contains(students: Table):
    uc = students.get_user_defined_colums()
 
    assert 'id' in uc
    assert not '_no_id' in uc

def test_columncollection_getitem_wrong_index(students: Table):
    sc = students.columns
    index = len(sc)
    with pytest.raises(IndexError, match=f'{index}'):
        col = sc[index]

def test_columncollection_add(students: Table):
    sc = students.columns
    fake_id = Column("fake_id", ObjectId(), primary_key=True)
    fake_id._set_parent(students)
    fake_archived = Column("fake_archived", ArchivalFlag())
    fake_archived._set_parent(students)
    sc.add(fake_id)
    sc.add(fake_archived)

    assert len(sc) == 10
    assert isinstance(sc.fake_id, Column)
    assert isinstance(sc.fake_archived, Column)

def test_columncollection_getitem_wrong_key(students: Table):
    sc = students.columns
    key = 'does_not_exist'
    with pytest.raises(KeyError, match=f'{key}'):
        col = sc[key]

def test_columncollection_asreadonly(students: Table):
    sc = students.columns
    ro_sc = sc.as_readonly()
    with pytest.raises(TypeError, match='object is immutable and/or readonly.'):
        del ro_sc['student_id']

def test_columncollection_no_duplicates(students: Table):
    sc = students.columns 
    dup_col = 'id'
    with pytest.raises(DuplicateColumnError, match=f'not allow duplicate columns: {dup_col}'):
        sc.add(Column('id', String()))

# --------------------------------------
# Table tests
#---------------------------------------        

def test_table_construct():
    metadata = MetaData()
    students = Table(
        'students',
        metadata,
        Column('student_id', Integer(), primary_key=True),
        Column('name', String(is_title=True)),
        Column('grade', String()),
        Column('since', Date())
    )
    assert students.columns._no_id.primary_key
    assert students.columns.student_id.primary_key
    assert not students.columns.name.primary_key
    assert isinstance(students.c['name'].type_, String)
    assert isinstance(students.c['since'].type_, Date)

def test_table_valid_minimal():
    metadata = MetaData()
    t = Table("students", metadata)

    assert t.name == "students"
    assert t.metadata is metadata

def test_table_missing_name():
    metadata = MetaData()

    with pytest.raises(ArgumentError, match="missing required argument 'name'"):
        Table(metadata, Column("id", Integer()))

def test_table_invalid_column_argument():
    metadata = MetaData()

    with pytest.raises(ArgumentError, match="Column objects"):
        Table("students", metadata, Column("id", Integer()), "not-a-column")

def test_table_invalid_autoload_with():
    metadata = MetaData()

    with pytest.raises(ArgumentError, match="autoload_with must be an Engine"):
        Table("students", metadata, autoload_with="engine")

def test_table_autoload_with_and_columns_mutually_exclusive(engine: Engine, metadata: MetaData):
    with pytest.raises(
        ArgumentError,
        match='Columns cannot be specified when using "autoload_with"'
    ):
        Table("students", metadata, Column("id", Integer()), autoload_with=engine)

def test_table_primary_key():
    metadata = MetaData()
    students = Table(
        'students',
        metadata,
        Column('student_id', Integer(), primary_key=True),
        Column('name', String(is_title=True)),
        Column('grade', String()),
        Column('since', Date())
    )

    primary_key = students.primary_key
    assert students.primary_key.table == students
    assert isinstance(primary_key, PrimaryKeyConstraint)
    assert '_no_id' in primary_key.c
    assert 'student_id' in primary_key.c
    assert not 'name' in primary_key.columns

def test_invalid_table_name_empty(metadata: MetaData):
    good = Table('students', metadata)

    with pytest.raises(ArgumentError):
        bad = Table('', metadata)

def test_invalid_table_blanks(metadata: MetaData):
    with pytest.raises(ArgumentError):
        bad = Table(' ', metadata)

    with pytest.raises(ArgumentError):
        bad = Table('bad ', metadata)

    with pytest.raises(ArgumentError):
        bad = Table('bad name', metadata)

    with pytest.raises(ArgumentError):
        bad = Table(' bad', metadata)

def test_create_table_populates_sys_table_page_id(engine: Engine, students: Table):
    # precondition
    assert hasattr(students, "_sys_tables_page_id")
    assert students._sys_tables_page_id is None
    assert engine._catalog is not None

    # act
    students.create(bind=engine)

    # postcondition
    assert students._sys_tables_page_id is not None
    assert isinstance(students._sys_tables_page_id, str)

    # consitency assertion
    row = engine.find_table_metadata(
        "students",
        table_catalog=engine._user_database_name
    )

    assert row.sys_tables_page_id == students._sys_tables_page_id

# --------------------------------------
# Metadata tests
#---------------------------------------

def includes_all(l: list[str], values: Iterable[str]) -> bool:
    for v in values:
        if v not in l:
            return False
    return True

def test_metadata_contains(metadata: MetaData):
    metadata = MetaData()
    students = Table(
        'students', 
        metadata, 
        Column('id', Integer()),
        Column('name', String(is_title=True))
    )
    teachers = Table(
        'teachers',
        metadata,
        Column('id', Integer()),
        Column('name', String(is_title=True))
    )

    assert 'students' in metadata.tables
    assert 'teachers' in metadata.tables

def test_metadata_sorted_tables():
    metadata = MetaData()
    students = Table(
        'students', 
        metadata, 
        Column('id', Integer()),
        Column('name', String(is_title=True))
    )
    teachers = Table(
        'teachers',
        metadata,
        Column('id', Integer()),
        Column('name', String(is_title=True))
    )
    classes = Table(
        'classes',
        metadata,
        Column('id', Integer()),
        Column('name', String(is_title=True))
    )

    assert metadata.sorted_tables == [classes, students, teachers]

# --------------------------------------
# Create table DDL tests
#---------------------------------------

def test_create_table_not_existing(engine: Engine, students: Table):
    students.create(engine, checkfirst=True)
    entry = engine.find_table_metadata('students', table_catalog=engine._user_database_name)
    assert entry is not None

    database_obj = engine._client._get_by_id(entry.table_id)
    assert  get_object_type(database_obj) == 'database'
    assert get_object_id(database_obj) == students.get_oid()

def test_create_existing_table_no_checkfirst_raises(engine: Engine, students: Table):
    students.create(engine)

    with pytest.raises(ProgrammingError) as exc:
        students.create(engine)     # checkfirst is False by default

    assert 'students' in str(exc.value)
    assert engine._user_database_name in str(exc.value)

def test_create_existing_table_checkfirst_does_not_raise(engine: Engine, students: Table):
    students.create(engine)
    students.create(engine, checkfirst=True)     # This does nothing, create() is idempotent

def test_create_sets_oid(engine, students):
    assert students.get_oid() is None

    students.create(engine)

    oid = students.get_oid()
    assert oid is not None

    students.create(engine, checkfirst=True)
    assert students.get_oid() == oid

# --------------------------------------
# Drop table DDL tests
#---------------------------------------

def test_drop_table_uses_cached_sys_table_page_id(students: Table, engine: Engine):
    students.create(bind=engine)
    cached_page_id = students._sys_tables_page_id
    assert cached_page_id is not None    

    engine.require_table_metadata = Mock(
        wraps=engine.find_table_metadata
    )

    students.drop(bind=engine)
    engine.require_table_metadata.assert_not_called()
    assert students._sys_tables_page_id == cached_page_id

def test_drop_table_falls_back_when_sys_table_page_id_missing(students: Table, engine: Engine):
    students.create(bind=engine)

    # simulate pre-step-1 Table or reflected Table
    students._sys_tables_page_id = None    

    engine.require_table_metadata = Mock(
        wraps=engine.find_table_metadata
    )

    students.drop(bind=engine)
    engine.require_table_metadata.assert_called_once()
    assert students._sys_tables_page_id is not None

def test_drop_table_recovers_from_stale_sys_table_page_id(students: Table, engine: Engine):
    # Create normally (cache is populated correctly)
    students.create(bind=engine)

    # Inject a stale/bogus page id
    sys_tables_page_id = students._sys_tables_page_id
    stale_page_id = "deadbeef-dead-beef-dead-beefdeadbeef"
    students._sys_tables_page_id = stale_page_id

    students.drop(bind=engine)

    # Cache must now be corrected
    assert students._sys_tables_page_id != stale_page_id
    assert students._sys_tables_page_id is not None
    assert students._sys_tables_page_id == sys_tables_page_id

def test_drop_table_detects_catalog_corruption_more_than_one_table_entry(
        students: Table, 
        engine: Engine
):
    # Create normally (cache is populated correctly)
    client = engine._client
    students.create(bind=engine)

     # corrupt the catalog by adding a second page for the same table
    page_obj = client.pages_create(
        payload={
            "parent": {
                "type": "database_id",
                "database_id": engine._tables_id,
            },
            "properties": {
                "table_name": {
                    "title": [{"text": {"content": students.name}}]
                },
                "table_schema": {
                    "rich_text": [{"text": {"content": ""}}]
                },
                "table_catalog": {
                    "rich_text": [{"text": {"content": engine._user_database_name}}]
                },
                "table_id": {
                    "rich_text": [{"text": {"content": students._database_id}}]
                },
            },
        },
    )
 
    students._sys_tables_page_id = None
    with pytest.raises(InternalError, match="multiple tables named 'students' in catalog 'memory'"):
        students.drop(bind=engine)

def test_drop_table_detects_catalog_corruption_no_table_entry_found(
    students: Table, 
    engine: Engine
):
    # Create normally (cache is populated correctly)
    client = engine._client
    students.create(bind=engine)

    # corrupt the catalog by removing the page for the table just created
    client._store.pop(students._sys_tables_page_id)

    with pytest.raises(InternalError, match="'students' is orphaned"):
        students.drop(bind=engine)

def test_drop_non_existing_table_raises(engine: Engine, students: Table):
    with pytest.raises(CompileError) as exc:
        students.drop(engine)

    assert 'students' in str(exc.value)
    assert 'neither created or reflected' in str(exc.value)

def test_drop_already_dropped_table_raises(engine: Engine, students: Table):
    students.create(engine)
    students.drop(engine)

    with pytest.raises(ProgrammingError) as exc:
        students.drop(engine)

    assert 'already dropped' in str(exc.value)
    assert 'students'in str(exc.value)

def test_drop_already_dropped_table_checkfirst_does_not_raise(engine: Engine, students: Table):
    students.create(engine)
    students.drop(engine)

    syscat = engine._catalog
    state = syscat.get_table_state(
        students.name,
        table_catalog=engine._user_database_name,
    )

    assert state is TableState.DROPPED

def test_create_after_drop_restores_when_option_enabled(engine: Engine, students: Table):
    students.create(engine)
    students.drop(engine)

    restore_engine = engine.execution_options(restore_dropped=True)
    students.create(restore_engine)

    state = restore_engine._catalog.get_table_state(
        students.name,
        table_catalog=restore_engine._user_database_name,
    )

    assert state is TableState.ACTIVE

def test_create_after_drop_without_restore_raises(engine, students):
    students.create(engine)
    students.drop(engine)

    with pytest.raises(ProgrammingError, match="restore_dropped=True"):
        students.create(engine)
        
@pytest.mark.skip('Table repair not supported yet.')
def test_create_when_orphaned_repairs_or_recreates(engine: Engine, students: Table):
    students.create(engine)

    entry = engine.find_table_metadata(
        students.name,
        table_catalog=engine._user_database_name,
    )

    # simulate physical deletion of database
    engine._client._store.pop(entry.table_id)

    state = engine._catalog.get_table_state(
        students.name,
        table_catalog=engine._user_database_name,
    )

    assert state is TableState.ORPHANED

    students.create(engine)

    new_state = engine._catalog.get_table_state(
        students.name,
        table_catalog=engine._user_database_name,
    )

    assert state is TableState.ACTIVE

def test_create_when_orphaned_raises(engine: Engine, students: Table):
    students.create(engine)

    entry = engine.find_table_metadata(
        students.name,
        table_catalog=engine._user_database_name,
    )

    # simulate physical deletion of database
    engine._client._store.pop(entry.table_id)

    state = engine._catalog.get_table_state(
        students.name,
        table_catalog=engine._user_database_name,
    )

    assert state is TableState.ORPHANED

    with pytest.raises(InternalError, match="'students' is orphaned."):
        students.create(engine)

def test_create_checkfirst_on_dropped_table_does_not_restore(engine: Engine, students: Table):
    students.create(engine)
    students.drop(engine)

    with pytest.raises(ProgrammingError, match="'students' is dropped."):
        students.create(engine)

# --------------------------------------
# Reflection tests
#---------------------------------------

def test_table_autoload_active(engine: Engine, metadata: MetaData):
    create_students_db(engine)
    students = Table('students', metadata, autoload_with=engine)
    columns = [c.name for c in students.columns]
    pk_cols = [c.name for c in students.primary_key.c]
    assert includes_all(columns, ['id', 'name', 'grade', 'is_active'])
    assert '_no_id' in columns
    assert '_no_archived' in columns
    assert len(pk_cols) == 1
    assert SpecialColumns.NO_ID.value in pk_cols

def test_table_autoload_missing_raises(engine: Engine, metadata: MetaData):
    with pytest.raises(NoSuchTableError) as exc:
        students = Table('students', metadata, autoload_with=engine)

    assert 'students' in str(exc.value)
    assert 'does not exist' in str(exc.value)
    assert 'memory' in str(exc.value)

def test_table_autoload_dropped_raises(engine: Engine, students: Table, metadata: MetaData):
    students.create(engine)
    students.drop(engine)

    with pytest.raises(ProgrammingError, match="'students' is dropped"):
        _ = Table('students', metadata, autoload_with=engine)

def test_table_autoload_orphaned_raises(    
    metadata: MetaData,
    students: Table, 
    engine: Engine
):
    # Create normally (cache is populated correctly)
    client = engine._client
    students.create(bind=engine)

    # corrupt the catalog by removing the page for the table just created
    client._store.pop(students._sys_tables_page_id)

    with pytest.raises(InternalError, match="'students' is orphaned"):
        _ = Table('students', metadata, autoload_with=engine)
       
@pytest.mark.skip('MetaData reflection not supported in this version')
def test_metadata_reflect(engine: Engine, metadata: MetaData):
    create_students_db(engine)
    metadata = MetaData()
    students = Table('students', metadata)
    metadata.reflect(engine)
    columns = [c.name for c in students.columns]
    assert includes_all(columns, ['student_id', 'name', 'grade', 'is_active'])
    assert SpecialColumns.NO_ID in columns
    assert SpecialColumns.NO_ARCHIVED in columns

