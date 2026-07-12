from __future__ import annotations
import pdb
from typing import Iterable
from unittest.mock import Mock, patch
import pytest

from normlite import (
    Engine, create_engine,
    Column, PrimaryKeyConstraint, Table, ForeignKey,
    Date, Integer, String, Boolean,
    ArgumentError, CompileError, NoSuchTableError, DuplicateColumnError
)
from normlite._constants import SpecialColumns
from normlite.engine.systemcatalog import TableState
from normlite.exceptions import InvalidRequestError, NoReferencedTableError
from normlite.notion_sdk.getters import get_object_id, get_object_type
from normlite.notiondbapi.dbapi2 import InternalError, ProgrammingError
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode
from normlite.sql.schema import MetaData, SystemColumn
from normlite.sql.type_api import ObjectId, ArchivalFlag

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

    # add the students to tables
    engine._client._add('page', {
        'parent': {
            'type': 'data_source_id',
            'data_source_id': engine._catalog._tables_dsid
        },
        'properties': {
            'table_name': {'title': [{'text': {'content': 'students'}}]},
            'table_schema': {'rich_text': [{'text': {'content': ''}}]},
            'table_catalog': {'rich_text': [{'text': {'content': 'memory'}}]},
            'table_id': {'rich_text': [{'text': {'content': db.get('id')}}]},
            'table_dsid': {'rich_text': [{'text': {'content': db['data_sources'][0]['id']}}]},
            'is_dropped': {'checkbox': False},
            'created_time': {'rich_text': [{'text': {'content': db['created_time']}}]}
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
    col_slice = sc[0:3]
    col_slice_names = [c.name for c in col_slice]
 
    assert isinstance(sc['id'], Column)
    assert isinstance(sc[0], Column)
    assert not sc['id'].primary_key
    assert sc[0].primary_key
    assert len(col_slice) == 3
    assert col_slice_names == ["object_id", "is_archived", "is_deleted"]

def test_columncollection_contains(students: Table):
    uc = students.user_columns
 
    assert 'id' in uc
    assert not 'object_id' in uc

def test_columncollection_truthness(metadata: MetaData):
    table = Table("no_col_table", metadata)

    assert not bool(table.uc)
    assert len(table.uc) == 0
    assert not bool(table.user_columns)
    assert len(table.user_columns) == 0
    assert bool(table._sys_columns)     # system columns are always present
    assert len(table._sys_columns) > 0

def test_columncollection_getitem_wrong_index(students: Table):
    sc = students.columns
    index = len(sc)
    with pytest.raises(IndexError, match=f'{index}'):
        col = sc[index]

def test_columncollection_add(students: Table):
    sc = students.columns
    fake_id = Column("fake_id", ObjectId())
    fake_id._set_parent(students)
    fake_archived = Column("fake_archived", ArchivalFlag())
    fake_archived._set_parent(students)
    sc.add(fake_id)
    sc.add(fake_archived)

    assert len(sc) == 7 + 4
    assert isinstance(sc.fake_id, Column)
    assert isinstance(sc.fake_archived, Column)

def test_columncollection_getitem_wrong_key(students: Table):
    sc = students.columns
    key = 'does_not_exist'
    with pytest.raises(KeyError, match=f'{key}'):
        col = sc[key]

def test_usr_columncollection_asreadonly(students: Table):
    sc = students.columns
    ro_sc = sc.as_readonly()
    with pytest.raises(TypeError, match='object is immutable and/or readonly.'):
        del ro_sc['id']

def test_sys_columncollection_asreadonly(students: Table):
    sc = students._sys_columns
    ro_sc = sc.as_readonly()
    with pytest.raises(TypeError, match='object is immutable and/or readonly.'):
        del ro_sc['created_at']

def test_usr_columncollection_no_duplicates(students: Table):
    sc = students.columns 
    dup_col = 'id'
    with pytest.raises(DuplicateColumnError, match=f'not allow duplicate columns: {dup_col}'):
        sc.add(Column('id', String()))

def test_sys_columncollection_no_duplicates(students: Table):
    sc = students._sys_columns 
    dup_col = 'is_archived'
    with pytest.raises(DuplicateColumnError, match=f'not allow duplicate columns: {dup_col}'):
        sc.add(Column('is_archived', String()))

# --------------------------------------
# Table tests
#---------------------------------------        

def test_unreflected_columns_returns_none_sys_column_values(students: Table):
    assert students.get_oid() is None
    assert students.created_at is None

def test_user_columns_returns_user_def_only(students: Table):
    assert all([not col.is_system for col in students.user_columns])

def test_all_sys_columns_have_parent(students: Table):
    assert all([sc.parent is students for sc in students._sys_columns])

def test_table_contains_sys_columns_if_no_usr_cols_defined(metadata: MetaData):
    no_usr_col_table = Table("no_usr_cols", metadata)

    assert "object_id" in no_usr_col_table._sys_columns
    assert "is_deleted" in no_usr_col_table._sys_columns
    assert "is_archived" in no_usr_col_table._sys_columns
    assert "created_at" in no_usr_col_table._sys_columns
    assert "table_name" not in no_usr_col_table._sys_columns

def test_sys_columns_api_names_correctly_mapped(metadata: MetaData):
    table = Table("no_usr_cols", metadata)

    assert table._sys_columns.object_id.api_key() == "id"
    assert table._sys_columns.is_deleted.api_key() == "in_trash"
    assert table._sys_columns.is_archived.api_key() == "archived"
    assert table._sys_columns.created_at.api_key() == "created_time"

def test_sys_columns_cannot_be_redefined(metadata: MetaData):
    with pytest.raises(ArgumentError) as exc:
        table = Table(
            "table",
            metadata,
            SystemColumn("object_id", String(is_title=True))
        )

    assert "object_id" in str(exc.value) 

def test_usr_columns_cannot_be_pk(metadata: MetaData):
    with pytest.raises(ArgumentError) as exc:
        table = Table(
            "students",
            metadata,
            Column("name", String(is_title=True), primary_key=True)
        )

    assert "name" in str(exc.value)

def test_all_columns_contains_them_all(students: Table):
    all_cols = [col.name for col in students.columns]

    assert all_cols == [
        "object_id",
        "is_archived",
        "is_deleted",
        "created_at",
        "name",
        "id",
        "is_active",
        "start_on",
        "grade"
    ]

def test_data_source_id_is_hidden_from_public_columns(students: Table):
    # data_source_id is a hidden system column (ADR-0017): captured in _sys_columns
    # for routing/value capture but excluded from the public table.c surface, the
    # same rule table_name/NO_TITLE already follows. It must never appear in .c.
    assert "data_source_id" not in students.c
    assert "data_source_id" not in [c.name for c in students.columns]

    # ...yet it is still captured internally for get_data_source_id()
    assert "data_source_id" in [c.name for c in students._sys_columns]

def test_table_construct():
    metadata = MetaData()
    students = Table(
        'students',
        metadata,
        Column('student_id', Integer()),
        Column('name', String(is_title=True)),
        Column('grade', String()),
        Column('since', Date())
    )
    assert students._sys_columns.object_id.primary_key
    assert not students.columns.student_id.primary_key
    assert isinstance(students.c['name'].type_, String)
    assert isinstance(students.c['since'].type_, Date)
    assert students.created_at is None

def test_table_repr_contains_sys_columns(students: Table):
    table_repr = repr(students)
    
    assert "object_id" in table_repr
    assert "is_deleted" in table_repr
    assert "is_archived" in table_repr
    assert "created_at" in table_repr
    assert "table_name" not in table_repr

def test_table_valid_minimal():
    metadata = MetaData()
    t = Table("students", metadata, Column('title', String(is_title=True)))

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

def test_table_title_column_missing():
    metadata = MetaData()

    with pytest.raises(ArgumentError) as exc:
        Table('students',
              metadata,
              Column('grade', String())
        )

    assert 'column of type String(is_title=True)' in str(exc.value)

def test_table_primary_key():
    metadata = MetaData()
    students = Table(
        'students',
        metadata,
        Column('student_id', Integer()),
        Column('name', String(is_title=True)),
        Column('grade', String()),
        Column('since', Date())
    )

    primary_key = students.primary_key
    assert students.primary_key.table == students
    assert isinstance(primary_key, PrimaryKeyConstraint)
    assert 'object_id' in primary_key.c
    # data_source_id is a captured system column but table-constant routing
    # plumbing (ADR-0014), not row identity — object_id alone is the PK.
    assert 'data_source_id' not in primary_key.c
    assert 'id' not in primary_key.c
    assert not 'name' in primary_key.columns

def test_invalid_table_name_empty(metadata: MetaData):
    good = Table('students', metadata, Column('name', String(is_title=True)))

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
    assert students.get_oid() is None
    assert students.created_at is None
    assert engine._catalog is not None

    # act
    students.create(bind=engine)

    # postcondition
    assert students._sys_tables_page_id is not None
    assert isinstance(students._sys_tables_page_id, str)
    assert students.get_oid() is not None
    assert students.created_at is None

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
                "type": "data_source_id",
                "data_source_id": engine._catalog._tables_dsid,
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
                    "rich_text": [{"text": {"content": students.get_oid()}}]
                },
                "table_dsid": {
                    "rich_text": [{"text": {"content": students.get_data_source_id()}}]
                },
                "is_dropped": {"checkbox": False},
                "created_time": {
                    "rich_text": [{"text": {"content": "2025-09-03T00:00:00.000Z"}}]
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
    assert not 'object_id' in students.uc
    assert not 'is_archived' in students.uc
    assert len(pk_cols) == 1
    assert "object_id" in pk_cols

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

# ---------------------------------------------------
# ForeignKey
# ---------------------------------------------------

def test_foreignkey_parses_table_and_column_name():
    fk = ForeignKey("students.object_id")
    assert fk.table_name == "students"
    assert fk.column_name == "object_id"

def test_foreignkey_database_id_initially_none():
    fk = ForeignKey("students.object_id")
    assert fk.data_source_id is None

def test_column_accepts_foreignkey_in_args():
    from normlite import Relation
    fk = ForeignKey("students.object_id")
    col = Column("students_oid", Relation(), fk)
    assert fk in col.foreign_keys

def test_column_rejects_unknown_positional_arg():
    from normlite import Relation
    with pytest.raises(ArgumentError, match="ForeignKey"):
        Column("students_oid", Relation(), "not-a-fk")

def test_table_autowires_foreignkey_constraint_on_create(engine: Engine):
    from normlite import Relation

    # Arrange
    metadata = MetaData()
    courses = Table("courses", metadata, Column("title", String(is_title=True)))
    courses.create(engine)

    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )

    # Act — no add_constraint, no ForeignKeyConstraint in sight
    students.create(engine)

    # Assert — the relation spec lives on the data source, targeting data_source_id (2025-09-03)
    db_obj = engine._client._get_by_id(students.get_oid())
    ds = engine._client.data_sources_retrieve(
        path_params={"data_source_id": db_obj["data_sources"][0]["id"]}
    )
    assert ds["properties"]["enrolled_in"]["relation"]["data_source_id"] == courses.get_data_source_id()

def test_table_rejects_relation_column_without_foreignkey():
    from normlite import Relation

    metadata = MetaData()

    with pytest.raises(ArgumentError, match="enrolled_in"):
        Table(
            "students",
            metadata,
            Column("name", String(is_title=True)),
            Column("enrolled_in", Relation()),   # no ForeignKey
        )

def test_foreignkey_column_resolves_to_referenced_column():
    from normlite import Relation

    metadata = MetaData()
    courses = Table("courses", metadata, Column("title", String(is_title=True)))
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )

    fk = next(iter(students.c.enrolled_in.foreign_keys))
    assert fk.column is courses._sys_columns["object_id"]

def test_table_foreign_keys_exposes_autowired_constraints():
    from normlite import Relation
    from normlite.sql.schema import ForeignKeyConstraint

    metadata = MetaData()
    courses = Table(
        "courses",
        metadata,
        Column("title", String(is_title=True)),
    )
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )

    fks = students.foreign_keys

    assert len(fks) == 1
    constraint = next(iter(fks))
    assert isinstance(constraint, ForeignKeyConstraint)
    assert constraint.column is students.c.enrolled_in

def test_sorted_tables_orders_by_fk_dependency():
    from normlite import Relation

    metadata = MetaData()
    products = Table(
        "products",
        metadata,
        Column("name", String(is_title=True)),
    )
    orders = Table(
        "orders",
        metadata,
        Column("title", String(is_title=True)),
        Column("product", Relation(), ForeignKey("products.object_id")),
    )

    assert metadata.sorted_tables == [products, orders]

def test_sorted_tables_raises_on_circular_dependency():
    from normlite import Relation, CircularDependencyError
    from normlite.sql.schema import ForeignKeyConstraint

    metadata = MetaData()
    a = Table(
        "a",
        metadata,
        Column("title", String(is_title=True)),
    )
    b = Table(
        "b",
        metadata,
        Column("title", String(is_title=True)),
        Column("a_ref", Relation(), ForeignKey("a.object_id")),
    )

    # Inject the reverse edge a → b directly via add_constraint.
    # The construction-time guard in _create_fk_constraints prevents
    # declaring this cycle through the public Column/ForeignKey path.
    b_ref = Column("b_ref", Relation(), ForeignKey("b.object_id"))
    b_ref._set_parent(a)
    a.add_constraint(ForeignKeyConstraint(b_ref, b))

    with pytest.raises(CircularDependencyError):
        metadata.sorted_tables

def test_sorted_tables_orders_three_table_chain():
    from normlite import Relation

    metadata = MetaData()
    a = Table(
        "z_root",
        metadata,
        Column("title", String(is_title=True)),
    )
    b = Table(
        "m_middle",
        metadata,
        Column("title", String(is_title=True)),
        Column("a_ref", Relation(), ForeignKey("z_root.object_id")),
    )
    c = Table(
        "a_leaf",
        metadata,
        Column("title", String(is_title=True)),
        Column("b_ref", Relation(), ForeignKey("m_middle.object_id")),
    )

    assert metadata.sorted_tables == [a, b, c]

def test_sorted_tables_orders_independent_and_dependent_tables():
    from normlite import Relation

    metadata = MetaData()
    standalone = Table(
        "standalone",
        metadata,
        Column("title", String(is_title=True)),
    )
    parent = Table(
        "parent",
        metadata,
        Column("title", String(is_title=True)),
    )
    child = Table(
        "child",
        metadata,
        Column("title", String(is_title=True)),
        Column("parent_ref", Relation(), ForeignKey("parent.object_id")),
    )

    result = metadata.sorted_tables

    # parent must appear before child; standalone may appear anywhere
    # but ordering must be deterministic (alphabetical tie-break among ready)
    assert result.index(parent) < result.index(child)
    assert set(result) == {parent, standalone, child}
    # Determinism: parent and standalone are both ready on pass 1;
    # alphabetical tie-break puts parent first
    assert result == [parent, standalone, child]

def test_create_table_with_unresolved_fk_raises_no_referenced_table_error(engine: Engine):
    from normlite import Relation

    metadata = MetaData()
    courses = Table(
        "courses",
        metadata,
        Column("title", String(is_title=True)),
    )
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )

    # courses has NOT been created — its oid is still None,
    # so the FK target cannot be resolved at compile time
    with pytest.raises(NoReferencedTableError, match="create_all"):
        students.create(engine)

def test_create_all_creates_tables_with_fk_dependencies(engine: Engine):
    from normlite import Relation

    metadata = MetaData()
    courses = Table(
        "courses",
        metadata,
        Column("title", String(is_title=True)),
    )
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )

    metadata.create_all(engine)

    assert courses.get_oid() is not None
    assert students.get_oid() is not None

    # Parent must be created strictly before child
    courses_obj = engine._client._get_by_id(courses.get_oid())
    students_obj = engine._client._get_by_id(students.get_oid())
    assert courses_obj["created_time"] < students_obj["created_time"]

def test_create_all_with_diamond_fk_schema(engine: Engine):
    from normlite import Relation

    metadata = MetaData()

    projects = Table(
        "projects",
        metadata,
        Column("name", String(is_title=True)),
    )
    sprints = Table(
        "sprints",
        metadata,
        Column("name", String(is_title=True)),
    )
    devs = Table(
        "devs",
        metadata,
        Column("name", String(is_title=True)),
        Column("project", Relation(), ForeignKey("projects.object_id")),
    )
    tasks = Table(
        "tasks",
        metadata,
        Column("title", String(is_title=True)),
        Column("project", Relation(), ForeignKey("projects.object_id")),
        Column("sprint", Relation(), ForeignKey("sprints.object_id")),
    )

    # Topological order: parents first, alphabetical tie-break among ready
    sorted_names = [t.name for t in metadata.sorted_tables]
    assert sorted_names == ["projects", "sprints", "devs", "tasks"]

    metadata.create_all(engine)

    # All tables physically created
    for table in (projects, sprints, devs, tasks):
        assert table.get_oid() is not None

    # DDL payload: every Relation column resolved to the right data_source_id
    # (2025-09-03: relation spec lives on the data source, not the container)
    def ds_props(table):
        db_obj = engine._client._get_by_id(table.get_oid())
        ds = engine._client.data_sources_retrieve(
            path_params={"data_source_id": db_obj["data_sources"][0]["id"]}
        )
        return ds["properties"]

    tasks_props = ds_props(tasks)
    assert tasks_props["project"]["relation"]["data_source_id"] == projects.get_data_source_id()
    assert tasks_props["sprint"]["relation"]["data_source_id"] == sprints.get_data_source_id()

    devs_props = ds_props(devs)
    assert devs_props["project"]["relation"]["data_source_id"] == projects.get_data_source_id()

    # Verify creation sequence: parents strictly before any child that depends on them
    objs = {t.name: engine._client._get_by_id(t.get_oid()) for t in (projects, sprints, devs, tasks)}
    assert objs["projects"]["created_time"] < objs["devs"]["created_time"]
    assert objs["projects"]["created_time"] < objs["tasks"]["created_time"]
    assert objs["sprints"]["created_time"] < objs["tasks"]["created_time"]

def test_create_all_is_idempotent(engine: Engine):
    from normlite import Relation

    metadata = MetaData()
    courses = Table(
        "courses",
        metadata,
        Column("title", String(is_title=True)),
    )
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )

    metadata.create_all(engine)
    courses_oid = courses.get_oid()
    students_oid = students.get_oid()

    # Second call must not raise and must not re-create
    metadata.create_all(engine)

    assert courses.get_oid() == courses_oid
    assert students.get_oid() == students_oid

def test_autoload_reflects_relation_with_resolved_data_source_id(engine: Engine):
    from normlite import Relation

    # Setup: declare and create courses + students with a Relation FK
    setup_meta = MetaData()
    courses = Table(
        "courses",
        setup_meta,
        Column("title", String(is_title=True)),
    )
    students = Table(
        "students",
        setup_meta,
        Column("name", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )
    setup_meta.create_all(engine)
    courses_dsid = courses.get_data_source_id()

    # Reflect students into a fresh, independent MetaData
    fresh_meta = MetaData()
    reflected = Table("students", fresh_meta, autoload_with=engine)

    enrolled_in = reflected.c.enrolled_in
    assert isinstance(enrolled_in.type_, Relation)

    fks = enrolled_in.foreign_keys
    assert len(fks) == 1
    fk = next(iter(fks))
    assert fk.table_name == "courses"
    assert fk.column_name == "object_id"
    # ADR-0014: relations retarget to the data source id, not the database uuid.
    assert fk.data_source_id == courses_dsid

def test_autoload_warns_when_relation_target_not_in_catalog(engine: Engine):
    client = engine._client
    unknown_target_dsid = "deadbeef-1234-5678-9abc-deadbeefcafe"

    # A Notion database whose relation property points to a data source
    # that exists in Notion but is NOT registered in our catalog
    db = client._add('database', {
        'parent': {'type': 'page_id', 'page_id': engine._user_tables_page_id},
        'title': [{
            'type': 'text',
            'text': {'content': 'students', 'link': None},
            'plain_text': 'students',
            'href': None,
        }],
        'initial_data_source': {
            'properties': {
                'name': {'title': {}},
                'enrolled_in': {
                    'relation': {
                        'data_source_id': unknown_target_dsid,
                        'single_property': {},
                    }
                },
            },
        },
    })

    # Register only the students DB in the catalog so reflection can find it
    client._add('page', {
        'parent': {'type': 'data_source_id', 'data_source_id': engine._catalog._tables_dsid},
        'properties': {
            'table_name': {'title': [{'text': {'content': 'students'}}]},
            'table_schema': {'rich_text': [{'text': {'content': ''}}]},
            'table_catalog': {'rich_text': [{'text': {'content': 'memory'}}]},
            'table_id': {'rich_text': [{'text': {'content': db.get('id')}}]},
            'table_dsid': {'rich_text': [{'text': {'content': db['data_sources'][0]['id']}}]},
            'is_dropped': {'checkbox': False},
            'created_time': {'rich_text': [{'text': {'content': db['created_time']}}]},
        },
    })

    metadata = MetaData()
    with pytest.warns(UserWarning, match="enrolled_in"):
        reflected = Table('students', metadata, autoload_with=engine)

    # The unresolvable relation column must not be present
    assert 'enrolled_in' not in reflected.c
    # But the rest of the schema reflects normally
    assert 'name' in reflected.c

def test_autoload_reflects_mixed_resolvable_and_unresolvable_relations(engine: Engine):
    # Resolvable target: created via the normal path so it lands in the catalog
    setup_meta = MetaData()
    courses = Table("courses", setup_meta, Column("title", String(is_title=True)))
    courses.create(engine)
    courses_dsid = courses.get_data_source_id()

    # Build a students DB with two relation properties:
    # - enrolled_in → courses (resolvable)
    # - favorite_topic → unknown uuid (NOT in catalog)
    client = engine._client
    unknown_target_dsid = "deadbeef-9999-9999-9999-deadbeef9999"
    students_db = client._add('database', {
        'parent': {'type': 'page_id', 'page_id': engine._user_tables_page_id},
        'title': [{
            'type': 'text',
            'text': {'content': 'students', 'link': None},
            'plain_text': 'students',
            'href': None,
        }],
        'initial_data_source': {
            'properties': {
                'name': {'title': {}},
                'enrolled_in': {
                    'relation': {
                        'data_source_id': courses_dsid,
                        'single_property': {},
                    }
                },
                'favorite_topic': {
                    'relation': {
                        'data_source_id': unknown_target_dsid,
                        'single_property': {},
                    }
                },
            },
        },
    })

    # Register students in the catalog
    client._add('page', {
        'parent': {'type': 'data_source_id', 'data_source_id': engine._catalog._tables_dsid},
        'properties': {
            'table_name': {'title': [{'text': {'content': 'students'}}]},
            'table_schema': {'rich_text': [{'text': {'content': ''}}]},
            'table_catalog': {'rich_text': [{'text': {'content': 'memory'}}]},
            'table_id': {'rich_text': [{'text': {'content': students_db.get('id')}}]},
            'table_dsid': {'rich_text': [{'text': {'content': students_db['data_sources'][0]['id']}}]},
            'is_dropped': {'checkbox': False},
            'created_time': {'rich_text': [{'text': {'content': students_db['created_time']}}]},
        },
    })

    # Reflect: only the unresolvable column triggers a warning
    fresh_meta = MetaData()
    with pytest.warns(UserWarning, match="favorite_topic"):
        reflected = Table('students', fresh_meta, autoload_with=engine)

    # Resolvable relation column reflected with FK populated
    assert 'enrolled_in' in reflected.c
    fks = reflected.c.enrolled_in.foreign_keys
    assert len(fks) == 1
    fk = next(iter(fks))
    assert fk.table_name == "courses"
    # ADR-0014: relations retarget to the data source id, not the database uuid.
    assert fk.data_source_id == courses_dsid

    # Unresolvable column skipped, normal columns intact
    assert 'favorite_topic' not in reflected.c
    assert 'name' in reflected.c


def test_reflected_relation_fk_carries_data_source_id(engine: Engine):
    # Seam C of the ADR-0014 relation retarget (C1 + C2). Seams A and B already land
    # a data_source_id on the reflected relation column and resolve the catalog row by
    # `table_dsid`. The FK assembly in ReflectTable._finalize_execution (sql/ddl.py) must
    # now surface that dsid on the *renamed* attribute `ForeignKey.data_source_id`
    # (ADR-0014: `ForeignKey.database_id` → `data_source_id`), not the legacy
    # `database_id` slot.
    #
    # Hand-built at the FK-assembly seam: rather than stand up a full live relation
    # fixture, we drive `_finalize_execution` directly and hand-feed its two phases —
    # phase 1 (system rows, carrying the data source id in the NO_DSID row) and phase 2
    # (the reflected relation user column). This isolates FK assembly against a real,
    # catalog-seeded engine while stubbing the phase-2 `data_sources.retrieve` round-trip
    # that STEP 3 of the 2-phase reflection slice added to `_finalize_execution`.
    from normlite.engine.resultmetadata import CursorResultMetaData
    from normlite.engine.row import Row
    from normlite.sql.ddl import ReflectTable

    courses_dsid = "deadbeef-dead-beef-dead-beefdeadbeef-ds"

    # Seed the catalog row for the relation target, keyed by its data_source_id.
    engine._client.pages_create(payload={
        "parent": {"type": "data_source_id", "data_source_id": engine._catalog._tables_dsid},
        "properties": {
            "table_name": {"title": [{"text": {"content": "courses"}}]},
            "table_schema": {"rich_text": [{"text": {"content": "public"}}]},
            "table_catalog": {"rich_text": [{"text": {"content": "memory"}}]},
            "table_id": {"rich_text": [{"text": {"content": "courses-db-0001"}}]},
            "table_dsid": {"rich_text": [{"text": {"content": courses_dsid}}]},
            "is_dropped": {"checkbox": False},
            "created_time": {"rich_text": [{"text": {"content": "2025-09-03T00:00:00.000Z"}}]},
        },
    })

    # A reflected relation DDL row whose on-wire target rides in `relation.data_source_id`
    # (Seam A). `_process_ddl_row` extracts the dsid into the row's value slot.
    ddl_description = (
        ("column_name", "string", None, None, None, None, None),
        ("column_type", "string", None, None, None, None, None),
        ("column_id",   "string", None, None, None, None, None),
        ("metadata",    "object", None, None, None, None, None),
        ("is_system",   "bool",   None, None, None, None, None),
    )
    relation_row = Row(
        CursorResultMetaData(ddl_description, is_ddl=True),
        (
            "enrolled_in",
            "relation",
            "col-enrolled",
            {"relation": {"data_source_id": courses_dsid, "single_property": {}}},
            False,  # user column
        ),
    )

    # Phase-1 system row carrying this table's data source id — `_finalize_execution`
    # reads it (NO_DSID) to issue the 2nd-phase retrieve. Value is arbitrary here since
    # the phase-2 round-trip is stubbed and the user rows are hand-fed below.
    students_dsid = "caffe000-0000-0000-0000-0000000000ff"
    dsid_row = Row(
        CursorResultMetaData(ddl_description, is_ddl=True),
        (
            SpecialColumns.NO_DSID,
            DBAPITypeCode.ID,
            None,
            students_dsid,
            True,  # system column
        ),
    )

    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def all(self):
            return self._rows

        def close(self):
            pass

    class _FakeRawConnection:
        def cursor(self):
            return None

    class _FakeEngine:
        # Delegate to the real engine (catalog lookups for FK resolution stay real),
        # but stub the phase-2 DBAPI round-trip that `_finalize_execution` now issues.
        def __init__(self, real):
            self._real = real

        def __getattr__(self, name):
            return getattr(self._real, name)

        def raw_connection(self):
            return _FakeRawConnection()

        def do_execute(self, cursor, operation, parameters):
            pass

    class _FakeContext:
        def __init__(self, engine, sys_rows, user_rows):
            self.engine = _FakeEngine(engine)
            self._sys_rows = sys_rows
            self._user_rows = user_rows
            self._result_cursor = None

        def setup_cursor_result(self, clear_buffered=False):
            return _FakeResult(self._user_rows if clear_buffered else self._sys_rows)

    reflected = Table("students", MetaData())
    stmt = ReflectTable(reflected)
    stmt._finalize_execution(_FakeContext(engine, [dsid_row], [relation_row]))

    fks = list(reflected.c.enrolled_in.foreign_keys)
    assert len(fks) == 1
    fk = fks[0]
    assert fk.table_name == "courses"
    assert fk.data_source_id == courses_dsid

def test_autoload_merges_scalar_user_columns_from_data_source(engine: Engine):
    # STEP 3 of the 2-phase reflection slice (ADR-0014). Under 2025-09-03 the user
    # columns live on the *data source*, not the database container, so
    # `databases.retrieve` (→ ResultSet._process_database) reflects SYSTEM ROWS ONLY.
    # ReflectTable._finalize_execution must make a 2nd-phase
    # `data_sources.retrieve(data_source_id)` call (dsid read from the just-reflected
    # NO_DSID system row), run ResultSet._process_data_source, and MERGE the user
    # columns after the system rows. Scalar-only here — relation-value → dsid
    # extraction is STEP 3b.
    setup_meta = MetaData()
    students = Table(
        "students",
        setup_meta,
        Column("name", String(is_title=True)),
        Column("age", Integer()),
    )
    setup_meta.create_all(engine)

    # Reflect into a fresh, independent MetaData via the LIVE autoload path.
    fresh_meta = MetaData()
    reflected = Table("students", fresh_meta, autoload_with=engine)

    # The scalar user column defined on the data source must be merged in.
    assert "age" in reflected.c
    age = reflected.c.age
    assert isinstance(age.type_, Integer)   # raw "number" → Integer via type_mapper
    assert age.get_oid() is not None        # non-empty property id from data_sources.retrieve
