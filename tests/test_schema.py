from __future__ import annotations
import pdb
from typing import Iterable
import pytest

from normlite import (
    DuplicateColumnError,
    Column, ColumnCollection, PrimaryKeyConstraint, Table,
    ArchivalFlag, Date, Integer, ObjectId, String
)
from normlite.engine import Engine, create_engine
from normlite.sql.schema import MetaData

@pytest.fixture
def sc() -> ColumnCollection:
    return ColumnCollection([
        ('student_id', Column('student_id', Integer(), primary_key=True)),
        ('name', Column('name', String(is_title=True))),
        ('grade', Column('grade', String())),
        ('since', Column('since', Date()))
    ])

def create_students_db(engine: Engine) -> None:
    # create a new table students in memory
    db = engine._client._add('database', {
        'parent': {
            'type': 'page_id',
            'page_id': engine._db_page_id
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

def test_columncollection_getattr(sc: ColumnCollection):
    assert isinstance(sc.since, Column)
    assert not sc.since.primary_key

def test_columncollection_getitem(sc: ColumnCollection):
    assert isinstance(sc['student_id'], Column)
    assert isinstance(sc[0], Column)
    assert sc['student_id'].primary_key
    assert sc[0].primary_key
    assert len(sc[0:3]) == 3

def test_columncollection_contains(sc: ColumnCollection):
    assert 'student_id' in sc
    assert not '_no_id' in sc

def test_columncollection_getitem_wrong_index(sc: ColumnCollection):
    index = len(sc)
    with pytest.raises(IndexError, match=f'{index}'):
        col = sc[index]

def test_columncollection_add(sc: ColumnCollection):
    sc.add(Column("_no_id", ObjectId(), primary_key=True))
    sc.add(Column("_no_archived", ArchivalFlag()))
    assert len(sc) == 6
    assert isinstance(sc._no_id, Column)
    assert isinstance(sc._no_archived, Column)

def test_columncollection_getitem_wrong_key(sc: ColumnCollection):
    key = 'does_not_exist'
    with pytest.raises(KeyError, match=f'{key}'):
        col = sc[key]

def test_columncollection_asreadonly(sc: ColumnCollection):
    ro_sc = sc.as_readonly()
    with pytest.raises(TypeError, match='object is immutable and/or readonly.'):
        #pdb.set_trace()
        del ro_sc['student_id']

def test_columncollection_no_duplicates(sc: ColumnCollection):
    dup_col = 'student_id'
    with pytest.raises(DuplicateColumnError, match=f'not allow duplicate columns: {dup_col}'):
        sc.add(Column('student_id', String()))
        
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
    assert students.c._no_id == students.columns._no_id
    assert students.c._no_archived == students.columns._no_archived

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
    assert students.c._no_id == primary_key.c._no_id
    assert students.c.student_id == primary_key.c.student_id

def includes_all(l: list[str], values: Iterable[str]) -> bool:
    for v in values:
        if v not in l:
            return False
    return True

def test_table_autoload():
    engine = create_engine(
        'normlite:///:memory:',
        _mock_ws_id = '12345678-0000-0000-1111-123456789012',
        _mock_ischema_page_id = 'abababab-3333-3333-3333-abcdefghilmn',
        _mock_tables_id = '66666666-6666-6666-6666-666666666666',
        _mock_db_page_id = '12345678-9090-0606-1111-123456789012'
    )
    create_students_db(engine)
    metadata = MetaData()
    students = Table('students', metadata, autoload_with=engine)
    columns = [c.name for c in students.columns]
    assert includes_all(columns, ['student_id', 'name', 'grade', 'is_active'])
    assert '_no_id' in columns
    assert '_no_archived' in columns

def test_metadata_contains():
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

def test_metadata_reflect():
    engine = create_engine(
        'normlite:///:memory:',
        _mock_ws_id = '12345678-0000-0000-1111-123456789012',
        _mock_ischema_page_id = 'abababab-3333-3333-3333-abcdefghilmn',
        _mock_tables_id = '66666666-6666-6666-6666-666666666666',
        _mock_db_page_id = '12345678-9090-0606-1111-123456789012'
    )
    create_students_db(engine)
    metadata = MetaData()
    students = Table('students', metadata)
    metadata.reflect(engine)
    columns = [c.name for c in students.columns]
    assert includes_all(columns, ['student_id', 'name', 'grade', 'is_active'])
    assert '_no_id' in columns
    assert '_no_archived' in columns

