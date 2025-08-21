from __future__ import annotations
import pytest

from normlite import (
    DuplicateColumnError,
    Column, ColumnCollection, PrimaryKeyConstraint, Table,
    ArchivalFlag, Date, Integer, ObjectId, String
)

@pytest.fixture
def sc() -> ColumnCollection:
    return ColumnCollection([
        ('student_id', Column('student_id', Integer(), primary_key=True)),
        ('name', Column('name', String(is_title=True))),
        ('grade', Column('grade', String())),
        ('since', Column('since', Date()))
    ])

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
    students = Table(
        'students',
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
    students = Table(
        'students',
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

    