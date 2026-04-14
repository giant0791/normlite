from __future__ import annotations
from typing import List, Any, Optional, Self, Union

import pytest

from normlite import Table, Column, Integer, String
from normlite.exceptions import ArgumentError
from normlite.sql.dml import insert
from normlite.sql.type_api import Boolean

class SQLANode:
    """Base class for all SQLAlchemy-like AST nodes."""
    pass


class BindParameter(SQLANode):
    def __init__(self, key: str, value: Any):
        self.key = key
        self.value = value

    def __repr__(self):
        return f"BindParameter({self.key!r}, {self.value!r})"


class BinaryExpression(SQLANode):
    def __init__(self, left: SQLANode, operator: str, right: SQLANode):
        self.left = left
        self.operator = operator
        self.right = right

    def __repr__(self):
        return f"BinaryExpression({self.left!r} {self.operator!r} {self.right!r})"


class BooleanClauseList(SQLANode):
    def __init__(self, operator: str, clauses: List[SQLANode]):
        self.operator = operator
        self.clauses = clauses

    def __repr__(self):
        return f"BooleanClauseList({self.operator!r}, {self.clauses!r})"


class WhereClause(SQLANode):
    def __init__(self, condition: SQLANode):
        self.condition = condition

    def __repr__(self):
        return f"WhereClause({self.condition!r})"

class Select(SQLANode):
    def __init__(self, table_or_cols: Union[Table, List[Column]], whereclause: Optional[WhereClause] = None):
        self.table_or_cols = table_or_cols
        self.whereclause = whereclause

    def where(self, condition: SQLANode) -> Self:
        pass

    def order_by(self, *column: Column, direction: str = 'ASC'):
        pass

    def limit(self, limit: int):
        pass

    def __repr__(self):
        return f"Select(columns={self.columns!r}, where={self.whereclause!r})"
    

@pytest.fixture
def students() -> Table:
    return Table(
        'students',
        Column('id', Integer()),
        Column('name', String(is_title=True)),
        Column('grade', String())            
    )

def test_insert_gen_values_kwargs(students: Table):
    insert_stmt = students.insert().values(id=123456, name='Isaac Newton', grade='B')

    assert insert_stmt._values['id'] == 123456
    assert insert_stmt._values['name'] == 'Isaac Newton'
    assert insert_stmt._values['grade'] == 'B'

    with pytest.raises(KeyError, match='non_existing'):
        insert_stmt._values['non_existing']

def test_insert_gen_values_dict(students: Table):
    insert_stmt = students.insert().values({'id': 123456, 'name':'Isaac Newton', 'grade': 'B'})

    assert insert_stmt._values['id'] == 123456
    assert insert_stmt._values['name'] == 'Isaac Newton'
    assert insert_stmt._values['grade'] == 'B'

    with pytest.raises(KeyError, match='non_existing'):
        insert_stmt._values['non_existing']

def test_insert_gen_values_tuple(students: Table):
    insert_stmt = students.insert().values((123456, 'Isaac Newton', 'B'))

    assert insert_stmt._values['id'] == 123456
    assert insert_stmt._values['name'] == 'Isaac Newton'
    assert insert_stmt._values['grade'] == 'B'

    with pytest.raises(KeyError, match='non_existing'):
        insert_stmt._values['non_existing']

def test_insert_gen_values_tuple_not_enough_values(students: Table):
    with pytest.raises(ArgumentError, match='Required: 3, supplied: 2'):
        insert_stmt = students.insert().values((123456, 'Isaac Newton',))

def test_effective_table_len(students: Table):
    # use len to get the number of user defined columns, 
    # i.e. excl. those starting with '_no_'
    assert students.c.len() == 3

def test_insert_returning_no_cols(students: Table):
    insert_stmt = students.insert()
    assert insert_stmt._returning == ('_no_id', '_no_archived',)

def test_insert_returning_usr_def_cols(students: Table):
    insert_stmt = students.insert().returning(students.c.id, students.c.name)
    assert insert_stmt._returning == ('_no_id', '_no_archived', 'id', 'name',)

def test_insert_returning_col_must_belong_to_table(students: Table):
    wrong_table = Table(
        'wrong_table',
        Column('id', Integer()),
        Column('name', String(is_title=True)),
        Column('is_active', Boolean())
    )

    with pytest.raises(ArgumentError, match='Column: id'):
        insert_stmt = students.insert().returning(wrong_table.c.id, students.c.name)

    with pytest.raises(ArgumentError, match='Column: is_active'):
        insert_stmt = students.insert().returning(students.c.id, wrong_table.c.is_active)

def test_insert_constructor(students: Table):
    insert_stmt = insert(students)
    insert_stmt.values(id=123456, name='Isaac Newton', grade='B')

    assert insert_stmt._returning == ('_no_id', '_no_archived',)
    assert insert_stmt._values['id'] == 123456
    assert insert_stmt._values['name'] == 'Isaac Newton'
    assert insert_stmt._values['grade'] == 'B'

    with pytest.raises(KeyError, match='non_existing'):
        insert_stmt._values['non_existing']

    insert_stmt.returning(students.c.id, students.c.name)
    assert insert_stmt._returning == ('_no_id', '_no_archived', 'id', 'name',)



