"""This test module focuses on verifying that compilation and parameters binding of the DML constructs are correct.

Compilation correctness means that tests check 
1. the emitted JSON code is as expected (template with named arguments)
2. the parameters are correctly bound to the named arguments

Note:
    DML construct execution is not in the test scope here.
"""
from datetime import datetime, date
import uuid
import pdb
import pytest
from normlite._constants import SpecialColumns
from normlite.engine.base import Connection, Engine, create_engine
from normlite.engine.context import ExecutionContext
from normlite.engine.cursor import CursorResult
from normlite.engine.row import Row
from normlite.sql.base import Compiled
from normlite.sql.compiler import NotionCompiler
from normlite.sql.dml import Insert, insert, select
from normlite.sql.elements import BinaryExpression, BindParameter
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, Money, Numeric, String

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

def insert_student(
    engine: Engine,
    *,
    student_id: int,
    name: str,
    grade: str,
    is_active: bool
) -> None:
    engine._client._add('page', {
        'parent': {
            'type': 'database_id',
            'database_id': engine._client._get_by_title('students', 'database')['id']
        },
        'properties': {
            'student_id': {'number': student_id},
            'name': {
                'title': [{'text': {'content': name}}]
            },
            'grade': {
                'rich_text': [{'text': {'content': grade}}]
            },
            'is_active': {'checkbox': is_active}
        }
    })


def test_insert_bind_values():
    metadata = MetaData()
    students = Table(
        'students', 
        metadata,
        Column('name', String(is_title=True)),
        Column('year_reg', Date())
    )
    stmt = insert(students).values(name='Galileo Galilei', year_reg=datetime(1689, 9, 1))
    name = stmt._values['name']
    year_reg = stmt._values['year_reg']
    assert stmt.is_insert
    assert isinstance(name, BindParameter)
    assert name.value == 'Galileo Galilei'
    assert isinstance(year_reg, BindParameter)
    assert year_reg.value == datetime(1689, 9, 1) 

def test_compile_insert_bindparams():
    metadata = MetaData()
    students = Table(
        'students', 
        metadata,
        Column('name', String(is_title=True)),
        Column('year_reg', Date())
    )
    # monkey patch the id to simulate reflection
    database_id = str(uuid.uuid5)
    students.set_oid(database_id)
    stmt = insert(students).values(name='Galileo Galilei', year_reg=datetime(1689, 9, 1))
    sql_compiler = NotionCompiler()
    compiled = stmt.compile(sql_compiler)
    name, name_usage = compiled.params['name']
    year_reg, year_reg_usage = compiled.params['year_reg']
    assert isinstance(name, BindParameter)
    assert name_usage == 'value'
    assert name.value == 'Galileo Galilei'
    assert year_reg_usage == 'value'
    assert year_reg.value == datetime(1689, 9, 1)

@pytest.mark.skip(reason='Need to fix the bool -> checkbox issue first')
def test_insert_can_add_new_row(engine: Engine):
    expected_values= {
        'student_id': 1,
        'name': 'Galileo Galilei',
        'grade': 'A',
        'is_active': False
    }
    create_students_db(engine)
    metadata = MetaData()
    students = Table('students', metadata, autoload_with=engine)
    assert 'is_active' in students.c

    stmt: Insert = insert(students).values(**expected_values)
    with engine.connect() as connection:
        result: CursorResult = connection.execute(stmt)
        row: Row = result.one()
        assert row._no_id
        with pytest.raises(AttributeError):
            # This should raise an AttributeError as special columns only are returned
            assert row.student_id

@pytest.mark.skip(reason='Integration test, not ready yet.')
def test_select_w_simple_condition(engine: Engine):
    expected_values= {
        'student_id': 1,
        'name': 'Galileo Galilei',
        'grade': 'A',
        'is_active': False
    }
    create_students_db(engine)
    metadata = MetaData()
    students = Table('students', metadata, autoload_with=engine)
    assert 'is_active' in students.c

    stmt: Insert = insert(students).values(**expected_values)
    connection = engine.connect()
    connection.execute(stmt)

    select_stmt = select(students).where(students.c.name == 'Galileo Galilei')

def test_create_ast_for_simple_col_expr():
    metadata = MetaData()
    students = Table('students', metadata, Column('name', String(is_title=True)))
    exp: BinaryExpression = students.c.name == 'Galileo Galilei'
    assert isinstance(exp, BinaryExpression)
    assert isinstance(exp.value, BindParameter)
    assert exp.value.effective_value == 'Galileo Galilei'

def test_compile_binexp_title_eq():
    metadata = MetaData()
    students = Table('students', metadata, Column('name', String(is_title=True)))
    exp: BinaryExpression = students.c.name == 'Galileo Galilei'
    compiled = exp.compile(NotionCompiler())
    as_dict = compiled.as_dict()
    assert as_dict['property'] == 'name'
    assert as_dict['title'] == {'equals': ':param_0'}

def test_compile_binexp_title_neq():
    metadata = MetaData()
    students = Table('students', metadata, Column('name', String(is_title=True)))
    exp: BinaryExpression = students.c.name != 'Galileo Galilei'
    compiled = exp.compile(NotionCompiler())
    as_dict = compiled.as_dict()
    assert as_dict['property'] == 'name'
    assert as_dict['title'] == {'does_not_equal': ':param_0'}

def test_compile_binexp_title_in():
    metadata = MetaData()
    students = Table('students', metadata, Column('name', String(is_title=True)))
    exp: BinaryExpression = students.c.name.in_('Galileo')
    compiled = exp.compile(NotionCompiler())
    as_dict = compiled.as_dict()
    assert as_dict['property'] == 'name'
    assert as_dict['title'] == {'contains': ':param_0'}

def test_compile_binexp_title_not_in():
    metadata = MetaData()
    students = Table('students', metadata, Column('name', String(is_title=True)))
    exp: BinaryExpression = students.c.name.not_in('Galileo')
    compiled = exp.compile(NotionCompiler())
    as_dict = compiled.as_dict()
    assert as_dict['property'] == 'name'
    assert as_dict['title'] == {'does_not_contain': ':param_0'}

def test_compile_binexp_title_endswith():
    metadata = MetaData()
    students = Table('students', metadata, Column('name', String(is_title=True)))
    exp: BinaryExpression = students.c.name.endswith('lilei')
    compiled = exp.compile(NotionCompiler())
    as_dict = compiled.as_dict()
    assert as_dict['property'] == 'name'
    assert as_dict['title'] == {'ends_with': ':param_0'}

def test_compile_binexp_title_startswith():
    metadata = MetaData()
    students = Table('students', metadata, Column('name', String(is_title=True)))
    exp: BinaryExpression = students.c.name.startswith('lilei')
    compiled = exp.compile(NotionCompiler())
    as_dict = compiled.as_dict()
    assert as_dict['property'] == 'name'
    assert as_dict['title'] == {'starts_with': ':param_0'}

def test_compile_binexp_number_operators():
    metadata = MetaData()
    students = Table(
        'students', 
        metadata, 
        Column('id', Integer()),
        Column('rate', Money(currency='dollar')),
        Column('grade', Numeric())    
    )

    e1 = students.c.id == 123456
    e2 = students.c.rate < 200_000
    e3 = students.c.grade > 1.6

    # reusing the compiler means that
    # the bind param counter increments and does not get reset
    nc = NotionCompiler()                                       
    c1 = e1.compile(nc)
    c2 = e2.compile(nc)
    c3 = e3.compile(nc)

    d1 = c1.as_dict()
    d2 = c2.as_dict()
    d3 = c3.as_dict()

    assert d1['property'] == 'id'
    assert d2['property'] == 'rate'
    assert d3['property'] == 'grade'
    assert d1['number'] == {'equals': ':param_0'}               
    assert d2['number'] == {'less_than': ':param_1'}            # because you are using the same compiler instance
    assert d3['number'] == {'greater_than': ':param_2'}         # because you are using the same compiler instance

def test_compile_binexp_date_before():
    metadata = MetaData()
    students = Table('students', metadata, Column('start_date', Date()))
    exp: BinaryExpression = students.c.start_date.before(date.today)
    compiled = exp.compile(NotionCompiler())
    as_dict = compiled.as_dict()
    assert as_dict['property'] == 'start_date'
    assert as_dict['date']['before'] == ':param_0'

def test_compile_binexp_date_after():
    metadata = MetaData()
    students = Table('students', metadata, Column('start_date', Date()))
    exp: BinaryExpression = students.c.start_date.after(date.today)
    sql_compiler = NotionCompiler()
    compiled = exp.compile(sql_compiler)
    as_dict = compiled.as_dict()
    assert as_dict['property'] == 'start_date'
    assert as_dict['date']['after'] == ':param_0'

def test_compile_binexp_bool_opertors():
    metadata = MetaData()
    students = Table('students', metadata, Column('is_active', Boolean()))
    e1: BinaryExpression = students.c.is_active == True
    e2: BinaryExpression = students.c.is_active != True
    e3: BinaryExpression = students.c.is_active.is_(True)
    e4: BinaryExpression = students.c.is_active.is_not(True)
    nc = NotionCompiler()
    c1 = e1.compile(nc)
    c2 = e2.compile(nc)
    c3 = e3.compile(nc)
    c4 = e4.compile(nc)
    d1 = c1.as_dict()
    d2 = c2.as_dict()
    d3 = c3.as_dict()
    d4 = c4.as_dict()
    bp1, u1 = c1._execution_binds['param_0']
    bp2, u2 = c2._execution_binds['param_1']
    bp3, u3 = c3._execution_binds['param_2']
    bp4, u4 = c4._execution_binds['param_3']
    assert d1['property'] == 'is_active'
    assert d1['checkbox']['equals'] == ':param_0'
    assert u1 == u2 == 'value'
    assert bp1.effective_value
    assert d2['property'] == 'is_active'
    assert d2['checkbox']['does_not_equal'] == ':param_1'
    assert bp2.effective_value
    assert d3['property'] == 'is_active'
    assert d3['checkbox']['equals'] == ':param_2'
    assert u3 == u4 == 'value'
    assert bp3.effective_value
    assert d4['property'] == 'is_active'
    assert d4['checkbox']['does_not_equal'] == ':param_3'
    assert bp4.effective_value

def test_compile_binexp_bool_forbid_truthiness():
    metadata = MetaData()
    students = Table('students', metadata, Column('is_active', Boolean()))

    with pytest.raises(TypeError, match='Use explicit comparison.'):
       if students.c.is_active:
           pass

def test_compile_select():
    metadata = MetaData()
    students = Table('students', metadata, Column('start_date', Date()))
    # monkey patch the id to simulate reflection
    database_id = str(uuid.uuid4())
    students.set_oid(database_id)
    stmt = select(students)
    sql_compiler = NotionCompiler()
    compiled = stmt.compile(sql_compiler)
    as_dict = compiled.as_dict()
    assert as_dict['operation']['request'] == 'query'
    assert as_dict['parameters']['database_id'] == database_id
    result_columns = ['start_date'] + [
        col 
        for col in SpecialColumns.values() 
        if col not in (SpecialColumns.NO_PID.value, SpecialColumns.NO_TITLE.value)
    ]
    assert result_columns == compiled.result_columns()

def test_compile_select_w_where():
    metadata = MetaData()
    students = Table('students', metadata, Column('start_date', Date()))
    # monkey patch the id to simulate reflection
    database_id = str(uuid.uuid4())
    students.set_oid(database_id)
    stmt = select(students).where(students.c.start_date.before(date.today))
    sql_compiler = NotionCompiler()
    compiled = stmt.compile(sql_compiler)
    as_dict = compiled.as_dict()
    filter = as_dict['operation']['template']['filter']
    assert filter['property'] == 'start_date'
    _, usage = sql_compiler._compiler_state.execution_binds['param_0']
    assert usage == 'filter'

def test_compile_select_concat_wheres():
    metadata = MetaData()
    students = Table(
        'students', 
        metadata, 
        Column('name', String(is_title=True)),
        Column('start_date', Date())
    )
    # monkey patch the id to simulate reflection
    database_id = str(uuid.uuid4())
    students.set_oid(database_id)
    stmt = select(students)
    stmt.where(students.c.name != 'Galileo Galilei')
    stmt.where(students.c.start_date.before(date.today))
    sql_compiler = NotionCompiler()
    compiled = stmt.compile(sql_compiler)
    as_dict = compiled.as_dict()
    filter = as_dict['operation']['template']['filter']
    and_clauselist =  filter['and'] 
    assert len(and_clauselist) == 2
    assert and_clauselist[0]['property'] == 'name'
    assert and_clauselist[1]['property'] == 'start_date'

def test_compile_nested_boolean_clause_list():
    metadata = MetaData()

    projects = Table(
        'projects',
        metadata,
        Column('name', String(is_title=True)),
        Column('done', Boolean()),
        Column('tags', String()),
    )

    # monkey patch the id to simulate reflection
    database_id = str(uuid.uuid4())
    projects.set_oid(database_id)

    # WHERE:
    # done = true
    # AND (tags contains "A" OR tags contains "B")
    expr = (
        (projects.c.done == True)
        &
        (
            projects.c.tags.in_("A")
            |
            projects.c.tags.in_("B")
        )
    )

    stmt = select(projects).where(expr)

    compiled = stmt.compile(NotionCompiler())
    result = compiled.as_dict()

    assert result['operation']['template']["filter"] == {
        "and": [
            {
                "property": "done",
                "checkbox": {
                    "equals": ":param_0"
                }
            },
            {
                "or": [
                    {
                        "property": "tags",
                        "rich_text": {
                            "contains": ":param_1"
                        }
                    },
                    {
                        "property": "tags",
                        "rich_text": {
                            "contains": ":param_2"
                        }
                    }
                ]
            }
        ]
    }

def test_select_where_filter_binding(dbapi_cursor):
    metadata = MetaData()

    students = Table(
        'students',
        metadata,
        Column('name', String(is_title=True)),
        Column('start_date', Date()),
    )

    # monkey patch the id to simulate reflection
    database_id = str(uuid.uuid4())
    students.set_oid(database_id)

    stmt = (
        select(students)
        .where(students.c.start_date.before(date(2020, 1, 1)))
    )

    compiled = stmt.compile(NotionCompiler())
    ctx = ExecutionContext(dbapi_cursor, compiled)
    ctx.setup()
    payload = compiled.as_dict()['operation']['payload']

    assert payload['filter'] == {
        "property": "start_date",
        "date": {
            "before": "2020-01-01"
        }
    }

def test_select_all_students(engine: Engine):
    create_students_db(engine)

    insert_student(engine, student_id=1, name='Galileo Galilei', grade='A', is_active=True)
    insert_student(engine, student_id=2, name='Isaac Newton', grade='A', is_active=False)
    insert_student(engine, student_id=3, name='Marie Curie', grade='B', is_active=True)
    insert_student(engine, student_id=4, name='Albert Einstein', grade='C', is_active=True)

    metadata = MetaData()
    students = Table('students', metadata, autoload_with=engine)

    with engine.connect() as connection:
        stmt = select(students)
        result = connection.execute(stmt)

        rows = result.fetchall()

    assert len(rows) == 4
    assert {r.name for r in rows} == {
        'Galileo Galilei',
        'Isaac Newton',
        'Marie Curie',
        'Albert Einstein',
    }


