from datetime import date
import pdb
from typing import Any, Mapping, Optional
import uuid
from faker import Faker
import pytest

from normlite.engine.base import Engine, create_engine
from normlite.engine.context import ExecutionContext, ExecutionStyle
from normlite.engine.interfaces import _distill_params
from normlite.exceptions import CompileError
from normlite.sql.dml import ExecutableClauseElement, insert, select, delete
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, String

@pytest.fixture
def mocked_db_id() -> str:
    return str(uuid.uuid4())

@pytest.fixture
def metadata() -> MetaData:
    return MetaData()

@pytest.fixture
def students(metadata: MetaData, mocked_db_id: str) -> Table:
    students = Table(
        'students',
        metadata,
        Column('name', String(is_title=True)),
        Column('id', Integer()),
        Column('is_active', Boolean()),
        Column('start_on', Date()),
        Column('grade',  String())
    )
    students._sys_columns["object_id"]._value = mocked_db_id      # ensure table is in reflected state
    return students

@pytest.fixture
def engine() -> Engine:
    engine = create_engine('normlite:///:memory:')
    engine.execution_options(preserve_rowcount=True)
    return engine


fake = Faker()
Faker.seed(42)

def generate_values() -> dict:
    return dict(
        name = fake.name(),
        id=fake.random_int(100000, 999999),
        is_active=True,
        start_on=fake.date_between(start_date='-10y', end_date='today'),
        grade=fake.random_element(["A", "B", "C", "D"])
    )

def setup_context(
    engine: Engine,
    elem: ExecutableClauseElement,
    *,
    execution_options: Optional[Mapping[str, Any]] = None
) -> ExecutionContext:

    parameters = None

    # 1. compile the statement
    compiler = engine._sql_compiler
    compiled = elem.compile(compiler)

    # 2. distill parameters
    distilled_params = _distill_params(parameters)  

    # 3. create the execution context
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        engine.raw_connection().cursor(),
        compiled,
        distilled_params,
        execution_options=execution_options
    )

    return ctx

def test_no_returning_no_returning_implicit_returns_none_pk(
    engine: Engine,
    students: Table
):
    # create the table
    students.create(bind=engine, checkfirst=True)
    values = generate_values()
    stmt = insert(students).values(**values)

    with engine.connect() as connection:
        result = connection.execute(stmt)

    assert not result.returns_rows
    assert result.all() == []
    assert result.returned_primary_keys_rows is None

def test_no_returning_true_returning_implicit_returns_pks_only(
    engine: Engine,
    students: Table        
):
    # create the table
    students.create(bind=engine, checkfirst=True)
    values = generate_values()
    stmt = insert(students).values(**values)

    with engine.connect() as connection:
        result = connection.execute(
            stmt,
            execution_options={"implicit_returning": True}
        )

    # metadata only since returning() is missing and returning_implicit is True
    assert not result.returns_rows
    assert result.all() == []
    assert len(result.returned_primary_keys_rows) == 1


def test_returning_false_returning_implicit_returns_all_cols_specified(
    engine: Engine,
    students: Table        
):
    # create the table
    students.create(bind=engine, checkfirst=True)
    values = generate_values()
    stmt = (
        insert(students)
        .values(**values)
        .returning(students.c.object_id, students.c.name)
    )

    with engine.connect() as connection:
        result = connection.execute(stmt)

    assert result.returns_rows
    rows = result.all()
    assert len(rows) == 1
    assert rows[0].name == values["name"]
    assert result.returned_primary_keys_rows is None

def test_returning_includes_object_id_explicitly_only(
    engine: Engine,
    students: Table        
):
    # create the table
    students.create(bind=engine, checkfirst=True)
    values = generate_values()
    stmt = (
        insert(students)
        .values(**values)
        .returning(students.c.object_id, students.c.name)
    )

    with engine.connect() as connection:
        result = connection.execute(stmt)

    rows = result.all()
    col_names = rows[0].keys()

    assert len(rows) == 1
    assert "object_id" in col_names
    assert "name" in col_names
    assert "is_deleted" not in col_names
    assert len(col_names) == 2

def test_returning_does_not_include_object_id_by_default(
    engine: Engine,
    students: Table        
):
    # create the table
    students.create(bind=engine, checkfirst=True)
    values = generate_values()
    stmt = (
        insert(students)
        .values(**values)
        .returning(students.c.name)
    )

    with engine.connect() as connection:
        result = connection.execute(stmt)

    rows = result.all()
    col_names = rows[0].keys()

    assert len(rows) == 1
    assert "object_id" not in col_names
    assert "name" in col_names
    assert "is_deleted" not in col_names
    assert len(col_names) == 1
