from datetime import date
import pdb
import uuid
from faker import Faker
import pytest

from normlite.engine.base import Engine, create_engine
from normlite.engine.context import ExecutionContext, ExecutionStyle
from normlite.engine.interfaces import _distill_params
from normlite.sql.reflection import ReflectedTableInfo
from normlite.notion_sdk.getters import get_object_id, rich_text_to_plain_text
from normlite.sql.compiler import NotionCompiler
from normlite.sql.ddl import CreateTable, DropTable
from normlite.sql.dml import ExecutableClauseElement, insert, select, delete
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, Number, String

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
def insert_values() -> dict:
    return dict(
        name = 'Galileo Galilei',
        id=123456,
        is_active=False,
        start_on=date(1690,1,1),
        grade='A'
    )

@pytest.fixture
def engine() -> Engine:
    return create_engine('normlite:///:memory:')

fake = Faker()
Faker.seed(42)

def generate_rows(
    engine: Engine, 
    students: Table,
    n: int,
):
    # create the table
    students.create(bind=engine, checkfirst=True)

    # generate the pages
    with engine.connect() as connection:
        for _ in range(n):
            stmt = insert(students).values(
                name = fake.name(),
                id=fake.random_int(100000, 999999),
                is_active=True,
                # fake.date() causes a TypeError, see issue 220 (https://github.com/giant0791/normlite/issues/220)
                start_on=fake.date_between(start_date='-10y', end_date='today'),
                grade=fake.random_element(["A", "B", "C", "D"])
            )

            connection.execute(stmt)

MAX_ROWS = 10

def setup_context(
    engine: Engine,
    elem: ExecutableClauseElement,
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
    )

    return ctx

#--------------------------------------------------
# delete context tests
#--------------------------------------------------
def test_rows_generation_works(
    engine: Engine,
    students: Table,
):
    # prefill database
    generate_rows(engine, students, n=MAX_ROWS)

    # check all pages have been correctly created
    stmt = select(students).where(students.c.is_active.is_(True))
    with engine.connect() as connection:
        result = connection.execute(stmt, execution_options={"preserve_rowcount": True})
    
    rows = result.all()
    all_existing = [not row["is_deleted"] for row in rows]

    assert result.rowcount == MAX_ROWS
    assert len(rows) == MAX_ROWS
    assert all(all_existing)

def test_delete_exec_ctx_execstyle_is_executemany(
    engine: Engine,
    students: Table,
):
    # prefill database
    generate_rows(engine, students, n=MAX_ROWS)
    elem = delete(students).where(students.c.is_active.is_(True))

    # 4. normalize params, options, payload
    ctx = setup_context(engine, elem)
    ctx.pre_exec()

    assert ctx.execution_style == ExecutionStyle.EXECUTEMANY

def test_delete_exec_ctx_prefetches_pages_to_delete(
    engine: Engine,
    students: Table,
):
    # prefill database
    generate_rows(engine, students, n=MAX_ROWS)
    elem = delete(students).where(students.c.is_active.is_(True))

    # 4. normalize params, options, payload
    ctx = setup_context(engine, elem)
    ctx.pre_exec()
    elem = ctx.invoked_stmt

    # 5. statement prepares execution
    elem._setup_execution(ctx)      
    assert len(ctx.bulk_parameters) == MAX_ROWS

def test_delete_ctx_bulk_params_contain_all_rows(
    engine: Engine,
    students: Table,
):
    # prefill database
    generate_rows(engine, students, n=MAX_ROWS)
    elem = delete(students).where(students.c.is_active.is_(True))

    # 4. normalize params, options, payload
    ctx = setup_context(engine, elem)
    ctx.pre_exec()
    elem = ctx.invoked_stmt

    # 5. statement prepares execution
    elem._setup_execution(ctx)      

    stmt = select(students).where(students.c.is_active.is_(True))
    with engine.connect() as connection:
        result = connection.execute(stmt)
    
    rows_to_delete = result.all()
    page_ids = [r["object_id"] for r in rows_to_delete]
    expected_bp = [
        {"path_params": {"page_id": page_id}, "payload": {"in_trash": True}}
        for page_id in page_ids
    ]

    assert expected_bp == ctx.bulk_parameters

def test_delete_ctx_execmany_deletes_all_rows(    
    engine: Engine,
    students: Table,
):
    # prefill database
    generate_rows(engine, students, n=MAX_ROWS)
    elem = delete(students).where(students.c.is_active.is_(True))

    # 4. normalize params, options, payload
    ctx = setup_context(engine, elem)
    ctx.pre_exec()
    elem = ctx.invoked_stmt

    # 5. statement prepares execution
    elem._setup_execution(ctx)      
    # 6. side effects happen (HTTP)
    engine.do_executemany(
        ctx.cursor, 
        ctx.bulk_operation, 
        ctx.bulk_parameters
    )

    stmt = select(students).where(students.c.is_active.is_(True))
    with engine.connect() as connection:
        result = connection.execute(stmt)
    
    rows = result.all()

    assert len(rows) == 0

def test_delete_complete_exec_pipeline(
    engine: Engine,
    students: Table,
):
    generate_rows(engine, students, n=MAX_ROWS)
    elem = delete(students).where(students.c.is_active.is_(True))

    with engine.connect() as connection:
        del_result = connection.execute(elem, execution_options={"preserve_rowcount": True})
        del_rows = del_result.all()
        stmt = select(students).where(students.c.is_active.is_(True))
        sel_result = connection.execute(stmt)

    rows = sel_result.all()

    assert len(rows) == 0
    assert del_result.rowcount == MAX_ROWS

    