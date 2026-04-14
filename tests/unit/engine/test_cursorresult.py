from datetime import datetime
import pdb
import pytest
from normlite.engine.base import Engine
from normlite.engine.context import ExecutionContext
from normlite.engine.cursor import _NO_CURSOR_RESULT_METADATA, CursorResult, Row
from normlite.exceptions import MultipleResultsFound, NoResultFound, ResourceClosedError
from normlite.notiondbapi.dbapi2 import Cursor
from normlite._constants import SpecialColumns

from normlite.sql.dml import select
from normlite.sql.resultschema import SchemaInfo
from normlite.sql.schema import Table
from tests.utils.db_helpers import (
    create_students_db,
    attach_table_oid,
    populate_students
)
from tests.utils.execution import run_context

def make_ctx(
    engine: Engine,
    students: Table,
    *,
    n_rows: int = 5,
):
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)

    if n_rows == 0:
        populate_students(engine, students, 1, is_active=True)
        stmt = select(students).where(students.c.is_active.is_(False))
        _, ctx = run_context(
            engine,
            stmt
        )
        return ctx

    populate_students(engine, students, n_rows)
    stmt = select(students)
    _, ctx = run_context(
        engine,
        stmt
    )
    return ctx

def make_insert_bulk_cursor(
    engine: Engine,
    students: Table,
    n_rows: int = 5
) -> CursorResult:
    
    # 1. create the students database
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)

    # 2. get the DBAPI cursor for the executemany operation
    connection = engine.raw_connection()
    cursor = connection.cursor()

    # 3. prepare bulk operation and parameters  
    bulk_operation = {"endpoint": "pages", "request": "create"}
    bulk_parameters = []

    for i in range(n_rows):
        payload = {
            "parent": {
                "type": "database_id",
                "database_id": db_id
            },
            "properties": {
                "name": {"title": [{"text": {"content": f"name_{i}"}}]},
                "id": {"number": i},
                "is_active": {"checkbox": True},
                "start_on": {"date": {"start": "1600-01-01"}},
                "grade": {"rich_text": [{"text": {"content": "A"}}]},
            },
        }
        bulk_parameters.append({"payload": payload})

    # 4. inject row description and fire
    schema = SchemaInfo.from_table(
        students,
        execution_names=[c.name for c in students.c if c.is_system],
        projected_names=[c.name for c in students.uc],
    )
    cursor._inject_description(schema.as_sequence())

    engine.do_executemany(
        cursor,
        bulk_operation,
        bulk_parameters
    )

    stmt = select(students)
    dummy_compiled = stmt.compile(engine._sql_compiler)

    ctx = ExecutionContext(
        engine,
        None,
        cursor,
        dummy_compiled,
    )

    result = ctx.setup_cursor_result()
    result._cursor = cursor

    return result

# -----------------------------------------
# construction tests
# -----------------------------------------

def test_cursorresult_can_process_metadata(
    engine: Engine,
    students: Table,
):
    exec_ctx = make_ctx(engine, students)
    result = CursorResult(exec_ctx)
    row = result.fetchone()
    columns = list(row.mapping().keys())

    assert columns == [
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

# -----------------------------------------
# one() tests
# -----------------------------------------

def test_one_expects_one_row_only(
    engine: Engine,
    students: Table,
):
    exec_ctx_one_row_only = make_ctx(engine, students, n_rows=1)
    result = CursorResult(exec_ctx_one_row_only)
    row = result.one()

    assert row["is_active"]
    assert row["name"] == "name_0"

def test_one_soft_closes_after_usage(
    engine: Engine,
    students: Table      
):
    exec_ctx = make_ctx(engine, students, n_rows=1)
    result = CursorResult(exec_ctx)
    row = result.one()

    assert not result.returns_rows      # cursor is soft-closed
    assert result.all() == []           # cursor returns empty result if soft-closed

def test_one_raises_if_more_rows_avail(
    engine: Engine,
    students: Table,
):
    exec_ctx = make_ctx(engine, students, n_rows=5)
    result = CursorResult(exec_ctx) 
    
    with pytest.raises(MultipleResultsFound):
        row = result.one()

def test_one_raises_if_no_rows_avail(
    engine: Engine,
    students: Table,
):
    no_rows_exec_ctx = make_ctx(engine, students, n_rows=0)
    result = CursorResult(no_rows_exec_ctx)         
    
    with pytest.raises(NoResultFound):
        row = result.one()

def test_one_raises_if_closed(
    engine: Engine,
    students: Table,
):
    no_rows_exec_ctx = make_ctx(engine, students)
    result = CursorResult(no_rows_exec_ctx)
    result.close()         
    
    with pytest.raises(ResourceClosedError):
        row = result.one()

# -----------------------------------------
# all() tests
# -----------------------------------------

def test_all_returns_all_rows(
    engine: Engine,
    students: Table,
):
    no_rows_exec_ctx = make_ctx(engine, students, n_rows=10)
    result = CursorResult(no_rows_exec_ctx)
    rows = result.all()

    assert len(rows) == 10
    assert rows[0].name == "name_0"
    assert rows[-1].name == "name_9"

def test_all_soft_closes_after_usage(
    engine: Engine,
    students: Table      
):
    exec_ctx = make_ctx(engine, students, n_rows=1)
    result = CursorResult(exec_ctx)
    row = result.all()

    assert not result.returns_rows      # cursor is soft-closed
    assert result.all() == []           # cursor returns empty result if soft-closed

def test_all_raises_if_closed(
    engine: Engine,
    students: Table,
):
    no_rows_exec_ctx = make_ctx(engine, students)
    result = CursorResult(no_rows_exec_ctx)
    result.close()         
    
    with pytest.raises(ResourceClosedError):
        row = result.all()

# -----------------------------------------
# first() tests
# -----------------------------------------

def test_first_returns_first_row(
    engine: Engine,
    students: Table,
):
    no_rows_exec_ctx = make_ctx(engine, students)
    result = CursorResult(no_rows_exec_ctx)
    first = result.first()

    assert first.name == "name_0"
    assert first.id == 0

# -----------------------------------------
# fetchone tests
# -----------------------------------------

def test_fetchone_returns_rows_one_by_one(
    engine: Engine,
    students: Table,
):
    no_rows_exec_ctx = make_ctx(engine, students, n_rows=3)
    result = CursorResult(no_rows_exec_ctx)
    first = result.fetchone()
    second = result.fetchone()
    third = result.fetchone()
    should_be_none = result.fetchone()
    
    assert first.name == "name_0"
    assert second.name == "name_1"
    assert third.name == "name_2"
    assert should_be_none is None

def test_fetchone_raises_if_closed(
    engine: Engine,
    students: Table,
):
    no_rows_exec_ctx = make_ctx(engine, students)
    result = CursorResult(no_rows_exec_ctx)
    result.close()         
    
    with pytest.raises(ResourceClosedError):
        row = result.fetchone()

# -----------------------------------------
# fetchall() tests
# -----------------------------------------

def test_fetchall_returns_all_rows_found(
    engine: Engine,
    students: Table
):
    exec_ctx = make_ctx(engine, students, n_rows=10)
    result = CursorResult(exec_ctx)
    rows = result.fetchall()

    assert rows[0].name == "name_0"
    assert rows[0].id == 0
    assert rows[-1].name == "name_9"
    assert rows[-1].id == 9
    assert rows[-2].name == "name_8"
    assert rows[-2].id == 8

def test_fetchall_soft_closes_cursor(
    engine: Engine,
    students: Table
):
    exec_ctx = make_ctx(engine, students, n_rows=10)
    result = CursorResult(exec_ctx)
    rows = result.fetchall()
    should_be_none = result.fetchone()

    assert should_be_none is None
    assert not result.returns_rows
    
# -----------------------------------------
# fetchall() tests
# -----------------------------------------

def test_fetchmany_returns_arrasize_multiples_of_rows(
    engine: Engine,
    students: Table
):
    def is_today(iso_string):
        try:
            # Parse the ISO 8601 string into a datetime object
            parsed_dt = datetime.fromisoformat(iso_string)
            
            # Compare only the date parts (Year, Month, Day)
            return parsed_dt.date() == datetime.now().date()
        except ValueError:
            # Handle cases where the string isn't a valid ISO format
            return False


    result = make_insert_bulk_cursor(engine, students, 8)
    first_batch = result.fetchmany(size=4)
    second_batch = result.fetchmany(size=4)

    assert len(first_batch) == 4
    assert len(second_batch) == 4
    assert is_today(first_batch[0].created_at)
    assert is_today(first_batch[3].created_at)
    assert is_today(second_batch[0].created_at)
    assert is_today(second_batch[3].created_at)   

def test_fetchmany_returns_all_if_arraysize_less_than_rows(
    engine: Engine,
    students: Table
):
    exec_ctx = make_ctx(engine, students, n_rows=10)
    result = CursorResult(exec_ctx)
    rows = result.fetchmany(size=12)

    assert len(rows) == 10

def test_fetchmany_returns_arraysize_batches_but_last(
    engine: Engine,
    students: Table
):
    exec_ctx = make_ctx(engine, students, n_rows=10)
    result = CursorResult(exec_ctx)
    first = result.fetchmany(size=4)
    second = result.fetchmany(size=4)
    third = result.fetchmany(size=4)
    should_be_empty = result.fetchmany(size=4)

    assert len(first) == 4
    assert len(second) == 4
    assert len(third) == 2
    assert should_be_empty == []
    assert not result.returns_rows

def test_fetchmany_consumes_resultset(
    engine: Engine,
    students: Table
):
    exec_ctx = make_ctx(engine, students, n_rows=4)
    result = CursorResult(exec_ctx)
    first = result.fetchmany(size=6)
    should_be_empty = result.fetchmany(size=4)

    assert len(first) == 4
    assert should_be_empty == []
    assert not result.returns_rows

def test_fetchmany_acts_like_fetchone_if_no_size(
    engine: Engine,
    students: Table
):
    exec_ctx = make_ctx(engine, students, n_rows=4)
    result = CursorResult(exec_ctx)
    only_one = result.fetchmany()

    assert len(only_one) == 1
    assert result.returns_rows
    