import pdb

from faker import Faker
import pytest

from normlite.engine.base import Engine
from normlite.engine.context import ExecutionStyle
from normlite.sql.dml import delete, insert, select

from normlite.sql.schema import Table
from tests.utils.execution import run_context
from tests.utils.db_helpers import (
    create_students_db,
    attach_table_oid,
    populate_students,
)

@pytest.fixture
def created_students(engine: Engine, students: Table) -> Table:
    # create the table
    students.create(bind=engine, checkfirst=True)
    return students


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


def test_insert_execution_style_is_execute(engine, created_students):
    stmt = insert(created_students).values(**generate_values())
    _, ctx = run_context(engine, stmt)

    assert ctx.execution_style == ExecutionStyle.EXECUTE

def test_insert_postfetches_rows_into_bulk_parameters(engine, created_students):
    stmt = (
        insert(created_students)
        .values(**generate_values())
        .returning(*created_students.c)
    )

    _, ctx = run_context(engine, stmt)

    # number of operations equals number of rows inserted
    assert len(ctx.bulk_parameters) == 1

def test_insert_bulk_parameters_structure(engine, created_students):
    stmt = (
        insert(created_students)
        .values(**generate_values())
        .returning(created_students.c.object_id)
    )
    result, ctx = run_context(engine, stmt)
    expected_ids = [row.object_id for row in result.all()]
    expected = [
        {
            "path_params": {"page_id": oid},
        }
        for oid in expected_ids
    ]

    assert ctx.bulk_parameters == expected

def test_insert_execution_adds_all_rows(engine, created_students):
    # add new rows
    ROWS = 10
    results = []
    for i in range(ROWS):
        stmt = (
            insert(created_students)
            .values(**generate_values())
        )

        result, _ = run_context(engine, stmt, execution_options={"implicit_returning": True})
        results.append(result)

    # verify side-effect (rows added)
    sel = select(created_students).where(
        created_students.c.is_active.is_(True)
    )

    result, _ = run_context(engine, sel)
    new_rows = result.all()
    total_rowcount = sum([r.rowcount for r in results])
    returned_keys = [
        r.returned_primary_keys_rows[0]             # IMPORTANT: Remember this is a list of tuples
        for r in results
    ]
    inserted_ids = [(r.object_id,) for r in new_rows]

    assert total_rowcount == ROWS
    assert returned_keys == inserted_ids
