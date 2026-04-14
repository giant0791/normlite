from faker import Faker
import pytest

from normlite.engine.base import Engine
from normlite.sql.dml import delete, insert, select

from normlite.sql.schema import Table
from tests.utils.execution import run_execute

from tests.utils.assertions import (
    assert_rowcount,
    assert_no_rows,
    assert_columns,
    assert_single_row,
)


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

def test_insert_rowcount(
    engine: Engine,
    students: Table
):
    # create the table
    students.create(bind=engine, checkfirst=True)

    with engine.connect() as connection:
        results = []
        for i in range(5):
            values = generate_values()
            stmt = insert(students).values(**values)
            results.append(connection.execute(stmt))

    total_rows_inserted = sum([r.rowcount for r in results])     

    assert total_rows_inserted == 5
    assert results[0].rowcount == 1
    assert results[-1].rowcount == 1

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

    assert_no_rows(result)
    assert result.returned_primary_keys_rows is None

def test_no_returning_returning_implicit_false_returns_pks_only(
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
    assert_no_rows(result)
    assert len(result.returned_primary_keys_rows) == 1


def test_returning_returning_implicit_false_returns_all_cols_specified(
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
    row = assert_single_row(result)
    assert row.name == values["name"]
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

    row = assert_single_row(result)

    assert_columns(row, ["object_id", "name"])
    assert "is_deleted" not in row.mapping()
    assert len(set(row.keys())) == 2

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

    row = assert_single_row(result)
    expected = row.mapping()

    assert "object_id" not in expected
    assert "is_deleted" not in expected
    assert_columns(row, ["name"])
    assert len(set(row.keys())) == 1

def test_returning_all_cols(
    engine: Engine,
    students: Table        
):
    # create the table
    students.create(bind=engine, checkfirst=True)
    values = generate_values()
    stmt = (
        insert(students)
        .values(**values)
        .returning(*students.c)
    )

    with engine.connect() as connection:
        result = connection.execute(stmt)

    row = assert_single_row(result)
    columns = [
        "object_id", 
        "is_archived", 
        "is_deleted", 
        "created_at", 
        "name", 
        "id", 
        "is_active",
        "start_on",
        "grade",    
    ]

    assert_columns(row, columns)
