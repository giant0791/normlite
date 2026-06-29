from normlite.sql.functions import func
from normlite.engine.base import Engine
from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.sql.dml import insert, select
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import String


def test_count_returns_one_row_with_the_number_of_matched_rows(engine: Engine):
    # Arrange: a table seeded with three rows. The aggregate's column operand
    # (employees.c.name) is what anchors the query to the employees table.
    metadata = MetaData()
    employees = Table(
        "employees",
        metadata,
        Column("name", String(is_title=True)),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        for name in ("Galileo Galilei", "Isaac Newton", "Marie Curie"):
            connection.execute(insert(employees).values(name=name))

        # Act: count the matched rows
        result = connection.execute(select(func.count(employees.c.name)))
        rows = result.fetchall()

    # Assert: exactly one synthetic row carrying the count
    assert len(rows) == 1
    assert rows[0]["count"] == 3
