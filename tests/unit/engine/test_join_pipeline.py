import pdb
import uuid

import pytest

from normlite import Relation, ForeignKey
from normlite.engine.base import Engine
from normlite.sql.dml import insert, select
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import String


def test_inner_join_pairs_left_and_right_rows(engine: Engine):
    # --- Arrange: two FK-linked tables ---
    metadata = MetaData()
    courses = Table(
        "courses",
        metadata,
        Column("title", String(is_title=True)),
    )
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        # seed one course, capture its object_id via RETURNING
        course_oid = (
            connection.execute(
                insert(courses)
                .values(title="Astronomy")
                .returning(courses.c.object_id)
            )
            .first()
            .object_id
        )

        # seed one student linked to that course
        connection.execute(
            insert(students).values(
                name="Galileo Galilei",
                enrolled_in=[course_oid],
            )
        )

        # execute the join
        result = connection.execute(
            select(students, courses).join(students.c.enrolled_in)
        )
        rows = result.fetchall()

    # --- Assert: exactly one paired row, both sides' user columns present ---
    assert len(rows) == 1
    row = rows[0]
    assert row.name == "Galileo Galilei"
    assert row.title == "Astronomy"

def test_inner_join_emits_one_pair_per_left_row_when_each_points_at_a_distinct_right_row(
    engine: Engine,
):
    # Arrange: two students, each enrolled in a *different* course.
    metadata = MetaData()
    courses = Table(
        "courses",
        metadata,
        Column("title", String(is_title=True)),
    )
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        astronomy_oid = (
            connection.execute(
                insert(courses)
                .values(title="Astronomy")
                .returning(courses.c.object_id)
            )
            .first()
            .object_id
        )
        physics_oid = (
            connection.execute(
                insert(courses)
                .values(title="Physics")
                .returning(courses.c.object_id)
            )
            .first()
            .object_id
        )

        connection.execute(
            insert(students).values(
                name="Galileo Galilei",
                enrolled_in=[astronomy_oid],
            )
        )
        connection.execute(
            insert(students).values(
                name="Isaac Newton",
                enrolled_in=[physics_oid],
            )
        )

        # Act
        result = connection.execute(
            select(students, courses).join(students.c.enrolled_in)
        )
        rows = result.fetchall()

    # Assert: both pairings present (order intentionally not asserted).
    pairs = {(row.name, row.title) for row in rows}
    assert pairs == {
        ("Galileo Galilei", "Astronomy"),
        ("Isaac Newton", "Physics"),
    }

def test_inner_join_silently_drops_left_rows_whose_relation_targets_no_existing_right_page(
    engine: Engine,
):
    # Arrange: two students. One enrolled in a real course; one whose relation
    # points at a synthetic page id that was never inserted.
    metadata = MetaData()
    courses = Table(
        "courses",
        metadata,
        Column("title", String(is_title=True)),
    )
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )
    metadata.create_all(engine)

    bogus_course_oid = str(uuid.uuid4())  # nothing in `courses` will match this

    with engine.connect() as connection:
        astronomy_oid = (
            connection.execute(
                insert(courses)
                .values(title="Astronomy")
                .returning(courses.c.object_id)
            )
            .first()
            .object_id
        )

        connection.execute(
            insert(students).values(
                name="Phantom Student",
                enrolled_in=[bogus_course_oid],
            )
        )

        connection.execute(
            insert(students).values(
                name="Galileo Galilei",
                enrolled_in=[astronomy_oid],
            )
        )

        # Act
        result = connection.execute(
            select(students, courses).join(students.c.enrolled_in)
        )
        rows = result.fetchall()

    # Assert: the dangling reference is silently dropped; the valid pair still appears.
    pairs = {(row.name, row.title) for row in rows}
    assert pairs == {("Galileo Galilei", "Astronomy")}

