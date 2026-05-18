import pytest

from normlite import Relation, ForeignKey
from normlite.engine.base import Engine
from normlite.sql.dml import insert, select
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import String


def test_e2e_insert_and_select_through_relation_filter(engine: Engine):
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
        # --- Act 1: insert a course, capture its object_id via RETURNING ---
        course_stmt = (
            insert(courses)
            .values(title="Math 101")
            .returning(courses.c.object_id)
        )
        course_result = connection.execute(course_stmt)
        course_oid = course_result.first().object_id

        # --- Act 2: insert a student linked to that course ---
        student_stmt = insert(students).values(
            name="Alice",
            enrolled_in=[course_oid],
        )
        connection.execute(student_stmt)

        # --- Act 3: select students linked to that course ---
        select_stmt = select(students).where(
            students.c.enrolled_in.contains(course_oid)
        )
        result = connection.execute(select_stmt)
        rows = result.all()

    # --- Assert: exactly one row, the linked student ---
    assert len(rows) == 1
    assert rows[0].name == "Alice"
