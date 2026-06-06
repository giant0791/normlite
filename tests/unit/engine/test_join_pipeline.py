import uuid

import pytest

from normlite import Relation, ForeignKey
from normlite.engine.base import Engine
from normlite.notiondbapi import DatabaseError
from normlite.notion_sdk.client import InMemoryNotionClient, NotionError
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

def test_outer_join_keeps_left_rows_whose_relation_targets_no_existing_right_page(
    engine: Engine,
):
    # Arrange: two students. One enrolled in a real course; one whose relation
    # points at a synthetic page id that was never inserted (dangling FK).
    # Mirrors the inner-join drop test — same fixture, opposite contract:
    # under OUTER join the dangling left row survives, right side None-filled.
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

        # Act: outer join over the same FK relation.
        result = connection.execute(
            select(students, courses).outerjoin(students.c.enrolled_in)
        )
        rows = result.fetchall()

    # Assert: BOTH students survive. Galileo is paired with his course;
    # Phantom's dangling reference is preserved with the right side None-filled.
    pairs = {(row.name, row.title) for row in rows}
    assert pairs == {
        ("Galileo Galilei", "Astronomy"),
        ("Phantom Student", None),
    }

def test_right_side_filter_on_outer_join_drops_none_filled_rows(engine: Engine):
    # Arrange: the same two-student fixture as the outer-join preservation test —
    # Galileo enrolled in a real course, Phantom Student pointing at a dangling id.
    # Under a bare outer join BOTH survive (Phantom None-filled). Here we layer a
    # WHERE on a RIGHT-side column (courses.title) on top of the outer join: the
    # None-filled row's title is None, so it must fail the filter and be dropped —
    # while the real match stays. This is the slice-5 ↔ slice-6 interaction:
    # the right-side predicate is answered client-side, after the join.
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

    bogus_course_oid = str(uuid.uuid4())  # never inserted -> dangling reference

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

        # Act: outer join, then a right-side filter on courses.title.
        result = connection.execute(
            select(students, courses)
            .outerjoin(students.c.enrolled_in)
            .where(courses.c.title.is_not_empty())
        )
        rows = result.fetchall()

    # Assert: the None-filled Phantom row fails the right-side filter and is
    # dropped; only the genuine match survives.
    pairs = {(row.name, row.title) for row in rows}
    assert pairs == {("Galileo Galilei", "Astronomy")}

def test_right_side_is_empty_drops_absent_entity(engine: Engine):
    # Arrange: As documented in ADR-0005, a right-side is_empty() drops the
    # phantom (absent entity) — is_empty is NOT IS NULL. Single-sided: the
    # present-entity-with-empty-title KEEP-side is currently unconstructable
    # (String.bind_processor never emits {title: []}), so this test only pins
    # the drop side. See memory project_unreachable_empty_title_boundary.
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

    bogus_course_oid = str(uuid.uuid4())  # never inserted -> dangling reference

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

        # Act: outer join, then a right-side filter on courses.title.
        result = connection.execute(
            select(students, courses)
            .outerjoin(students.c.enrolled_in)
            .where(courses.c.title.is_empty())
        )
        no_rows = result.fetchall()


    # Assert: the absent entity is dropped
    no_pairs = {(row.name, row.title) for row in no_rows}
    assert no_pairs == set()


def test_join_propagates_non_404_retrieve_error(engine: Engine, monkeypatch):
    # Differential to the dangling-reference tests above: a phase-2
    # `pages.retrieve` that 404s (`object_not_found`) is benign and silenced
    # (ADR-0002 lax-FK: a dangling relation entry is an absent reference, not
    # an error). But ANY OTHER retrieve error — here a 500 server error on a
    # legitimately-referenced right page — must NOT be swallowed: it propagates
    # via `_join_errorhandler`'s default raise path as a DBAPI DatabaseError.
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
        # Seed a genuine match so phase-2 actually issues a retrieve on a real,
        # resolvable right page (not a dangling id that would 404).
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
                name="Galileo Galilei",
                enrolled_in=[astronomy_oid],
            )
        )

        # Make phase-2 `pages.retrieve` fail with a non-404 server error AFTER
        # seeding, so only the join's retrieve is affected.
        def boom(self, path_params=None, query_params=None, payload=None):
            raise NotionError(
                "Internal server error",
                status_code=500,
                code="internal_server_error",
            )

        monkeypatch.setattr(InMemoryNotionClient, "pages_retrieve", boom)

        # Act + Assert: the server error propagates rather than being silenced.
        with pytest.raises(DatabaseError) as exc_info:
            connection.execute(
                select(students, courses).outerjoin(students.c.enrolled_in)
            )

    # The propagated DBAPI error carries the original NotionError as its cause,
    # and it is the non-404 we injected — not an accidental silencing/other path.
    cause = exc_info.value.__cause__
    assert isinstance(cause, NotionError)
    assert cause.code == "internal_server_error"
    assert cause.status_code == 500
