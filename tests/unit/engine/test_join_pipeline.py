import pdb
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

def test_right_side_filter_under_narrowed_projection_keeps_genuine_match(engine: Engine):
    # Arrange: same FK-linked two-table fixture as the full-projection
    # right-side-filter test — Galileo enrolled in a real course, Phantom
    # Student pointing at a dangling id. The ONLY difference here is the
    # projection: instead of select(students, courses) we narrow to a
    # column list (one left col, one right col) and THEN layer the same
    # right-side WHERE on courses.title. The genuine match (Galileo →
    # Astronomy) satisfies is_not_empty(), so a correct filter must KEEP it.
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

        # Act: NARROWED projection (column list, not whole tables) + the
        # right-side filter on courses.title.
        result = connection.execute(
            select(students.c.name, courses.c.title)
            .outerjoin(students.c.enrolled_in)
            .where(courses.c.title.is_not_empty())
        )
        rows = result.fetchall()

    # Assert: the genuine match survives. (Phantom's dangling row is
    # None-filled and correctly fails the right-side filter.)
    pairs = {(row.name, row.title) for row in rows}
    assert pairs == {("Galileo Galilei", "Astronomy")}


def test_right_side_filter_on_colliding_column_keeps_genuine_match(engine: Engine):
    # Arrange: two FK-linked tables that BOTH declare a `title` column -- the
    # name collision. `students.enrolled_in` points at a `courses` row. Seed a
    # single genuine match: Galileo's student row links to the Astronomy course.
    # Then layer a right-side WHERE on the COLLIDING column courses.title.
    #
    # Today this returns [] (the all-None phantom guard over-drops because the
    # right-side filter selects its getters by bare name -- `title` -- which is
    # qualified to `courses.title` in the merged schema and so never matches;
    # the synthetic page is keyed by the qualified name while the compiled
    # filter references the bare `title`). See ADR-0009: provenance + identity
    # selection fixes it. The genuine match satisfies is_not_empty(), so a
    # correct filter MUST keep it.
    metadata = MetaData()
    courses = Table(
        "courses",
        metadata,
        Column("title", String(is_title=True)),
    )
    students = Table(
        "students",
        metadata,
        Column("title", String(is_title=True)),
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
        connection.execute(
            insert(students).values(
                title="Galileo Galilei",
                enrolled_in=[astronomy_oid],
            )
        )

        # Act: inner join, then a right-side filter on the colliding courses.title.
        result = connection.execute(
            select(students, courses)
            .join(students.c.enrolled_in)
            .where(courses.c.title.is_not_empty())
        )
        rows = result.fetchall()

    # Assert: the genuine match survives, with both titles reachable under their
    # table-qualified keys.
    assert len(rows) == 1
    m = rows[0].mapping()
    assert m["students.title"] == "Galileo Galilei"
    assert m["courses.title"] == "Astronomy"


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

def test_join_qualifies_colliding_column_names_by_table(engine: Engine):
    # Arrange: two tables that BOTH declare a `name` column, FK-linked.
    # `students.advisor` points at an `instructors` row. A bare merged schema
    # would emit two columns both literally named "name" — and because the
    # cursor metadata keys columns by name, the right side would silently
    # clobber the left. The join must instead disambiguate on collision using
    # each table's public name.
    metadata = MetaData()
    instructors = Table(
        "instructors",
        metadata,
        Column("name", String(is_title=True)),
    )
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("advisor", Relation(), ForeignKey("instructors.object_id")),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        advisor_oid = (
            connection.execute(
                insert(instructors)
                .values(name="Vincenzo Galilei")
                .returning(instructors.c.object_id)
            )
            .first()
            .object_id
        )
        connection.execute(
            insert(students).values(
                name="Galileo Galilei",
                advisor=[advisor_oid],
            )
        )

        # Act: join the two tables, both of which carry a `name`.
        result = connection.execute(
            select(students, instructors).join(students.c.advisor)
        )
        row = result.fetchall()[0]

    # Assert: both `name`s are reachable under table-qualified keys, with no
    # bare-`name` shadow collapsing the two into one.
    keys = set(row.keys())
    assert "students.name" in keys
    assert "instructors.name" in keys
    assert "name" not in keys

    m = row.mapping()
    assert m["students.name"] == "Galileo Galilei"
    assert m["instructors.name"] == "Vincenzo Galilei"


def test_join_projects_columns_from_both_tables(engine: Engine):
    # Arrange: an FK-linked pair with DISTINCT column names, so qualification
    # never enters the picture. The point of this behavior is narrower: a SELECT
    # that lists columns drawn from BOTH joined tables must be expressible and
    # must surface every projected column, each carrying its own side's value.
    metadata = MetaData()
    instructors = Table(
        "instructors",
        metadata,
        Column("director", String(is_title=True)),
    )
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("advisor", Relation(), ForeignKey("instructors.object_id")),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        advisor_oid = (
            connection.execute(
                insert(instructors)
                .values(director="Vincenzo Galilei")
                .returning(instructors.c.object_id)
            )
            .first()
            .object_id
        )
        connection.execute(
            insert(students).values(
                name="Galileo Galilei",
                advisor=[advisor_oid],
            )
        )

        # Act: project one column from each side of the join.
        result = connection.execute(
            select(students.c.name, instructors.c.director).join(students.c.advisor)
        )
        row = result.fetchall()[0]

    # Assert: both projected columns are reachable, each with its own table's value.
    keys = set(row.keys())
    assert "name" in keys
    assert "director" in keys

    m = row.mapping()
    assert m["name"] == "Galileo Galilei"
    assert m["director"] == "Vincenzo Galilei"

def test_join_keeps_projected_name_bare_when_collision_is_unprojected(engine: Engine):
    # Arrange: `name` exists on BOTH tables, but only as a TABLE-level
    # coincidence -- on `instructors` it is a plain (non-title) property that
    # the query never asks for. The SELECT projects `students.name` and
    # `instructors.director`; `instructors.name` is NOT projected. There is
    # therefore no collision *in the projection*, so the surviving `name` key
    # must stay BARE. Qualifying it (`students.name`) would be over-
    # qualification driven by the table schemas rather than by what was asked.
    metadata = MetaData()
    instructors = Table(
        "instructors",
        metadata,
        Column("director", String(is_title=True)),
        Column("name", String()),
    )
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("advisor", Relation(), ForeignKey("instructors.object_id")),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        advisor_oid = (
            connection.execute(
                insert(instructors)
                .values(director="Vincenzo Galilei", name="Pisa Cathedral")
                .returning(instructors.c.object_id)
            )
            .first()
            .object_id
        )
        connection.execute(
            insert(students).values(
                name="Galileo Galilei",
                advisor=[advisor_oid],
            )
        )

        # Act: project `name` from ONE side only, alongside the other side's
        # `director`. The unprojected `instructors.name` is the collision bait.
        result = connection.execute(
            select(students.c.name, instructors.c.director).join(students.c.advisor)
        )
        row = result.fetchall()[0]

    # Assert: the projected `name` surfaces under its BARE key (no ambiguity to
    # resolve), and is NOT over-qualified to `students.name`.
    keys = set(row.keys())
    assert "name" in keys
    assert "students.name" not in keys
    assert "director" in keys

    m = row.mapping()
    assert m["name"] == "Galileo Galilei"
    assert m["director"] == "Vincenzo Galilei"
