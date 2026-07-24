import pdb
import uuid

import pytest

from normlite import Relation, ForeignKey
from normlite.engine.base import Engine
from normlite.sql.compiler import NotionCompiler
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


def test_right_side_filter_on_colliding_column_with_extra_right_column_survives(engine: Engine):
    # Arrange: both tables declare `title` (the collision). courses also has a
    # SECOND, non-colliding right column (`code`). This is the sub-case that,
    # before ADR-0009, raised ValueError instead of silently over-dropping:
    # with another right column present the synthetic page was keyed by the
    # qualified name, so the bare-named compiled filter could not find `title`.
    metadata = MetaData()
    courses = Table(
        "courses",
        metadata,
        Column("title", String(is_title=True)),
        Column("code", String()),
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
                .values(title="Astronomy", code="ASTRO-101")
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

        # Act: right-side filter on the colliding courses.title, with the
        # non-colliding courses.code also in the projection (full expansion).
        result = connection.execute(
            select(students, courses)
            .join(students.c.enrolled_in)
            .where(courses.c.title.is_not_empty())
        )
        rows = result.fetchall()

    # Assert: no ValueError; the genuine match survives, both colliding titles
    # are reachable under their qualified keys, and the extra right column
    # carries its bare name and value.
    assert len(rows) == 1
    m = rows[0].mapping()
    assert m["students.title"] == "Galileo Galilei"
    assert m["courses.title"] == "Astronomy"
    assert m["code"] == "ASTRO-101"


def test_right_side_filter_on_colliding_column_drops_non_matching_row(engine: Engine):
    # Arrange: same collision as the survival tests (both tables declare
    # `title`), but the right-side predicate is one the genuine match FAILS.
    # If the fix were an "always keep" cheat, this row would wrongly survive.
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

        # Act: filter the colliding courses.title against a value the matched
        # course ("Astronomy") does NOT have.
        result = connection.execute(
            select(students, courses)
            .join(students.c.enrolled_in)
            .where(courses.c.title == "Chemistry")
        )
        rows = result.fetchall()

    # Assert: the predicate discriminates -- the non-matching row is dropped.
    assert rows == []


def test_right_side_filter_on_outer_join_drops_phantom_under_collision(engine: Engine):
    # Arrange: the outer-join phantom-drop scenario (ADR-0005) under a left/right
    # `title` collision. Phantom Student points at a dangling id (all-None right
    # slice); Galileo is a genuine match. A right-side WHERE on the colliding
    # courses.title must drop the None-filled phantom and keep the real match --
    # provenance-based getter selection must not break the all-None phantom guard.
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
                title="Phantom Student",
                enrolled_in=[bogus_course_oid],
            )
        )
        connection.execute(
            insert(students).values(
                title="Galileo Galilei",
                enrolled_in=[astronomy_oid],
            )
        )

        # Act: outer join, then a right-side filter on the colliding courses.title.
        result = connection.execute(
            select(students, courses)
            .outerjoin(students.c.enrolled_in)
            .where(courses.c.title.is_not_empty())
        )
        rows = result.fetchall()

    # Assert: phantom dropped, genuine match kept, with both titles under their
    # qualified keys carrying their own side's value.
    assert len(rows) == 1
    m = rows[0].mapping()
    assert m["students.title"] == "Galileo Galilei"
    assert m["courses.title"] == "Astronomy"


def test_join_projects_explicit_colliding_columns_from_both_tables(engine: Engine):
    # Arrange: two FK-linked tables that BOTH declare `title`. Unlike the
    # full-table-expansion collision test, here the collision is asked for
    # EXPLICITLY in the projection -- select(students.c.title, courses.c.title).
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

        # Act: explicitly project the colliding `title` from each side.
        result = connection.execute(
            select(students.c.title, courses.c.title).join(students.c.enrolled_in)
        )
        row = result.fetchall()[0]

    # Assert: both projected `title`s are reachable under table-qualified keys,
    # each carrying its own side's value -- no bare-`title` shadow collapse.
    m = row.mapping()
    assert m["students.title"] == "Galileo Galilei"
    assert m["courses.title"] == "Astronomy"


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


def test_inner_join_order_by_left_then_right_breaks_ties_by_right_key(engine: Engine):
    # ORDER BY students.name, courses.title. The LEFT key is primary (pushed to
    # phase-1); the RIGHT key is the tie-break, which can only be applied
    # client-side AFTER the join. One student enrolled in two courses yields two
    # same-name pairs, so the trailing right key alone decides their order.
    #
    # The relation list is seeded in the REVERSE of title order, so the correct
    # result cannot be the incidental retrieval/merge order — it must be ACTIVELY
    # ordered by the trailing right key.
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
        physics_oid = (
            connection.execute(
                insert(courses).values(title="Physics").returning(courses.c.object_id)
            ).first().object_id
        )
        astronomy_oid = (
            connection.execute(
                insert(courses).values(title="Astronomy").returning(courses.c.object_id)
            ).first().object_id
        )

        # relation list deliberately in REVERSE title order: Physics before Astronomy
        connection.execute(
            insert(students).values(
                name="Galileo",
                enrolled_in=[physics_oid, astronomy_oid],
            )
        )

        result = connection.execute(
            select(students, courses)
            .join(students.c.enrolled_in)
            .order_by(students.c.name.asc(), courses.c.title.asc())
        )
        rows = result.fetchall()

    # both pairs share the name, so the trailing right key (title) orders them:
    # Astronomy precedes Physics ascending.
    assert [r.title for r in rows] == ["Astronomy", "Physics"]


def test_join_with_right_order_by_sorts_with_residual_off_the_compiled_dict(
    engine: Engine,
):
    # Mirror of the WHERE residual pipeline test: a right-side ORDER BY tie-break
    # is held back and applied client-side. It must keep sorting correctly while
    # the held-back sort no longer travels as join_right_sorts on the compiled
    # dict — the residual is sourced from the PlanningContext AST instead.
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
        physics_oid = (
            connection.execute(
                insert(courses).values(title="Physics").returning(courses.c.object_id)
            ).first().object_id
        )
        astronomy_oid = (
            connection.execute(
                insert(courses).values(title="Astronomy").returning(courses.c.object_id)
            ).first().object_id
        )

        # relation list deliberately in REVERSE title order
        connection.execute(
            insert(students).values(
                name="Galileo",
                enrolled_in=[physics_oid, astronomy_oid],
            )
        )

        # A statement is single-shot: compiling assigns bind roles, so executing and
        # inspecting need one fresh statement each.
        def title_tiebreak_join():
            return (
                select(students, courses)
                .join(students.c.enrolled_in)
                .order_by(students.c.name.asc(), courses.c.title.asc())
            )

        # Act: the held-back right key still actively orders the pairs ...
        rows = connection.execute(title_tiebreak_join()).fetchall()

        # ... while the residual sort no longer travels on the compiled dict.
        compiled_dict = title_tiebreak_join().compile(NotionCompiler()).as_dict()

    # Assert: Astronomy precedes Physics ascending — the trailing right key ordered them.
    assert [r.title for r in rows] == ["Astronomy", "Physics"]

    # Assert: the residual sort is gone from the compiled dict, which stays JSON-like data.
    assert "join_right_sorts" not in compiled_dict


def test_inner_join_order_by_left_then_right_breaks_ties_by_right_key_descending(engine: Engine):
    # Same shape as the ascending tie-break above, but the trailing right key is
    # DESCENDING. The held-back right key is applied client-side AFTER the join,
    # so this pins that the client-side sort honors direction (reverse), not just
    # that it sorts at all. Empties placement is NOT asserted here: an empty/None
    # right-side title is unconstructable through the public interface (insert
    # title=None is rejected by the client; title="" is non-empty), so there is
    # no black-box input to exercise it. See ADR-0005 / the unreachable-empty-
    # title boundary.
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
        physics_oid = (
            connection.execute(
                insert(courses).values(title="Physics").returning(courses.c.object_id)
            ).first().object_id
        )
        astronomy_oid = (
            connection.execute(
                insert(courses).values(title="Astronomy").returning(courses.c.object_id)
            ).first().object_id
        )

        # relation list seeded in ASCENDING title order, so the DESC result
        # cannot be the incidental retrieval/merge order.
        connection.execute(
            insert(students).values(
                name="Galileo",
                enrolled_in=[astronomy_oid, physics_oid],
            )
        )

        result = connection.execute(
            select(students, courses)
            .join(students.c.enrolled_in)
            .order_by(students.c.name.asc(), courses.c.title.desc())
        )
        rows = result.fetchall()

    # both pairs share the name, so the trailing right key (title) orders them:
    # Physics precedes Astronomy descending.
    assert [r.title for r in rows] == ["Physics", "Astronomy"]

def test_join_with_parameterised_right_side_where_executes_with_residual_off_the_compiled_dict(
    engine: Engine,
):
    # Arrange: two courses, one student enrolled in each. The WHERE names a RIGHT-side
    # column with a *bind parameter* (courses.title == "Astronomy"), so it cannot be
    # answered by phase-1 and must be held back for client-side evaluation.
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
                insert(courses).values(title="Astronomy").returning(courses.c.object_id)
            )
            .first()
            .object_id
        )
        physics_oid = (
            connection.execute(
                insert(courses).values(title="Physics").returning(courses.c.object_id)
            )
            .first()
            .object_id
        )

        connection.execute(
            insert(students).values(name="Galileo Galilei", enrolled_in=[astronomy_oid])
        )
        connection.execute(
            insert(students).values(name="Isaac Newton", enrolled_in=[physics_oid])
        )

        # A statement is single-shot: compiling assigns bind roles, so executing and
        # inspecting need one fresh statement each.
        def astronomy_join():
            return (
                select(students, courses)
                .join(students.c.enrolled_in)
                .where(courses.c.title == "Astronomy")
            )

        # Act: the join still answers correctly ...
        rows = connection.execute(astronomy_join()).fetchall()

        # ... while the residual no longer travels on the compiled dict.
        compiled_dict = astronomy_join().compile(NotionCompiler()).as_dict()

    # Assert: the held-back predicate was honoured — only the Astronomy pair survives.
    assert {(row.name, row.title) for row in rows} == {("Galileo Galilei", "Astronomy")}

    # Assert: the residual is gone from the compiled dict, which stays JSON-like data.
    assert "join_right_filter" not in compiled_dict
