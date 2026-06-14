"""Seam-level tests for the JoinExecution object (issue #314, ADR-0008).

These exercise JoinExecution through its public interface only:
    JoinExecution(join, projection, right_filter)
        .prepare(left_rows)   -> bulk_params
        .assemble(right_rows) -> (merged_schema, merged_rows)

The existing free-function tests (test_join_strategy.py) and the full
pipeline tests stay green and act as the characterization baseline.
"""

import pytest

from normlite import Relation, ForeignKey
from normlite.sql.dml import Join, JoinExecution
from normlite.sql.resultschema import SchemaInfo
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import String


def test_prepare_turns_left_rows_into_deduplicated_retrieve_batch():
    # Arrange: a students->courses join on the Relation FK, and three left
    # rows in raw phase-1 shape. Two students share course "c-astro"; one
    # points at "c-physics"; the third repeats "c-astro" (intra+inter dup).
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
    join = Join(students, courses, students.c.enrolled_in)

    left_schema = SchemaInfo.from_table(
        students,
        execution_names=[students.c.object_id.name],
        projected_names=[c.name for c in students.uc],
    )

    def left_row(name: str, oids: list[str], oid: str) -> tuple:
        cells = [None] * len(left_schema.columns)
        cells[left_schema.column_index("name")] = {"title": name}
        cells[left_schema.column_index("enrolled_in")] = {
            "relation": [{"id": o} for o in oids]
        }
        cells[left_schema.column_index("object_id")] = oid
        return tuple(cells)

    left_rows = [
        left_row("Galileo Galilei", ["c-astro"],              "s-1"),
        left_row("Isaac Newton",    ["c-physics"],            "s-2"),
        left_row("Marie Curie",     ["c-astro", "c-astro"],   "s-3"),
    ]

    # Act: build the seam object from config only, then feed it phase-1 rows.
    join_execution = JoinExecution(join, projection=None, right_filter=None)
    bulk_params = join_execution.prepare(left_rows)

    # Assert: each distinct target id appears exactly once, in first-seen
    # order, wrapped in the path_params envelope phase 2 expects.
    assert bulk_params == [
        {"path_params": {"page_id": "c-astro"}},
        {"path_params": {"page_id": "c-physics"}},
    ]

def test_assemble_merges_the_prepared_left_rows_with_the_retrieved_right_rows():
    # Arrange: a students->courses inner join. Two left rows in raw phase-1
    # shape: "Galileo" enrolled in a real course ("c-astro"), "Phantom"
    # pointing at a dangling id ("c-ghost") with no matching right row. One
    # right row comes back from the phase-2 retrieve ("Astronomy" / "c-astro").
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
    join = Join(students, courses, students.c.enrolled_in)

    # The projection a join select carries: all user columns, left then right.
    projection = [*students.uc, *courses.uc]

    left_schema = SchemaInfo.from_table(
        students,
        execution_names=[students.c.object_id.name],
        projected_names=[c.name for c in students.uc],
    )
    right_schema = SchemaInfo.from_table(
        courses,
        execution_names=[courses.c.object_id.name],
        projected_names=[c.name for c in courses.uc],
    )

    def left_row(name: str, oids: list[str], oid: str) -> tuple:
        cells = [None] * len(left_schema.columns)
        cells[left_schema.column_index("name")] = {"title": name}
        cells[left_schema.column_index("enrolled_in")] = {
            "relation": [{"id": o} for o in oids]
        }
        cells[left_schema.column_index("object_id")] = oid
        return tuple(cells)

    def right_row(title: str, oid: str) -> tuple:
        cells = [None] * len(right_schema.columns)
        cells[right_schema.column_index("title")] = title
        cells[right_schema.column_index("object_id")] = oid
        return tuple(cells)

    left_rows = [
        left_row("Galileo Galilei", ["c-astro"], "s-1"),
        left_row("Phantom Student", ["c-ghost"], "s-2"),  # dangling FK
    ]
    right_rows = [
        right_row("Astronomy", "c-astro"),
    ]

    # Act: prepare seeds the phase-1 state; assemble takes ONLY the phase-2
    # rows and must reuse the left rows captured across the dispatch boundary.
    join_execution = JoinExecution(join, projection=projection, right_filter=None)
    join_execution.prepare(left_rows)
    merged_schema, merged_rows = join_execution.assemble(right_rows)

    # Assert: the joined schema spans both sides in projection order.
    assert tuple(col[0] for col in merged_schema.as_sequence()) == (
        "name",
        "enrolled_in",
        "title",
    )

    # Assert: exactly one merged row — Galileo paired with Astronomy; the
    # dangling "Phantom" left row is dropped (inner-join semantics carried
    # through the seam).
    assert len(merged_rows) == 1
    assert {"title": "Galileo Galilei"} in merged_rows[0]
    assert "Astronomy" in merged_rows[0]
