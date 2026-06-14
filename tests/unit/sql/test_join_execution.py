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
