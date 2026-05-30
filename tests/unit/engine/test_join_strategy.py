import pdb

import pytest

from normlite import Relation, ForeignKey
from normlite.sql.dml import build_phase_two_batch
from normlite.sql.resultschema import SchemaInfo
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import String


def test_build_phase_two_batch_dedups_target_ids_preserving_first_seen_order():
    # Arrange: a left schema with a Relation FK column whose values are
    # `list[str]` of target page ids — exactly the shape phase-1 leaves
    # in the cursor after fetchall().
    metadata = MetaData()
    Table(
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
    onclause = students.c.enrolled_in

    left_schema = SchemaInfo.from_table(
        students,
        execution_names=[students.c.object_id.name],
        projected_names=[c.name for c in students.uc],
    )

    # Three students. Two share course "c-astro"; one points at "c-physics".
    # The third row repeats "c-astro" again (intra-row + inter-row dup).
    name_idx = left_schema.column_index("name")
    enrolled_idx = left_schema.column_index("enrolled_in")
    object_id_idx = left_schema.column_index("object_id")

    def row(name: str, oids: list[str], oid: str) -> tuple:
        cells = [None] * len(left_schema.columns)
        cells[name_idx] = {"title": name}
        cells[enrolled_idx] = {"relation": oids}
        cells[object_id_idx] = oid
        return tuple(cells)

    left_rows = [
        row("Galileo Galilei", [{"id":"c-astro"}],                     "s-1"),
        row("Isaac Newton",    [{"id": "c-physics"}],                  "s-2"),
        row("Marie Curie",     [{"id": "c-astro"}, {"id": "c-astro"}], "s-3"),
    ]

    # Act
    batch = build_phase_two_batch(left_schema, onclause, left_rows)

    # Assert: each distinct id appears exactly once, in first-seen order,
    # wrapped in the path_params envelope phase-2 expects.
    assert batch == [
        {"path_params": {"page_id": "c-astro"}},
        {"path_params": {"page_id": "c-physics"}},
    ]

from normlite.sql.dml import merge_inner_join_rows


def test_merge_inner_join_emits_one_row_per_left_to_right_match_dropping_unmatched_left_rows():
    # Arrange: two students, two courses. Student "Galileo" enrolled in
    # one real course; student "Phantom" points at an id with no matching
    # right row (dangling FK — must be dropped, inner-join semantics).
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
    onclause = students.c.enrolled_in

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

    # left rows are still in raw phase-1 shape (Relation cell is dict-list)
    def left_row(name: str, oids: list[str], oid: str) -> tuple:
        cells = [None] * len(left_schema.columns)
        cells[left_schema.column_index("name")] = {"title": name}
        cells[left_schema.column_index("enrolled_in")] = {
            "relation": [{"id": o} for o in oids]
        }
        cells[left_schema.column_index("object_id")] = oid
        return tuple(cells)

    # right rows are post-phase-2 retrieve, already in tuple shape that
    # column_getter("object_id") can index — match what context._staged_result_cursor
    # yields via _iter_all().
    def right_row(title: str, oid: str) -> tuple:
        cells = [None] * len(right_schema.columns)
        cells[right_schema.column_index("title")] = title
        cells[right_schema.column_index("object_id")] = oid
        return tuple(cells)

    left_rows = [
        left_row("Galileo Galilei", ["c-astro"],  "s-1"),
        left_row("Phantom Student", ["c-ghost"],  "s-2"),  # dangling
    ]
    right_rows = [
        right_row("Astronomy", "c-astro"),
    ]

    # Act
    merged = merge_inner_join_rows(
        left_schema, right_schema, onclause, left_rows, right_rows
    )

    # Assert: exactly one merged row (Phantom's dangling FK dropped).
    # The merged tuple is (left projected ⊕ right projected) minus object_id
    # from each side — matching the existing _project_inner_join shape.
    assert len(merged) == 1
    assert {"title": "Galileo Galilei"} in merged[0]
    assert "Astronomy" in merged[0]
    

    # Assert: exactly one merged row (Phantom's dangling FK dropped).

    # Assert: exactly one merged row (Phantom's dangling FK dropped).
    # The merged tuple is (left projected ⊕ right projected) minus object_id
    # from each side — matching the existing _project_inner_join shape.
    assert len(merged) == 1
    assert {"title": "Galileo Galilei"} in merged[0]
    # The merged tuple is (left projected ⊕ right projected) minus object_id
    # from each side — matching the existing _project_inner_join shape.
    assert len(merged) == 1
    assert {"title": "Galileo Galilei"} in merged[0]
    assert len(merged) == 1
    assert {"title": "Galileo Galilei"} in merged[0]
    assert {"title": "Galileo Galilei"} in merged[0]
    assert "Astronomy" in merged[0]
