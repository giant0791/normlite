import json
import pdb
import uuid

import pytest

from normlite import Relation, ForeignKey
from normlite.exceptions import ArgumentError, CompileError
from normlite.sql.compiler import NotionCompiler
from normlite.sql.dml import Join, select
from normlite.sql.elements import ColumnElement, OrderByExpression, or_
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import String


@pytest.fixture
def metadata() -> MetaData:
    return MetaData()


@pytest.fixture
def courses(metadata: MetaData) -> Table:
    return Table(
        "courses",
        metadata,
        Column("title", String(is_title=True)),
    )


@pytest.fixture
def students(metadata: MetaData, courses: Table) -> Table:
    table = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )
    # Phase-1 SELECT routes on the queried table's data_source_id under
    # 2025-09-03; reflect it so the "previously reflected" guard passes.
    table._sys_columns["data_source_id"]._value = str(uuid.uuid4())
    return table


def test_join_node_carries_left_right_onclause_and_defaults_to_inner(
    students: Table, courses: Table
):
    join = Join(students, courses, students.c.enrolled_in)

    assert join.left is students
    assert join.right is courses
    assert join.onclause is students.c.enrolled_in
    assert join.isouter is False
    assert join.__visit_name__ == "join"

def test_select_init_accepts_two_tables_without_raising(
    students: Table, courses: Table
):
    stmt = select(students, courses)

    # construction succeeds (no ArgumentError) and the first table
    # remains the "left" driver, reachable through the existing accessor
    assert stmt._table is students

def test_join_is_generative_and_accumulates_join_node(
    students: Table, courses: Table
):
    base = select(students, courses)

    joined = base.join(students.c.enrolled_in)

    # generative: returns a new Select, the receiver is not mutated
    assert joined is not base
    assert len(base._joins) == 0
    assert len(joined._joins) == 1

    # the accumulated Join is wired against the entity list + onclause
    j = joined._joins[0]
    assert isinstance(j, Join)
    assert j.left is students
    assert j.right is courses
    assert j.onclause is students.c.enrolled_in
    assert j.isouter is False

def test_select_with_join_emits_join_metadata_in_compiled_dict(
    students: Table, courses: Table
):
    students._sys_columns["object_id"]._value = str(uuid.uuid4())

    stmt = select(students, courses).join(students.c.enrolled_in)

    nc = NotionCompiler()
    compiled = stmt.compile(nc)
    asdict = compiled.as_dict()

    # bug: this shall not fail
    assert nc._compiler_state.stmt is not None

    # phase-1 portion: same shape as a plain select(students) would produce
    assert asdict['operation'] == {'endpoint': 'data_sources', 'request': 'query'}
    assert asdict['payload']['page_size'] == 100
    # data_sources.query has no in_trash body param (2025-09-03); it is no
    # longer emitted in the payload.
    assert 'in_trash' not in asdict['payload']

    # new: a top-level 'joins' block carries one join entry
    assert 'joins' in asdict
    assert len(asdict['joins']) == 1

    j = asdict['joins'][0]
    assert j['left'] == 'students'
    assert j['right'] == 'courses'
    assert j['onclause'] == 'enrolled_in'
    assert j['isouter'] is False

def test_join_target_table_auto_resolves_single_fk_to_same_compiled_output(
    students: Table, courses: Table
):
    # Seed object_id so phase-1 compiles deterministically
    # (mirrors test_select_with_join_emits_join_metadata_in_compiled_dict).
    students._sys_columns["object_id"]._value = str(uuid.uuid4())

    # explicit-column form (the canonical slice-1 contract)
    explicit = (
        select(students, courses)
        .join(students.c.enrolled_in)
        .compile(NotionCompiler())
        .as_dict()
    )

    # sugar form: hand .join() the target table instead of the column
    sugar = (
        select(students, courses)
        .join(courses)
        .compile(NotionCompiler())
        .as_dict()
    )

    # the sugar must rewrite to the same join and compile identically
    assert sugar == explicit

def test_join_unreferenced_table_raises_argument_error_not_index_error(
    students: Table,
):
    # A table the left table has no foreign key to, built under a SEPARATE
    # MetaData so its name isn't even present in students' metadata. The sugar
    # form must reject it with a clear ArgumentError — not crash with IndexError
    # from indexing an empty relation-column list.
    other_meta = MetaData()
    workshops = Table(
        "workshops",
        other_meta,
        Column("title", String(is_title=True)),
    )

    with pytest.raises(ArgumentError, match=r"not a referenced table"):
        select(students).join(workshops)

def test_join_with_non_relation_column_raises_argument_error(
    students: Table, courses: Table
):
    # students.c.name is a String column, not a Relation — illegal as an onclause
    with pytest.raises(ArgumentError, match=r"Relation"):
        select(students, courses).join(students.c.name)

def test_join_onclause_from_outside_left_table_raises_argument_error(
    metadata: MetaData, students: Table, courses: Table
):
    # A third table with its own Relation column. The column is well-formed
    # (Relation type + ForeignKey), but its parent is `instructors`, not the
    # left table `students` of the select-join under test.
    instructors = Table(
        "instructors",
        metadata,
        Column("name", String(is_title=True)),
        Column("teaches", Relation(), ForeignKey("courses.object_id")),
    )

    with pytest.raises(ArgumentError, match=r"students|left"):
        select(students, courses).join(instructors.c.teaches)

def test_diagnostic_parent_identity(students: Table):
    assert students.c.enrolled_in.parent is students

def test_join_explicit_onclause_with_non_relation_left_raises_argument_error(
    students: Table, courses: Table
):
    # A BinaryExpression whose left side is students.c.name (a String, not a Relation).
    # The join machinery has nothing to traverse from a String column.
    bad_expr = students.c.name == "Galileo"

    with pytest.raises(ArgumentError, match=r"Relation"):
        select(students, courses).join(bad_expr)

def test_two_relations_to_same_target_table_raise_naming_both_columns(
    metadata: MetaData, courses: Table
):
    # Notion's data model forbids two relation properties pointing at the same
    # database. When a schema author declares two such columns, the error must
    # name BOTH colliding columns — otherwise they can't tell which to fix.
    with pytest.raises(ArgumentError, match=r"already has a relation.*courses"):
        Table(
            "enrollments",
            metadata,
            Column("title", String(is_title=True)),
            Column("primary_course", Relation(), ForeignKey("courses.object_id")),
            Column("backup_course", Relation(), ForeignKey("courses.object_id")),
        )

def test_projecting_same_column_twice_raises_argument_error(metadata: MetaData):
    # ADR-0009: slice 2 dropped the name-keyed ColumnCollection so duplicate
    # *names* across tables are constructable (select(a.c.title, b.c.title) is
    # valid and gets qualified by from_join). That reopened a different hole:
    # the SAME (parent, name) projected twice would be qualified into two
    # IDENTICAL keys and silently collapse. So the same column twice is a hard
    # error -- keyed on (parent, name), not on bare name (the cross-table same
    # name must stay legal, slice 2).
    students = Table(
        "students",
        metadata,
        Column("title", String(is_title=True)),
    )

    with pytest.raises(ArgumentError):
        select(students.c.title, students.c.title)


def test_right_side_where_filter_stays_out_of_phase_one_query_payload(
    students: Table, courses: Table
):
    students._sys_columns["object_id"]._value = str(uuid.uuid4())

    # A filter on the RIGHT table (courses). Notion's databases.query can only
    # filter the left table's properties, so this condition must NOT ride along
    # in the phase-1 query payload — it has to be answered client-side instead.
    stmt = (
        select(students, courses)
        .join(students.c.enrolled_in)
        .where(courses.c.title == "Astronomy")
    )

    asdict = stmt.compile(NotionCompiler()).as_dict()

    # phase-1 payload must carry no filter built from a right-table column
    assert "filter" not in asdict["payload"]

def test_where_expression_with_no_source_table_raises_compile_error(students: Table):
    students._sys_columns["object_id"]._value = str(uuid.uuid4())
    class _UnknownExpr(ColumnElement):
        __visit_name__ = "unknown_expr"
    stmt = select(students).where(_UnknownExpr())
    with pytest.raises(CompileError, match=r"route WHERE expression"):
        stmt.compile(NotionCompiler())

def test_right_side_order_by_stays_out_of_phase_one_query_payload(
    students: Table, courses: Table
):
    students._sys_columns["object_id"]._value = str(uuid.uuid4())

    # An ORDER BY on the RIGHT table (courses). Notion's databases.query can only
    # sort by the left table's properties, so this sort must NOT ride along in the
    # phase-1 query payload — it has to be applied client-side after the join.
    stmt = (
        select(students, courses)
        .join(students.c.enrolled_in)
        .order_by(courses.c.title.asc())
    )

    asdict = stmt.compile(NotionCompiler()).as_dict()

    # phase-1 payload must carry no sort built from a right-table column
    assert "sorts" not in asdict["payload"]

def test_order_by_right_then_left_pushes_nothing_into_phase_one(
    students: Table, courses: Table
):
    # ORDER BY with a RIGHT key FIRST. The primary sort key is a right-table
    # property databases.query cannot sort by, so the leading run is empty and
    # NOTHING is pushable. The trailing left key is only a secondary tie-break
    # under a client-side primary, so it cannot ride in phase-1 either. This is
    # the mirror of `ORDER BY left, right` (which pushes the left prefix) — sort
    # routing is positional, NOT set-membership.
    students._sys_columns["object_id"]._value = str(uuid.uuid4())
    asdict = (
        select(students, courses).join(students.c.enrolled_in)
        .order_by(courses.c.title.asc(), students.c.name.asc())
        .compile(NotionCompiler()).as_dict()
    )

    # a leading right-table key stops the prefix immediately: no sorts in phase-1
    assert "sorts" not in asdict["payload"]

def test_order_by_expression_with_no_source_table_raises_compile_error(students: Table):
    students._sys_columns["object_id"]._value = str(uuid.uuid4())
    class _UnknownExpr(ColumnElement):
        __visit_name__ = "unknown_expr"
    # An ORDER BY clause whose sort key is an unattributable node: the router can
    # reach no source table, so it must fail loudly rather than drop the sort.
    stmt = select(students).order_by(OrderByExpression(_UnknownExpr(), "asc"))
    with pytest.raises(CompileError, match=r"route ORDER BY expression"):
        stmt.compile(NotionCompiler())

def test_compound_and_where_pushes_only_left_conjunct_into_phase_one(
      students: Table, courses: Table
  ):
      students._sys_columns["object_id"]._value = str(uuid.uuid4())

      # Compound WHERE = a LEFT (students) conjunct AND a RIGHT (courses) conjunct.
      # The left conjunct only narrows the phase-1 candidate set to a SUPERSET of the
      # final answer, so databases.query may safely carry it; the right conjunct must
      # be answered client-side after the join.
      stmt = (
          select(students, courses)
          .join(students.c.enrolled_in)
          .where(students.c.name == "Galileo")      # LEFT  — pushable
          .where(courses.c.title == "Astronomy")    # RIGHT — client-side only
      )

      asdict = stmt.compile(NotionCompiler()).as_dict()

      phase_one_filter = json.dumps(asdict["payload"].get("filter"))
      held_back = json.dumps(asdict.get("join_right_filter"))

      # phase-1 query carries the left (students.name) condition ...
      assert '"property": "name"' in phase_one_filter
      # ... but NOT the right (courses.title) condition.
      assert '"property": "title"' not in phase_one_filter

      # the right (courses.title) condition is held back for client-side evaluation.
      assert '"property": "title"' in held_back

def test_compound_or_spanning_both_sides_pushes_nothing_into_phase_one(
    students: Table, courses: Table
):
    students._sys_columns["object_id"]._value = str(uuid.uuid4())

    # Compound OR spanning BOTH sides. A row qualifies if EITHER side holds, so
    # narrowing phase-1 to just the left disjunct would silently drop rows that
    # match only the right one — wrong answer, no error. Unlike AND, an OR that
    # touches the right table must stay WHOLLY client-side: push nothing.
    stmt = (
        select(students, courses)
        .join(students.c.enrolled_in)
        .where(or_(students.c.name == "Galileo", courses.c.title == "Astronomy"))
    )

    asdict = stmt.compile(NotionCompiler()).as_dict()

    # phase-1 query carries NO filter at all ...
    assert "filter" not in asdict["payload"]

    # ... and the whole OR is held back for client-side eval, with BOTH disjuncts
    # still present and joined by "or" — i.e. the connective was never split.
    held_back = asdict["join_right_filter"]
    assert "or" in held_back
    assert '"property": "name"' in json.dumps(held_back)
    assert '"property": "title"' in json.dumps(held_back)

def test_order_by_left_then_right_pushes_only_left_prefix_into_phase_one(
    students: Table, courses: Table
):
    students._sys_columns["object_id"]._value = str(uuid.uuid4())

    # ORDER BY with a LEFT key first, a RIGHT key second. Sort pushability is
    # POSITIONAL, not set-membership: only the LEADING RUN of left-table keys can
    # ride in phase-1 (databases.query sorts by left properties only). The primary
    # left key is pushable; the trailing right key must STOP the prefix and be left
    # for a client-side stable sort after the join.
    stmt = (
        select(students, courses)
        .join(students.c.enrolled_in)
        .order_by(students.c.name.asc(), courses.c.title.asc())
    )

    asdict = stmt.compile(NotionCompiler()).as_dict()

    # phase-1 sorts carry EXACTLY the leading left (students.name) key — the
    # right (courses.title) key is NOT pushed.
    assert asdict["payload"].get("sorts") == [
        {"property": "name", "direction": "ascending"}
    ]
