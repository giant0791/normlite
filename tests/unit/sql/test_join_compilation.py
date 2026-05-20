import uuid

import pytest

from normlite import Relation, ForeignKey
from normlite.exceptions import ArgumentError
from normlite.sql.compiler import NotionCompiler
from normlite.sql.dml import Join, select
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
    return Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )


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

    # phase-1 portion: same shape as a plain select(students) would produce
    assert asdict['operation'] == {'endpoint': 'databases', 'request': 'query'}
    assert asdict['payload']['page_size'] == 100
    assert asdict['payload']['in_trash'] is False

    # new: a top-level 'joins' block carries one join entry
    assert 'joins' in asdict
    assert len(asdict['joins']) == 1

    j = asdict['joins'][0]
    assert j['left'] == 'students'
    assert j['right'] == 'courses'
    assert j['onclause'] == 'enrolled_in'
    assert j['isouter'] is False

def test_execute_with_join_raises_notimplementederror_until_slice_2(
    students: Table, courses: Table
):
    # plain select must remain executable (no-op setup)
    plain = select(students)
    plain._setup_execution(None)  # no raise

    # select with a join must fail loudly with a pointer to slice 2 / #304
    joined = select(students, courses).join(students.c.enrolled_in)
    with pytest.raises(NotImplementedError, match=r"slice 2|#304"):
        joined._setup_execution(None)

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