from datetime import date

import pytest

from normlite import ForeignKey, Relation
from normlite.engine.base import _distill_params
from normlite.exceptions import InvalidRequestError
from normlite.engine.context import ExecutionContext
from normlite.notiondbapi.dbapi2 import Connection as DBAPIConnection
from normlite.sql.dml import insert, select
from normlite.sql.elements import or_
from normlite.sql.queryplan import Filter, HashJoin, Planner, Scan, VolcanoOperator
from normlite.sql.resultschema import SchemaInfo
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Date, String

from tests.utils.db_helpers import (
    create_students_db,
    attach_table_oid,
    populate_students,
)


class _PageCountingClient:
    """Transparent proxy that counts ``data_sources.query`` page pulls.

    Laziness is only observable by counting backend calls — the row totals look
    identical whether the plan drained every page up front or pulled on demand.
    """

    def __init__(self, wrapped):
        self._wrapped = wrapped
        self.query_calls = 0

    def __call__(self, endpoint, request, path_params=None, query_params=None, payload=None):
        if endpoint == "data_sources" and request == "query":
            self.query_calls += 1
        return self._wrapped(endpoint, request, path_params, query_params, payload)

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


def test_scan_yields_the_store_rows_as_a_batch_then_reports_exhaustion(engine, students):
    # Arrange: a real store with 3 rows, and a prepared cursor + compiled payload,
    # built the way phase-1 callers do — but with NOTHING driving the cursor yet.
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=3)

    compiled = select(students).compile(engine._sql_compiler)
    cursor = engine.raw_connection().cursor()
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=cursor,
        compiled=compiled,
        distilled_params=_distill_params(None),
        execution_options={},
    )
    ctx.pre_exec()
    ctx.invoked_stmt._setup_execution(ctx)   # plain select: a no-op today, cursor stays undriven

    scan = Scan(ctx.operation, ctx.parameters)

    # Act: the plan's leaf drives the cursor itself.
    scan.open(cursor)
    first = scan.next()
    second = scan.next()
    scan.close()

    # Assert: all rows arrive as one batch, then exhaustion is signalled.
    assert first is not None
    assert len(first) == 3
    assert second is None


def test_scan_returns_one_notion_page_per_next(engine, students):
    # Arrange: a real store with 150 rows — more than one Notion page. Notion's
    # page size maxes out at 100, so a full scan spans two pages (100 + 50).
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=150)

    compiled = select(students).compile(engine._sql_compiler)
    cursor = engine.raw_connection().cursor()
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=cursor,
        compiled=compiled,
        distilled_params=_distill_params(None),
        execution_options={},
    )
    ctx.pre_exec()
    ctx.invoked_stmt._setup_execution(ctx)

    scan = Scan(ctx.operation, ctx.parameters)

    # Act: the plan drives the cursor and pulls one Notion page per next().
    scan.open(cursor)
    first = scan.next()
    second = scan.next()
    third = scan.next()
    scan.close()

    # Assert: each next() surfaces exactly one page (100, then the remaining 50),
    # then exhaustion. A single drain-all fetch would put all 150 rows in `first`.
    assert first is not None
    assert len(first) == 100
    assert second is not None
    assert len(second) == 50
    assert third is None


def test_scan_pulls_pages_lazily_fetching_only_what_next_demands(engine, students):
    # Arrange: 150 rows across two Notion pages (100 + 50), driven through a proxy
    # that counts how many backend pages have actually been pulled.
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=150)

    compiled = select(students).compile(engine._sql_compiler)
    counting = _PageCountingClient(engine._client)
    cursor = DBAPIConnection(counting).cursor()
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=cursor,
        compiled=compiled,
        distilled_params=_distill_params(None),
        execution_options={},
    )
    ctx.pre_exec()
    ctx.invoked_stmt._setup_execution(ctx)

    scan = Scan(ctx.operation, ctx.parameters)

    # Act: open the scan and pull only the first page.
    scan.open(cursor)
    first = scan.next()

    # Assert: the first page is in hand, but the second has NOT been fetched yet —
    # the plan pulls pages on demand, not all up front. An eager drain in open()
    # would already show 2 page pulls here.
    assert len(first) == 100
    assert counting.query_calls == 1

    # And pulling again does fetch the next page — lazy, not truncated.
    second = scan.next()
    assert len(second) == 50
    assert counting.query_calls == 2


def test_scan_is_recognised_as_a_volcano_operator_but_a_partial_object_is_not(engine, students):
    # The plan drives every node through one uniform contract: open / next / close.
    # Scan already speaks it, so it must be recognised as a VolcanoOperator — while
    # an object missing part of the contract (here, no next()) must NOT be, or the
    # protocol would guarantee nothing.
    scan = Scan(operation={}, parameters={})

    class _MissingNext:
        def open(self, cursor): ...
        def close(self): ...

    assert isinstance(scan, VolcanoOperator)
    assert not isinstance(_MissingNext(), VolcanoOperator)


def test_planner_turns_a_plain_select_into_a_single_scan_that_yields_the_store(engine, students):
    # A plain select has no join and no residual, so its plan is a lone leaf: one
    # Scan over the store. The Planner reads everything the leaf needs off the
    # execution context (the compiled operation and the run-time parameters), and
    # the plan it hands back, when driven, yields exactly the store's rows.
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=3)

    compiled = select(students).compile(engine._sql_compiler)
    cursor = engine.raw_connection().cursor()
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=cursor,
        compiled=compiled,
        distilled_params=_distill_params(None),
        execution_options={},
    )
    ctx.pre_exec()
    ctx.invoked_stmt._setup_execution(ctx)

    # Act: the Planner compiles the statement into a plan.
    plan = Planner(ctx).plan()

    # The plan is a single Scan — a leaf, not a tree.
    assert isinstance(plan, Scan)

    # ... and driving it yields the whole store, then exhaustion.
    plan.open(cursor)
    first = plan.next()
    second = plan.next()
    plan.close()

    assert first is not None
    assert len(first) == 3
    assert second is None


def test_planner_turns_a_join_select_into_a_hashjoin_over_two_scans(engine):
    # A join select is two-phase: a phase-1 data_sources.query over the LEFT
    # store, then a phase-2 pages.retrieve of the RIGHT pages the left rows
    # point at. The Planner must turn that shape into a BINARY plan -- a
    # HashJoin whose two children are the two leaf Scans (left = the phase-1
    # query, right = the phase-2 retrieve) -- NOT the lone Scan a plain select
    # gets, and NOT the None the join branch falls through to today. With no
    # right-side WHERE there is no residual, so the HashJoin is the WHOLE tree:
    # nothing is layered on top of it.
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

    # A join select with NO right-side WHERE, built into a real ExecutionContext
    # the way phase-1 callers do (compile -> bind), but with nothing driving the
    # cursor yet -- the Planner reads the plan off the context, it does not run it.
    stmt = select(students, courses).join(students.c.enrolled_in)
    compiled = stmt.compile(engine._sql_compiler)
    cursor = engine.raw_connection().cursor()
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=cursor,
        compiled=compiled,
        distilled_params=_distill_params(None),
        execution_options={},
    )
    ctx.pre_exec()

    # Act: the Planner compiles the join statement into a plan.
    plan = Planner(ctx).plan()

    # Assert: the top of the plan is a HashJoin -- no residual, so no operator is
    # layered above it -- and its two children are leaf Scans. The left leaf is
    # the phase-1 data_sources.query over the store.
    #
    # (What is pinned here is operator TYPES + parent->child wiring only, not
    #  rows. How the RIGHT leaf sources its retrieve parameters -- which depend
    #  on the left rows -- is the open design point for the driving reds; this
    #  test does not constrain the right leaf's operation, only that it IS a
    #  Scan child.)
    assert isinstance(plan, HashJoin)
    assert isinstance(plan._left_child, Scan)
    assert isinstance(plan._right_child, Scan)
    assert plan._left_child._operation["endpoint"] == "data_sources"


def test_planner_right_leaf_scan_retrieves_the_batch_pages_on_its_own_cursor(engine):
    # The structural sibling above pins the join plan's SHAPE (HashJoin over two
    # Scans) but leaves the right leaf's operation "the open design point for the
    # driving reds". THIS drives it: the right leaf must be a REAL
    # Scan(pages.retrieve), not the placeholder Scan(None, None) the join branch
    # falls through to today.
    #
    # The right leaf is a DEPENDENT scan: its retrieve parameters are the deduped
    # ids the left rows point at, which only exist after the left side drains --
    # so it is driven NOT by open() but by execute_with(batch) (the seam HashJoin
    # already calls). And a join has TWO result sets -- the left
    # data_sources.query and this pages.retrieve -- but a DBAPI cursor carries
    # ONE, so the right leaf must run its retrieve on its OWN cursor. Handed a
    # two-envelope batch for two seeded courses, execute_with must run the
    # EXECUTEMANY retrieve and next() must drain BOTH retrieved pages (one result
    # set per page). Scan(None, None) -- no execute_with, no operation -- cannot.
    #
    # A Scan carries only (operation, parameters): it has NO access to the right
    # table, so it cannot build the cursor description itself. Shaping the cursor
    # is the schema-aware caller's job -- here the arrange phase, standing in for
    # whoever stands up the right leaf in production. The leaf receives its
    # already-shaped cursor via open(); execute_with only supplies the batch and
    # runs it; next() drains.
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

    # Seed two courses so the batch is a real EXECUTEMANY of two retrieves.
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

    # A residual-free join, built into a real ExecutionContext the phase-1 way;
    # the Planner reads the plan off the context, it does not run it.
    stmt = select(students, courses).join(students.c.enrolled_in)
    compiled = stmt.compile(engine._sql_compiler)
    cursor = engine.raw_connection().cursor()
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=cursor,
        compiled=compiled,
        distilled_params=_distill_params(None),
        execution_options={},
    )
    ctx.pre_exec()

    # The Planner builds the plan; its right leaf is the phase-2 retrieve Scan.
    right_leaf = Planner(ctx).plan()._right_child

    # The schema-aware caller shapes the right leaf's OWN cursor (the second
    # result set) and hands it over via open() -- the Scan cannot build this
    # description, having no access to the right table.
    right_schema = SchemaInfo.from_table(
        courses,
        execution_names=[courses.c.object_id.name],
        projected_names=[c.name for c in courses.uc],
    )
    right_cursor = engine.raw_connection().cursor()
    right_cursor._inject_description(right_schema.as_sequence())
    right_leaf.open(right_cursor)
    
    # Act: drive ONLY the right leaf, the parametrised way. The batch stands in
    # for what prepare() would compute from the (undriven) left rows: one
    # path_params envelope per target course id.
    right_leaf.execute_with([
        {"path_params": {"page_id": astronomy_oid}},
        {"path_params": {"page_id": physics_oid}},
    ])
    right_rows = right_leaf.next()
    
    # Assert: both seeded course pages came back on the right leaf's own cursor --
    # two rows, each carrying the object_id of the page it retrieved.
    assert right_rows is not None
    assert len(right_rows) == 2
    assert any(astronomy_oid in row for row in right_rows)
    assert any(physics_oid in row for row in right_rows)


def test_planner_layers_a_filter_carrying_the_residual_over_the_hashjoin(engine):
    # A join select whose WHERE names a RIGHT-side column (courses.title).
    # databases.query cannot answer it in phase-1, so the compiler holds it
    # back as the residual on the PlanningContext. The Planner must honour that
    # residual by layering a Filter ON TOP of the HashJoin (Red 1's whole tree),
    # carrying the predicate as the Notion filter the Filter evaluates client-
    # side -- the AST residual compiled to JSON with its value ("Astronomy")
    # inlined, exactly the shape join_right_filter used to travel as.
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

    stmt = (
        select(students, courses)
        .join(students.c.enrolled_in)
        .where(courses.c.title == "Astronomy")
    )
    compiled = stmt.compile(engine._sql_compiler)
    cursor = engine.raw_connection().cursor()
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=cursor,
        compiled=compiled,
        distilled_params=_distill_params(None),
        execution_options={},
    )
    ctx.pre_exec()

    # Act: the Planner compiles the join-with-residual into a plan.
    plan = Planner(ctx).plan()

    # Assert: the top of the plan is a Filter (NOT the bare HashJoin a
    # residual-free join gets), its source is the HashJoin, and its predicate
    # is the residual as inlined Notion JSON.
    assert isinstance(plan, Filter)
    assert isinstance(plan._source, HashJoin)
    assert plan._filter == {"property": "title", "title": {"equals": "Astronomy"}}


def test_planner_processes_the_residual_value_the_same_way_a_bindparam_would(engine):
    # The residual travels as AST and the Planner renders it to inlined Notion
    # JSON. Rendering must apply the column type's filter_value_processor()
    # exactly as ExecutionContext._resolve_bindparam would for a COLUMN_FILTER
    # bind -- otherwise the inlined value is the RAW Python object, and the
    # Notion filter silently carries the wrong shape.
    #
    # String's processor is None, so the existing residual test (title ==
    # "Astronomy") can't see this: a raw pass-through and a processed value
    # look identical. Date HAS a real processor (date -> ISO string), so a
    # right-side Date residual forces the question: the Filter's predicate must
    # carry "2026-01-01", NOT a bare datetime.date object.
    metadata = MetaData()
    courses = Table(
        "courses",
        metadata,
        Column("title", String(is_title=True)),
        Column("start_date", Date()),
    )
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )
    metadata.create_all(engine)

    stmt = (
        select(students, courses)
        .join(students.c.enrolled_in)
        .where(courses.c.start_date.after(date(2026, 1, 1)))
    )
    compiled = stmt.compile(engine._sql_compiler)
    cursor = engine.raw_connection().cursor()
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=cursor,
        compiled=compiled,
        distilled_params=_distill_params(None),
        execution_options={},
    )
    ctx.pre_exec()

    # Act: the Planner compiles the join-with-Date-residual into a plan.
    plan = Planner(ctx).plan()

    # Assert: the residual's value is processed to Notion's ISO date string,
    # not left as the raw date object.
    assert isinstance(plan, Filter)
    assert plan._filter == {"property": "start_date", "date": {"after": "2026-01-01"}}


def test_planner_rejects_a_compound_residual_loudly_instead_of_crashing(engine):
    # A compound OR spanning both join sides is held back WHOLE as the residual
    # (see test_compound_or_spanning_both_sides in test_join_compilation): the
    # residual is a BooleanClauseList (.operator / .clauses), not a single
    # BinaryExpression. The Planner's residual renderer reaches for
    # residual_where.column / .operator / .value -- attributes a
    # BooleanClauseList does not have -- so today it dies with a bare, opaque
    # AttributeError deep inside _compile_type_filter.
    #
    # Compound residuals are out of scope for this slice, but the boundary must
    # be HANDLED, not stumbled into: the Planner must reject a non-single-binary
    # residual loudly, with a breadcrumb naming the limitation and the issue,
    # rather than leaking an AttributeError. (Carrying the AST to the Filter and
    # rendering compounds at the edge is the ADR-0019 slice-2 endpoint.)
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

    stmt = (
        select(students, courses)
        .join(students.c.enrolled_in)
        .where(or_(students.c.name == "Galileo", courses.c.title == "Astronomy"))
    )
    compiled = stmt.compile(engine._sql_compiler)
    cursor = engine.raw_connection().cursor()
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=cursor,
        compiled=compiled,
        distilled_params=_distill_params(None),
        execution_options={},
    )
    ctx.pre_exec()

    # Act + Assert: planning fails loudly with a single-binary breadcrumb, not a
    # bare AttributeError.
    with pytest.raises(InvalidRequestError, match="single-binary"):
        Planner(ctx).plan()
