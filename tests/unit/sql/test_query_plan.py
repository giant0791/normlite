from datetime import date

import pytest

from normlite import ForeignKey, Relation
from normlite.engine.base import _distill_params
from normlite.exceptions import InvalidRequestError
from normlite.engine.context import ExecutionContext
from normlite.notiondbapi.dbapi2 import Connection as DBAPIConnection
from normlite.sql.dml import select
from normlite.sql.elements import or_
from normlite.sql.functions import func
from normlite.sql.queryplan import (
    Aggregate,
    Filter,
    HashJoin,
    Planner,
    Project,
    Scan,
    VolcanoOperator,
)
from normlite.sql.resultschema import SchemaInfo
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Date, Integer, String

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
    # Arrange: a real store with 3 rows and the compiled phase-1 payload. The plan
    # leaf now takes the DBAPI *connection* and mints its own cursor off it (#364),
    # so nothing outside the leaf shapes or drives a cursor.
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=3)

    compiled = select(students).compile(engine._sql_compiler)
    connection = engine.raw_connection()
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=connection.cursor(),
        compiled=compiled,
        distilled_params=_distill_params(None),
        execution_options={},
    )
    ctx.pre_exec()
    ctx.invoked_stmt._setup_execution(ctx)   # plain select: a no-op today

    schema = SchemaInfo.from_table(
        students,
        execution_names=compiled.fetch_columns(),
        projected_names=compiled.result_columns(),
    )
    scan = Scan(ctx.operation, ctx.parameters, schema=schema)

    # Act: the leaf mints its own cursor off the connection and drives it.
    scan.open(connection)
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
    connection = engine.raw_connection()
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=connection.cursor(),
        compiled=compiled,
        distilled_params=_distill_params(None),
        execution_options={},
    )
    ctx.pre_exec()
    ctx.invoked_stmt._setup_execution(ctx)

    schema = SchemaInfo.from_table(
        students,
        execution_names=compiled.fetch_columns(),
        projected_names=compiled.result_columns(),
    )
    scan = Scan(ctx.operation, ctx.parameters, schema=schema)

    # Act: the leaf mints its own cursor and pulls one Notion page per next().
    scan.open(connection)
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
    connection = DBAPIConnection(counting)
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=connection.cursor(),
        compiled=compiled,
        distilled_params=_distill_params(None),
        execution_options={},
    )
    ctx.pre_exec()
    ctx.invoked_stmt._setup_execution(ctx)

    schema = SchemaInfo.from_table(
        students,
        execution_names=compiled.fetch_columns(),
        projected_names=compiled.result_columns(),
    )
    scan = Scan(ctx.operation, ctx.parameters, schema=schema)

    # Act: the leaf mints its cursor off the counting connection and pulls only
    # the first page.
    scan.open(connection)
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


def test_the_plan_batches_by_yield_per_not_by_the_notion_page_maximum(engine, students):
    # #384 / ADR-0022 step 2b, parts 2 and 3. `yield_per` must reach the wire AND
    # size the leaf's batch; this is the only test that can see either.
    #
    # The two Scan tests above are BLIND to this, and not by oversight: they use
    # 150 rows, so the backend splits at 100 + 50 and `fetchmany(100)` lines up
    # with one Notion page BY COINCIDENCE. Under that arithmetic a leaf that
    # ignored `yield_per` completely still looks perfectly lazy.
    #
    # Measured (session 21), through the real machinery with routing flipped and
    # `yield_per=2` over 6 rows, BEFORE parts 2 and 3:
    #
    #     PROBE first plan.next() -> 6 rows
    #     PROBE total rows=6 backend pages=3
    #
    # The FIRST batch returned the whole data source. Two independent causes, and
    # this test fences off both:
    #   - part 2 missing: `yield_per` never reaches `Cursor.execute`, so the
    #     request keeps `page_size` 100 and all 6 rows arrive in one page.
    #   - part 3 missing: `Scan.next()` asks `fetchmany(100)` regardless, and
    #     `fetchmany` pulls FORWARD across page boundaries to satisfy its `n`
    #     (dbapi2.py:639-646) -- so the leaf drains all 3 pages into one batch.
    #
    # That second cause is why the old handoff's "Scan is internally streaming
    # already; the loss is at the drain, not in the leaf" was false. The leaf
    # drained too.
    #
    # MUTATION-PROVEN, and the two causes are caught by DIFFERENT assertions:
    #   - part 3 reverted (`fetchmany(NOTION_MAX_PAGE_SIZE)`) -> `assert 6 == 2`
    #     on the first batch, the probe number above.
    #   - part 2 reverted (`open()` drops `yield_per`) -> the first THREE
    #     assertions still pass. One backend page holds all 6 rows, so a 2-row
    #     `fetchmany` is served from the buffer and looks perfectly lazy. Only
    #     the LAST assertion fails (`assert 1 == 2`): a second batch served from
    #     that same buffer never pulls a second page.
    # So the "pull again" block below is load-bearing. Delete it and this test
    # stops seeing part 2 entirely.
    #
    # Driven through `Planner.plan()` rather than a hand-built `Scan` on purpose:
    # that covers the CASCADE (part 2's `ctx.execution_options` -> `Scan`) as
    # well as the granularity (part 3), which a direct `Scan(yield_per=2)` would
    # skip. It does NOT go through `conn.execute`, so it stays readable while the
    # eager drain in `_execute_query_plan` is still there to be removed by part 4.
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=6)

    # Seeding is done; only the read below is counted.
    compiled = select(students).compile(engine._sql_compiler)
    counting = _PageCountingClient(engine._client)
    connection = DBAPIConnection(counting)
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=connection.cursor(),
        compiled=compiled,
        distilled_params=_distill_params(None),
        execution_options={"yield_per": 2},
    )
    ctx.pre_exec()
    ctx.invoked_stmt._setup_execution(ctx)

    plan = Planner(ctx).plan()

    # Act: drive one batch out of the plan.
    plan.open(connection)
    first = plan.next()

    # The batch is `yield_per` wide, not the whole store: 2 rows, not 6.
    assert len(first) == 2
    # ... and it cost exactly one backend page. 3 here would mean the leaf pulled
    # forward across every page to fill a 100-row fetch -- the session-21 probe.
    assert counting.query_calls == 1

    # Pulling again advances one page further: lazy, not truncated at page 1.
    second = plan.next()
    assert len(second) == 2
    assert counting.query_calls == 2

    plan.close()


def test_scan_is_recognised_as_a_volcano_operator_but_a_partial_object_is_not(engine, students):
    # The plan drives every node through one uniform contract: open / next / close.
    # Scan already speaks it, so it must be recognised as a VolcanoOperator — while
    # an object missing part of the contract (here, no next()) must NOT be, or the
    # protocol would guarantee nothing.
    scan = Scan(operation={}, parameters={}, schema=SchemaInfo(columns=[]))

    class _MissingNext:
        def open(self, connection): ...
        def close(self): ...

    assert isinstance(scan, VolcanoOperator)
    assert not isinstance(_MissingNext(), VolcanoOperator)


class _RecordingCursor:
    """A DBAPI-cursor stand-in that records how the leaf shapes and drives it."""
    def __init__(self):
        self.injected = None
        self.executed = None

    def _inject_description(self, entries):
        self.injected = entries

    def execute(self, operation, parameters, *, stream_results=False, yield_per=None):
        self.executed = (operation, parameters, stream_results)
        return self


class _RecordingConnection:
    """A DBAPI-connection stand-in that hands out (and records) recording cursors."""
    def __init__(self):
        self.minted = []

    def cursor(self):
        cur = _RecordingCursor()
        self.minted.append(cur)
        return cur


def test_scan_open_mints_its_own_cursor_from_the_connection_and_shapes_it_with_its_schema(students):
    # Decision #5 (#364) reverses the schema-blind "receive an already-shaped
    # cursor" scaffold: a Scan now carries its OWN schema, and open() takes the
    # DBAPI *Connection* the engine hands down — not a cursor. The leaf mints its
    # own cursor off that connection (one cursor = one result set; a Join's two
    # leaves need two), shapes it with its schema's description (the Scan owns
    # this now, no schema-aware parent doing it from the outside), and — as an
    # EXECUTE / left leaf — drives the query eagerly with streaming on.
    schema = SchemaInfo.from_table(
        students,
        execution_names=[students.c.object_id.name],
        projected_names=[c.name for c in students.uc],
    )
    operation = {"endpoint": "data_sources", "request": "query"}
    parameters = {"payload": {"filter": {}}}
    scan = Scan(operation, parameters, schema=schema)

    conn = _RecordingConnection()

    # Act: the leaf opens itself against the CONNECTION, not a cursor.
    scan.open(conn)

    # Assert: it minted exactly one cursor of its own off the connection...
    assert len(conn.minted) == 1
    minted = conn.minted[0]
    # ...shaped that cursor with its own schema's description...
    assert minted.injected == schema.as_sequence()
    # ...and eagerly drove the phase-1 query on it, streaming.
    assert minted.executed == (operation, parameters, True)


def test_planner_turns_a_plain_select_into_a_project_over_a_single_scan(engine, students):
    # A plain select has no join and no residual, so its plan is a two-node stem:
    # one Scan over the store, with a Project above it owning the result schema
    # (ADR-0022 step 2). The Planner reads everything the leaf needs off the
    # execution context (the compiled operation and the run-time parameters), and
    # the plan it hands back, when driven, yields exactly the store's rows.
    #
    # This test previously asserted `isinstance(plan, Scan)` and called the plan
    # "a lone leaf, not a tree". Wiring Project deliberately made that false. It
    # is NOT the movement ADR-0022 §2 says to stop on: that rule fires when the
    # trimmed schema fails to reproduce what the root advertised, and the root
    # schema is MEASURED identical before and after the wiring, on every non-join
    # shape (`select(t)`, `select(t.c.name)`, `select(t.c.object_id)`,
    # `select(oid, name)`). What moved was a claim about plan STRUCTURE, which
    # the settled fork changed on purpose.
    #
    # Today Project trims NOTHING -- superset == projection, because nothing
    # widens `fetch_columns` until step 4 adds the recheck's predicate columns.
    # The schema-identity assertion below pins exactly that no-op, and it is
    # meant to be the line that changes when step 4 lands: once the leaf is
    # widened, the root and the leaf stop agreeing, and that divergence IS the
    # feature.
    #
    # HISTORY, because this comment used to say the opposite and the reversal is
    # the point of step 2b. Until `context.py` was flipped, this test was the ONLY
    # thing guarding the non-join branch: a plain SELECT did not reach the Planner
    # at all. Routing sent a statement to EXECUTEQUERYPLAN only `if stmt.is_select
    # and (stmt._joins or stmt._is_aggregate)`; everything else took
    # ExecutionStyle.EXECUTE and `_execute_single`. Measured with a spy on
    # `Planner.plan`: `select(t)` and `select(t).where(t.c.effort != 5)` -- #384's
    # own repro shape -- gave ZERO invocations, `select(func.count())` gave one.
    # The branch wired above was dead in production, and trimming it to the wrong
    # column list broke no pipeline test whatsoever.
    #
    # That was the unnamed PREREQUISITE for ADR-0022 step 4: "wire the recheck on
    # the scan path" cannot work while the engine never executes that path for
    # exactly the statement #384 is about. Step 2b closed it -- routing is now
    # `if stmt.is_select`, and `test_a_plain_select_is_driven_through_the_query_plan`
    # (tests/unit/engine/test_context.py) is what pins it, because the streaming
    # tests are green on BOTH sides of the flip and cannot tell the two apart.
    #
    # So this test no longer stands alone, but it still owns the branch's SHAPE
    # (Project over Scan) and the schema identity below, which the routing test
    # says nothing about.
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=3)

    compiled = select(students).compile(engine._sql_compiler)
    connection = engine.raw_connection()
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=connection.cursor(),
        compiled=compiled,
        distilled_params=_distill_params(None),
        execution_options={},
    )
    ctx.pre_exec()
    ctx.invoked_stmt._setup_execution(ctx)

    # Act: the Planner compiles the statement into a plan.
    plan = Planner(ctx).plan()

    # The plan is a Project over a Scan — the leaf still does the I/O, the root
    # owns what the user sees. Both nodes are pinned: asserting only the root
    # would stop covering that the leaf is a Scan at all.
    assert isinstance(plan, Project)
    assert isinstance(plan._source, Scan)

    # The trim is an IDENTITY today: the root advertises exactly what the leaf
    # does, which is what makes wiring Project in step 2 behaviour-neutral.
    assert plan.result_schema.as_sequence() == plan._source.result_schema.as_sequence()

    # ... and driving it (the leaf mints its own cursor off the connection)
    # yields the whole store, then exhaustion.
    plan.open(connection)
    first = plan.next()
    second = plan.next()
    plan.close()

    assert first is not None
    assert len(first) == 3
    assert second is None


def test_planner_turns_an_aggregate_select_into_an_aggregate_over_a_scan(engine):
    # An aggregate select (`select(func.sum(headcount))`) has no join, so today it
    # falls through the Planner's `if not _joins:` branch and gets the lone Scan a
    # plain select gets -- the reduction lives OUTSIDE the plan, in
    # Select._finalize_execution's `if self._is_aggregate:` hook (#362 retires it).
    #
    # This slice moves the reduction INTO the operator tree: the Planner must build
    # a BLOCKING Aggregate ON TOP of the phase-1 Scan -- NOT the bare Scan a plain
    # select gets -- so the engine drives aggregates through EXECUTEQUERYPLAN just
    # like joins. Structural red: `plan` is a Scan today, must become an Aggregate.
    #
    # THE LOCKSTEP (dml.py:1037-1040): reduce() indexes operands by position into
    # the drained row, so the Scan UNDER the Aggregate must inject the compiler's
    # aggregate fetch_columns as its execution_names (the operand column, here
    # "headcount") -- exactly as the plain-select Scan does. Pinned below.
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("team", String(is_title=True)),
        Column("headcount", Integer()),
    )
    metadata.create_all(engine)

    # An aggregate select, built into a real ExecutionContext the phase-1 way; the
    # Planner reads the plan off the context, it does not run it.
    stmt = select(func.sum(accounts.c.headcount))
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

    # Act: the Planner compiles the aggregate select into a plan.
    plan = Planner(ctx).plan()

    # Assert (shape): the top of the plan is an Aggregate wrapping a single leaf
    # Scan -- the phase-1 data_sources.query over the store.
    assert isinstance(plan, Aggregate)
    assert isinstance(plan._source, Scan)
    assert plan._source._operation["endpoint"] == "data_sources"

    # Assert (result schema): the Aggregate surfaces the aggregate key ("sum") via
    # from_aggregate, tying the plan node to THIS aggregate select.
    assert [entry[0] for entry in plan.result_schema.as_sequence()] == ["sum"]

    # Assert (lockstep): the Scan under the Aggregate injects the operand column
    # ("headcount") as an execution name, so the drained row lays the operand cell
    # exactly where reduce() reads it. Without this the operands misalign silently.
    scan_names = [entry[0] for entry in plan._source.result_schema.as_sequence()]
    assert "headcount" in scan_names


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


def test_planner_builds_the_right_leaf_as_a_full_query_scan_of_the_right_data_source(engine):
    # ADR-0021: the right side of a join is no longer a retrieve-by-id. ADR-0018
    # built it as a `Retrieve` -- a bulk `pages.retrieve` keyed on the ids the left
    # rows point at -- which is D HTTP round trips for D distinct referenced ids
    # (`executemany` loops one retrieve per id at the wire). Scan-both replaces that
    # with a FULL `data_sources.query` scan of the RIGHT data source, matched
    # client-side by `object_id` (⌈R/100⌉ round trips, cheaper for small/medium
    # right tables). So the Planner must build the right child as a query Scan
    # targeting `join.right.get_data_source_id()` -- NOT a `pages.retrieve`.
    #
    # The structural sibling (test_planner_turns_a_join_select_into_a_hashjoin_over_
    # two_scans) deliberately left the right leaf's OPERATION "the open design point
    # for the driving reds" -- it pinned only that the right child IS a Scan. THIS
    # pins the operation: a query on the right data source, not a retrieve. Today
    # the join branch still builds `Retrieve(parameters=None)`, whose operation is
    # `pages.retrieve` and whose `_parameters` is None -- so both assertions fail.
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

    # A residual-free join, built into a real ExecutionContext the phase-1 way; the
    # Planner reads the plan off the context, it does not run it.
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

    join = ctx.invoked_stmt._joins[0]

    # Act: the Planner compiles the join into a plan; take its right leaf.
    right_leaf = Planner(ctx).plan()._right_child

    # Assert: the right leaf is a full data_sources.query Scan (NOT a pages.retrieve)
    # and it scans the RIGHT data source (courses) -- the whole right table, to be
    # matched client-side by object_id downstream.
    assert right_leaf._operation == {"endpoint": "data_sources", "request": "query"}
    assert (
        right_leaf._parameters["path_params"]["data_source_id"]
        == join.right.get_data_source_id()
    )


def test_hashjoin_open_forwards_the_connection_to_both_leaves():
    # HashJoin is a pass-through for I/O: each leaf now mints its OWN cursor off
    # the connection and shapes it with its OWN schema (decision #5, #364), so
    # open() just hands the SAME connection down to both children -- it mints and
    # shapes nothing itself. (It used to mint the right leaf's cursor off the
    # left's and inject the right schema from outside; the right Scan owns that
    # now.) Driving through a spy for each leaf isolates the forwarding contract.
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
    stmt = select(students, courses).join(students.c.enrolled_in)
    join = stmt._joins[0]
    projection = list(stmt._projection)

    class _SpyLeaf:
        def __init__(self):
            self.opened_with = "<unopened>"

        def open(self, connection):
            self.opened_with = connection

        def next(self): ...
        def close(self): ...

    left, right = _SpyLeaf(), _SpyLeaf()
    hashjoin = HashJoin(left, right, join, projection)

    connection = object()   # opaque stand-in for the DBAPI Connection

    # Act: open the join.
    hashjoin.open(connection)

    # Assert: both leaves received the SAME connection, untouched -- the parent
    # minted no cursor of its own.
    assert left.opened_with is connection
    assert right.opened_with is connection


def test_planner_layers_a_filter_carrying_the_residual_over_the_hashjoin(engine):
    # A join select whose WHERE names a RIGHT-side column (courses.title).
    # databases.query cannot answer it in phase-1, so the compiler holds it
    # back as the residual on the PlanningContext. The Planner must honour that
    # residual by layering a Filter ON TOP of the HashJoin (Red 1's whole tree),
    # carrying the predicate the Filter evaluates client-side.
    #
    # That predicate is now the residual AST ITSELF, handed over untouched --
    # not compiled to Notion JSON. The Filter answers it with eval3, which
    # needs what only the AST carries: the column's type_, to pick the per-type
    # operator rule, and a node shape that can express AND/OR/NOT so a verdict
    # can come back UNKNOWN. Compiling to JSON threw both away, and forced the
    # answer back through the fake client's _Filter -- the notion_sdk import
    # ADR-0019 exists to sever.
    #
    # Identity, not equality: the Planner must PASS the residual, not rebuild
    # something equal to it. An equal-but-rebuilt node would mean a second
    # renderer still sits on this path.
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
    # is the very residual the compiler held back.
    assert isinstance(plan, Filter)
    assert isinstance(plan._source, HashJoin)
    assert plan._filter is ctx.compiled.planning_context.recheck_where


def test_planner_hands_the_residual_over_with_its_literal_unprocessed(engine):
    # The mirror of the test above, and the reason the two are separate.
    #
    # This test used to assert the OPPOSITE: that the Planner renders the
    # residual to inlined Notion JSON, applying the column type's
    # filter_value_processor() exactly as _resolve_bindparam does for a
    # COLUMN_FILTER bind, so the date arrives as "2026-01-01" rather than a
    # bare datetime.date. Nothing renders the residual any more, so no
    # processor runs on this path at all -- the literal reaches the Filter as
    # the Python object the user wrote.
    #
    # That is safe, but NOT trivially so, and it is the one thing worth pinning
    # here: eval3's date rules normalise the literal themselves, routing it
    # through the same b.isoformat() the pushed filter would have used. So a
    # residual date predicate and a pushed one still agree -- which is the
    # pushdown-soundness invariant ADR-0019 names, at the one type where the
    # two sides speak different languages (ISO strings on the wire, date
    # objects in the AST).
    #
    # Date is what makes this observable: String's processor is None, so the
    # title residual above cannot tell an unprocessed literal from a processed
    # one. Here the two are visibly different objects.
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

    # Assert: the residual arrives as the AST, and its literal is still the
    # date object the user wrote -- NOT Notion's "2026-01-01" ISO string.
    assert isinstance(plan, Filter)
    assert plan._filter is ctx.compiled.planning_context.recheck_where
    assert plan._filter.value.effective_value == date(2026, 1, 1)


def test_planner_rejects_a_compound_residual_loudly_instead_of_crashing(engine):
    # A compound OR spanning both join sides is held back WHOLE as the residual
    # (see test_compound_or_spanning_both_sides in test_join_compilation): the
    # residual is a BooleanClauseList (.operator / .clauses), not a single
    # BinaryExpression. The guard was written when the Planner still rendered
    # the residual to Notion JSON: the renderer reached for
    # residual_where.column / .operator / .value -- attributes a
    # BooleanClauseList does not have -- and died with a bare, opaque
    # AttributeError deep inside _compile_type_filter.
    #
    # That renderer is gone, and eval3 handles AND/OR/NOT natively, so nothing
    # would CRASH on a compound residual any more. The guard stays anyway, and
    # deliberately: lifting it is a behaviour change that opens compound
    # residuals to users, and it needs its own slice with its own tests. What
    # this test now pins is that the limit is still declared out loud rather
    # than quietly lapsing -- the failure mode it was written against would now
    # be silent acceptance, not an AttributeError.
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


class _WideSource:
    """A ``VolcanoOperator`` yielding one fixed batch under a fixed schema.

    ``Project`` has to be handed a source whose schema is WIDER than what it
    advertises, and today no real plan builds one: the scan leaf's schema is
    ``fetch_columns()`` and nothing widens it until step 4 of ADR-0022 adds the
    recheck's predicate columns. Driving a stub is not a shortcut here -- it is
    the only way to exercise the trim before the thing that needs it exists.
    """

    def __init__(self, schema: SchemaInfo, rows: list[tuple]) -> None:
        self._schema = schema
        self._rows = rows
        self._drained = False

    def open(self, connection) -> None:
        self._drained = False

    def next(self):
        if self._drained:
            return None
        self._drained = True
        return list(self._rows)

    def close(self) -> None:
        pass

    @property
    def result_schema(self) -> SchemaInfo:
        return self._schema


def test_project_trims_the_rows_and_the_schema_it_advertises_to_the_projected_names(students):
    # ADR-0022 step 2. The recheck must READ columns the user did not project --
    # `SELECT name WHERE effort != 5` needs `effort` in hand to re-check the
    # predicate -- but `effort` must not reach the user's Row. `SchemaInfo`
    # cannot express "fetch, don't return": `_merge_names` (resultschema.py:45)
    # flattens execution and projected names into ONE ordered list and
    # `ResultColumn` has no `projected` flag, so a widened `fetch_columns`
    # reaches the user. A plan STAGE owns the distinction instead:
    #
    #     Scan(superset) -> Filter(recheck) -> Project(projection)
    #
    # `Project` must trim BOTH HALVES CONSISTENTLY -- the tuples it yields and
    # the schema it advertises -- because `engine/base.py:319` PAIRS them:
    # `ResultSet(plan.result_schema.as_sequence(), "page", rows)`. Trimming the
    # rows alone shifts every value under the wrong key; trimming the schema
    # alone hides a column that is still in the tuple. One behaviour, so one
    # test asserts both.
    #
    # MEASURED, and it decides what the trim list IS: today the non-join plan
    # root advertises exactly `fetch_columns()`, NOT `result_columns()`. The two
    # differ -- `result_columns` is `fetch_columns` minus SpecialColumns
    # (compiler.py:715) -- so for `select(students.c.object_id)` the root is
    # `['object_id']` while `result_columns()` is `[]`, and a `Project` trimming
    # to `result_columns()` would hand back a column-less row. Hence: the
    # projected names are the PRE-WIDENING `fetch_columns`, and step 4 must keep
    # that snapshot rather than recompute it after widening.
    #
    # Nothing in the ENGINE would catch that mistake, which is why it is pinned
    # here and in the planner test: a plain SELECT never reaches the Planner at
    # all today (context.py:402 -- see the note there), so trimming to
    # `result_columns()` was MEASURED to red the planner test and not one single
    # pipeline test.
    #
    # The source's second row carries `{"number": None}` -- the #384 valueless
    # cell. Project is a positional trim over RAW cells and must not get clever
    # about them: no decoding, no emptiness rule, no None-fill. Only `eval3`
    # answers what a valueless cell means.
    #
    # Fails at import today: `Project` does not exist.

    # Arrange: a source three columns wide, of which the user projected one.
    wide = SchemaInfo.from_table(
        students,
        execution_names=["object_id", "name", "id"],
    )
    source = _WideSource(
        wide,
        [
            ("page-1", {"title": [{"text": {"content": "Galileo"}}]}, {"number": 5}),
            ("page-2", {"title": [{"text": {"content": "Isaac"}}]}, {"number": None}),
        ],
    )

    # Act: project down to the one column the user asked for.
    project = Project(source, ["name"])
    project.open(None)
    batch = project.next()
    project.close()

    # Assert: the rows are trimmed to the projected column, positionally...
    assert batch == [
        ({"title": [{"text": {"content": "Galileo"}}]},),
        ({"title": [{"text": {"content": "Isaac"}}]},),
    ]

    # ...and the schema it advertises is trimmed to match, so the pair
    # engine/base.py:319 builds the ResultSet from stays consistent.
    assert [entry[0] for entry in project.result_schema.as_sequence()] == ["name"]


def test_project_maps_one_source_batch_to_one_batch_and_forwards_exhaustion(students):
    # The trim test above drives a single batch and stops, which cannot see the
    # `next()` CONTRACT: one source batch in, one trimmed batch out, and `None`
    # forwarded when the source is spent. Two things ride on it.
    #
    # EXHAUSTION. `engine/base.py:311-313` drains the plan with
    # `while (batch := plan.next()) is not None:`. An operator that returns `[]`
    # instead of `None` at exhaustion does not end that loop -- `[] is not None`
    # -- so the engine spins forever. Measured against a draining implementation:
    # 100 000 spins, 1 row, no termination. This is the sibling assertion to
    # test_scan_yields_the_store_rows_as_a_batch_then_reports_exhaustion.
    #
    # STREAMING. `Scan` pulls one Notion page per `next()` on purpose, and that
    # laziness is pinned (test_scan_pulls_pages_lazily_fetching_only_what_next_
    # demands). `Project` is a per-row transform, so it must preserve the batch
    # boundaries it is handed: draining the source into one big batch would
    # materialise the whole table before the engine sees a row and would silently
    # undo ADR-0010's pagination for every SELECT once Project is the plan root.
    # `Aggregate` drains because a cross-row reduction cannot do otherwise;
    # `Project` has no such excuse. `Filter` (queryplan.py:304-314) is the shape.

    class _TwoBatchSource(_WideSource):
        """A source that hands out its rows one batch per ``next()``."""
        def next(self):
            if not self._rows:
                return None
            return [self._rows.pop(0)]

    wide = SchemaInfo.from_table(students, execution_names=["object_id", "name"])
    source = _TwoBatchSource(
        wide,
        [
            ("page-1", {"title": [{"text": {"content": "Galileo"}}]}),
            ("page-2", {"title": [{"text": {"content": "Isaac"}}]}),
        ],
    )

    # Act: drive the operator one batch at a time, one call past the end.
    project = Project(source, ["name"])
    project.open(None)
    first = project.next()
    second = project.next()
    third = project.next()
    project.close()

    # Assert: the source's batch boundaries survive the trim -- two batches in,
    # two batches out, NOT one drained batch of two rows.
    assert first == [({"title": [{"text": {"content": "Galileo"}}]},)]
    assert second == [({"title": [{"text": {"content": "Isaac"}}]},)]

    # ...and exhaustion is signalled as None, which is what ends the engine's
    # drain loop. `[]` here is the non-terminating answer, so `is None` is the
    # assertion and `not third` would NOT do.
    assert third is None
