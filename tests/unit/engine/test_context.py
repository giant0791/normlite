from normlite import CursorResult
from datetime import date
import pdb
from contextlib import nullcontext
from types import SimpleNamespace
import uuid
import pytest

from normlite.engine.base import Engine, create_engine
from normlite.engine.context import ExecutionContext, ExecutionStyle
from normlite.engine.interfaces import _distill_params
from normlite.exceptions import ArgumentError, CompileError, ResourceClosedError, StatementError
from normlite.sql.reflection import ReflectedTableInfo
from normlite.notion_sdk.getters import get_object_id
from normlite.sql.ddl import CreateTable, DropTable
from normlite.sql.dml import insert, select, delete
from normlite.sql.elements import BindParameter, _BindRole
from normlite.sql.functions import func
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, String


# =========================================================
# Fixtures
# =========================================================

@pytest.fixture
def engine() -> Engine:
    return create_engine(
        'normlite:///:memory:',
        _mock_ws_id='12345678-0000-0000-1111-123456789012',
        _mock_ischema_page_id='abababab-3333-3333-3333-abcdefghilmn',
        _mock_tables_id='66666666-6666-6666-6666-666666666666',
        _mock_db_page_id='12345678-9090-0606-1111-123456789012'
    )


@pytest.fixture
def metadata() -> MetaData:
    return MetaData()


@pytest.fixture
def students(metadata: MetaData) -> Table:
    return Table(
        'students',
        metadata,
        Column('name', String(is_title=True)),
        Column('id', Integer()),
        Column('is_active', Boolean()),
        Column('start_on', Date()),
        Column('grade', String())
    )


@pytest.fixture
def insert_values():
    return dict(
        name='Galileo Galilei',
        id=123456,
        is_active=False,
        start_on=date(1690, 1, 1),
        grade='A'
    )


# =========================================================
# Test helpers (CORE REFACTOR)
# =========================================================

def create_students_db(engine: Engine) -> dict:
    # As of Notion 2025-09-03 (ADR-0014) the column schema lives on the data
    # source (`initial_data_source.properties`), the catalog row parents to the
    # `tables` data source id, and it persists both the database id (`table_id`)
    # and its data source id (`table_dsid`).
    db = engine._client._add('database', {
        'parent': {'type': 'page_id', 'page_id': engine._user_tables_page_id},
        "title": [{"text": {"content": "students"}}],
        'initial_data_source': {
            'properties': {
                'name': {'title': {}},
                'id': {'number': {}},
                'is_active': {'checkbox': {}},
                'start_on': {'date': {}},
                'grade': {'rich_text': {}},
            }
        }
    })

    engine._client._add('page', {
        'parent': {'type': 'data_source_id', 'data_source_id': engine._catalog._tables_dsid},
        'properties': {
            'table_name': {'title': [{'text': {'content': 'students'}}]},
            'table_schema': {'rich_text': [{'text': {'content': ''}}]},
            'table_catalog': {'rich_text': [{'text': {'content': 'memory'}}]},
            'table_id': {'rich_text': [{'text': {'content': db['id']}}]},
            'table_dsid': {'rich_text': [{'text': {'content': db['data_sources'][0]['id']}}]},
            'is_dropped': {'checkbox': False},
            'created_time': {'rich_text': [{'text': {'content': db['created_time']}}]},
        }
    })

    return db


def add_students_rows(engine: Engine, students: Table):
    ds_id = students.get_data_source_id()

    for name, sid in [("Galileo Galilei", 1500), ("Isaac Newton", 1600)]:
        engine._client.pages_create(
            payload={
                'parent': {'type': 'data_source_id', 'data_source_id': ds_id},
                'properties': {
                    'name': {'title': [{'text': {'content': name}}]},
                    'id': {'number': sid},
                    'is_active': {'checkbox': False},
                    'start_on': {'date': {'start': '1600-01-01'}},
                    'grade': {'rich_text': [{'text': {'content': 'A'}}]},
                }
            }
        )


@pytest.fixture
def students_db(engine, students):
    db = create_students_db(engine)
    students._sys_columns["object_id"]._value = db['id']
    students._sys_columns["data_source_id"]._value = db['data_sources'][0]['id']
    return db


@pytest.fixture
def populated_students(engine, students, students_db):
    add_students_rows(engine, students)
    return students


# =========================================================
# Execution harness
# =========================================================

def run_context(engine, stmt, params=None, execution_options=None) -> tuple[CursorResult, ExecutionContext]:
    compiled = stmt.compile(engine._sql_compiler)
    cursor = engine.raw_connection().cursor()

    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=cursor,
        compiled=compiled,
        distilled_params=_distill_params(params),
        execution_options=execution_options or {},
    )

    ctx.pre_exec()
    ctx.invoked_stmt._setup_execution(ctx)

    if ctx.execution_style == ExecutionStyle.EXECUTE:
        engine.do_execute(cursor, ctx.operation, ctx.parameters)
    else:
        engine.do_executemany(cursor, ctx.bulk_operation, ctx.bulk_parameters)

    ctx.post_exec()
    ctx.invoked_stmt._finalize_execution(ctx)

    return ctx.setup_cursor_result(), ctx


def run_execute(engine, stmt, params=None, execution_options=None):
    with engine.connect() as conn:
        return conn.execute(stmt, params, execution_options=execution_options)


def is_valid_uuid4(value: str) -> bool:
    try:
        uuid.UUID(value, version=4)
        return True
    except Exception:
        return False


# =========================================================
# Unit tests (pure logic)
# =========================================================

def test_distill_params():
    assert _distill_params() == [{}]
    assert _distill_params({'a': 1}) == [{'a': 1}]
    assert _distill_params([]) == []

    with pytest.raises(TypeError):
        _distill_params(123)

    with pytest.raises(TypeError):
        _distill_params([{'a': 1}, 123])


def test_unused_bind_params_raises(engine, students):
    stmt = insert(students).values(name="A")

    with pytest.raises(CompileError):
        run_context(engine, stmt, params={"unknown": 123})


@pytest.mark.parametrize(
    "role, rechecked, raises",
    [
        # an INSERT value nobody writes is data loss, recheck or not
        (_BindRole.COLUMN_VALUE, True, True),
        # a pruned leaf's bind with no recheck reaches no evaluator
        (_BindRole.COLUMN_FILTER, False, True),
        # a pruned leaf's bind reaches eval3 through the recheck (#383)
        (_BindRole.COLUMN_FILTER, True, False),
    ],
    ids=["value-with-recheck", "filter-without-recheck", "filter-with-recheck"],
)
def test_a_leftover_bind_raises_unless_the_recheck_consumes_it(role, rechecked, raises):
    # The pipeline cannot leave an unknown key behind: construct_params
    # refuses it first (see test_unused_bind_params_raises). So the guard
    # is driven directly, with a stand-in carrying only what it reads.
    leftover = BindParameter("param_0", "orphan")
    leftover.role = role
    ctx = SimpleNamespace(
        invoked_stmt=SimpleNamespace(is_update=False),
        compiled=SimpleNamespace(
            planning_context=SimpleNamespace(
                recheck_where=object() if rechecked else None
            )
        ),
    )

    expectation = pytest.raises(ArgumentError, match="param_0") if raises else nullcontext()
    with expectation:
        ExecutionContext._assert_all_params_consumed(ctx, [{"param_0": leftover}])


def test_execution_style_delete(engine, populated_students, students):
    stmt = delete(students)
    _, ctx = run_context(engine, stmt)

    assert ctx.execution_style == ExecutionStyle.EXECUTEMANY

def test_aggregate_select_is_driven_through_the_query_plan(engine):
    # #362 cut-over: an aggregate select (e.g. select(func.sum(col))) must be driven
    # by the operator tree, exactly like a join select. Today the Planner already
    # builds the blocking Aggregate-over-Scan (b207dc4), but routing still sends
    # aggregates to EXECUTE, so the plan is inert -- the reduction runs in the
    # Select._finalize_execution `if self._is_aggregate:` hook instead.
    #
    # This routing fact is red until context.py routes aggregate selects to
    # EXECUTEQUERYPLAN. Making it green while KEEPING the hook would double-drive:
    # _execute_query_plan synthesises the result cursor, then the hook fires,
    # fetchall()s the never-executed exec cursor and clobbers it with reduce([]).
    # The existing test_aggregate_pipeline oracle forces the hook's retirement in
    # the SAME change -- the routing flip and the hook deletion are atomic.
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("team", String(is_title=True)),
        Column("headcount", Integer()),
    )
    metadata.create_all(engine)

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

    assert ctx.execution_style == ExecutionStyle.EXECUTEQUERYPLAN

@pytest.mark.parametrize("with_where", [False, True], ids=["bare", "where"])
def test_a_plain_select_is_driven_through_the_query_plan(engine, students, students_db, with_where):
    # #384 / ADR-0022 step 2b. The sibling of the #362 routing fact above, for the
    # statement shape #384 actually reports.
    #
    # `context.py` routes to EXECUTEQUERYPLAN only `if stmt.is_select and
    # (stmt._joins or stmt._is_aggregate)`. Everything else takes EXECUTE ->
    # _execute_single and never constructs a Planner. So the whole scan path of
    # the operator tree -- Scan, Filter, and the Project added in 27fdef2 -- is
    # unreachable in production, exercised only by direct Planner(ctx).plan()
    # calls in tests/unit/sql/test_query_plan.py.
    #
    # That is why the recheck cannot land first: ADR-0022 makes every pushed
    # WHERE conjunct decide client-side, and `SELECT ... WHERE id != 5` -- the
    # `where` parametrization here, #384's own repro shape -- would be rechecked
    # on a branch the engine never executes for it.
    #
    # Red until the routing flips. Note the four streaming tests and
    # test_select_rowcount cannot pin this: they are green BEFORE the flip and
    # must be green AFTER it, so nothing in them distinguishes the two routings.
    # This test is the only thing standing between step 2b and a silent revert.
    #
    # Both shapes are pinned because the recheck is not conditional on a WHERE
    # being present: a routing rule that inspects the WHERE clause would give one
    # statement kind two execution paths, which is the shape of thing that
    # produced #384. See the R3 option in ADR-0022's Consequences.
    stmt = select(students)
    if with_where:
        stmt = stmt.where(students.c.id != 5)

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

    assert ctx.execution_style == ExecutionStyle.EXECUTEQUERYPLAN


def test_insert_missing_values_raises(engine, students, students_db):
    stmt = insert(students)

    with pytest.raises(StatementError) as exc:
        run_context(engine, stmt, params={"name": "Alice"})

    msg = str(exc.value)
    assert "name" not in msg
    assert "id" in msg
    assert "is_active" in msg
    assert "start_on" in msg
    assert "grade" in msg

def test_execution_options_precedence(engine, students, students_db):
    stmt = insert(students).execution_options(preserve_rowcount=False)

    _, ctx = run_context(
        engine,
        stmt,
        params={"name": "Alice", "id": 123456, "is_active": True, "start_on": date(1999,1,1), "grade": "B"},
        execution_options={"preserve_rowcount": True},
    )

    assert ctx.execution_options["preserve_rowcount"] is True


def test_result_idempotent(engine, students, students_db):
    result, ctx = run_context(
        engine, 
        insert(students), 
        params={
            "name": "Alice", 
            "id": 123456, 
            "is_active": True, 
            "start_on": date(1999,1,1), 
            "grade": "B"
        }
    )

    assert ctx.setup_cursor_result() is ctx.setup_cursor_result()


# =========================================================
# Pipeline tests (single source of truth)
# =========================================================

@pytest.mark.parametrize("preserve_rowcount,expected", [
    (True, 1),
    (False, -1)
])
def test_insert_rowcount(engine, students, students_db, insert_values, preserve_rowcount, expected):
    result = run_execute(
        engine,
        insert(students),
        insert_values,
        execution_options={"preserve_rowcount": preserve_rowcount},
    )

    assert result.rowcount == expected

def test_insert_implicit_returning(engine, students, students_db, insert_values):
    result = run_execute(
        engine,
        insert(students),
        insert_values,
        execution_options={"implicit_returning": True},
    )

    assert not result.returns_rows
    assert len(result.returned_primary_keys_rows) == 1


def test_select_projection(engine, populated_students, students):
    stmt = select(students.c.is_active)

    result = run_execute(engine, stmt)
    rows = result.all()

    assert len(rows) == 2
    assert "is_active" in rows[0].mapping()
    assert "name" not in rows[0].mapping()


@pytest.mark.parametrize("preserve_rowcount,expected", [
    (True, 2),
    (False, -1)
])
def test_select_rowcount(engine, populated_students, students, preserve_rowcount, expected):
    stmt = select(students)

    result = run_execute(
        engine,
        stmt,
        execution_options={"preserve_rowcount": preserve_rowcount},
    )

    assert result.rowcount == expected


@pytest.mark.parametrize("preserve_rowcount,expected", [
    (True, 1),
    (False, -1)
])
def test_aggregate_select_rowcount(engine, populated_students, students, preserve_rowcount, expected):
    # #392. The sibling of test_select_rowcount for the QUERY-PLAN path.
    #
    # There are two cursors on the plan path, and post_exec used to read the one
    # that never executed. _execute_query_plan drains the plan and parks the rows
    # on a NEW cursor, `context._result_cursor` (base.py), leaving `_cursor`
    # untouched -- so `_cursor.rowcount` answered with its -1 sentinel and
    # post_exec memoized that. `ExecutionContext.cursor` resolves
    # `_result_cursor or _cursor`, which is why the ROWS were right while the
    # COUNT was not, and reading through that property is the fix.
    #
    # An aggregate select is the cheapest statement that reaches the plan path
    # (joins reach it too, and were equally affected). One row out, so the
    # expected count is 1, not the 2 rows the table holds.
    #
    # This was never a #384 regression -- it reproduced on main with no routing
    # change at all. It is pinned here because ADR-0022 step 2b routes plain
    # SELECTs through the planner, at which point the bug would have swallowed
    # test_select_rowcount[True-2] above and step 2b could not have been green.
    # The False case is here so a fix cannot simply stop consulting the sentinel.
    stmt = select(func.count()).select_from(students)

    result = run_execute(
        engine,
        stmt,
        execution_options={"preserve_rowcount": preserve_rowcount},
    )

    assert len(result.all()) == 1
    assert result.rowcount == expected


# =========================================================
# DDL tests (merged)
# =========================================================

def test_create_table(engine, students):
    students._db_parent_id = engine._user_tables_page_id

    result = run_execute(engine, CreateTable(students))

    assert not result.returns_rows
    assert result.rowcount == -1
    assert is_valid_uuid4(students.get_oid())
    assert all(c._id for c in students.user_columns)


def test_drop_table(engine, students, students_db):
    inspector = engine.inspect()
    result, ctx = run_context(engine, DropTable(students))

    with pytest.raises(ResourceClosedError):
        rows = result.all()

    assert inspector.is_dropped(students)
    assert inspector.is_dropped(students.name)
    assert not result.returns_rows


# =========================================================
# Integration sanity test (pipeline correctness)
# =========================================================

def test_full_pipeline_create_table(engine, students):
    students._db_parent_id = engine._user_tables_page_id

    result = run_execute(engine, CreateTable(students))

    assert not result.returns_rows
    assert is_valid_uuid4(students.get_oid())