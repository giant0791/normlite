import pytest

from normlite.engine.base import _distill_params
from normlite.engine.context import ExecutionContext
from normlite.notiondbapi.dbapi2 import Connection as DBAPIConnection
from normlite.sql.dml import select
from normlite.sql.queryplan import Scan

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
