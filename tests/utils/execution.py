from typing import Any, Mapping, Optional, Tuple

from normlite.engine.context import ExecutionContext, ExecutionStyle
from normlite.engine.interfaces import _distill_params
from normlite.engine.base import Engine
from normlite.sql.dml import ExecutableClauseElement


def run_context(
    engine: Engine,
    stmt: ExecutableClauseElement,
    params: Optional[Mapping[str, Any]] = None,
    execution_options: Optional[Mapping[str, Any]] = None,
) -> Tuple[Any, ExecutionContext]:
    """
    Execute a statement through the full ExecutionContext pipeline
    without going through Connection.execute().

    Returns:
        (CursorResult, ExecutionContext)
    """
    compiled = stmt.compile(engine._sql_compiler)
    cursor = engine.raw_connection().cursor()
    connection = engine.connect()

    ctx = ExecutionContext(
        engine,
        connection,
        cursor=cursor,
        compiled=compiled,
        distilled_params=_distill_params(params),
        execution_options=execution_options or {},
    )

    # --- execution pipeline ---
    ctx.pre_exec()
    ctx.invoked_stmt._setup_execution(ctx)

    if ctx.execution_style == ExecutionStyle.EXECUTEQUERYPLAN:
        # NOTE: this duplicates Connection._execute_context's dispatch, as the
        # branches below do. Delegate to the connection the context was built
        # with -- minting a second one here would leave the plan executing
        # against a Connection the ExecutionContext does not know about.
        connection._execute_query_plan(ctx)
    elif ctx.execution_style == ExecutionStyle.EXECUTE:
        engine.do_execute(ctx._get_exec_cursor(), ctx.operation, ctx.parameters)
    else:
        engine.do_executemany(ctx._get_exec_cursor(), ctx.bulk_operation, ctx.bulk_parameters)

    ctx.post_exec()
    ctx.invoked_stmt._finalize_execution(ctx)

    return ctx.setup_cursor_result(), ctx


def run_execute(
    engine: Engine,
    stmt: ExecutableClauseElement,
    params: Optional[Mapping[str, Any]] = None,
    execution_options: Optional[Mapping[str, Any]] = None,
):
    """
    Execute a statement using the public Engine/Connection API.

    This is the canonical pipeline entrypoint for pipeline/integration tests.
    """
    with engine.connect() as conn:
        return conn.execute(stmt, params, execution_options=execution_options)