"""Pure-compute tests for the Aggregate Volcano operator (issue #362).

The Aggregate operator is a VolcanoOperator: it is driven only through the
``open`` / ``next`` / ``close`` contract. Like the Join operator (#363) it does
NOT drive any I/O -- its single input is supplied by a child operator, and the
test feeds raw phase-1 rows through a trivial in-memory source (no cursor, no
engine, no port; ADR-0018 Correction (7) -- there is NO QueryIO port). The
arrange pattern mirrors ``tests/unit/sql/test_join_operator.py``.

Aggregate is a BLOCKING unary operator: it drains its child fully, reduces the
whole scan to ONE synthetic row (wrapping ``AggregateExecution`` verbatim,
exactly as HashJoin wraps the merge), emits that one row from the first
``next()``, then reports exhaustion. Draining happens on the first ``next()``,
consistent with HashJoin (Scan executes at ``open``, drains in ``next``).
"""

from normlite.sql.dml import select
from normlite.sql.functions import func
from normlite.sql.queryplan import Aggregate
from normlite.sql.resultschema import SchemaInfo
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Integer, String


class _RowSource:
    """In-memory VolcanoOperator child: yields one fixed batch, then exhaustion.

    Stands in for the phase-1 ``Scan`` that produces the rows Aggregate reduces,
    so the operator can be driven as pure compute. It speaks only the Volcano
    open/next/close contract.
    """

    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows
        self._drained = False

    def open(self, connection) -> None:
        pass

    def next(self):
        if self._drained:
            return None
        self._drained = True
        return self._rows

    def close(self) -> None:
        pass


def test_aggregate_operator_reduces_child_rows_into_one_synthetic_sum_row():
    # Arrange: a `select(func.sum(headcount))` whose phase-1 rows are supplied by
    # an in-memory child (the scan itself is not under test). Three seeded rows
    # carry headcounts 5, 10, 3 as the raw `{"number": n}` cells a real scan
    # surfaces.
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("team", String(is_title=True)),
        Column("headcount", Integer()),
    )
    stmt = select(func.sum(accounts.c.headcount))
    raw_columns = stmt._raw_columns

    # THE LOCKSTEP the Scan-under-Aggregate must honour (dml.py:1037-1040):
    # reduce() indexes each operand by position into the drained row, and that
    # layout is `SchemaInfo.from_table` deduped over the compiler's aggregate
    # fetch_columns (`operand_names or ["object_id"]`). Build the child's rows
    # under that exact schema so the operand cell lands where reduce reads it.
    operand_names = list(dict.fromkeys(
        f.column.name for f in raw_columns if f.column is not None
    )) or ["object_id"]
    scan_schema = SchemaInfo.from_table(
        accounts,
        execution_names=operand_names,
        projected_names=operand_names,
    )

    def scan_row(headcount: int) -> tuple:
        cells = [None] * len(scan_schema.columns)
        cells[scan_schema.column_index("headcount")] = {"number": headcount}
        return tuple(cells)

    source = _RowSource([scan_row(5), scan_row(10), scan_row(3)])

    # Act: build the Aggregate over its child and drive it through the Volcano
    # contract only -- the in-memory child ignores the connection, so None stands
    # in for it (no I/O here). The blocking drain + reduce happens on next().
    agg = Aggregate(source, raw_columns)
    agg.open(None)
    first = agg.next()
    second = agg.next()
    agg.close()

    # Assert: exactly ONE synthetic row carrying the raw total (5+10+3 = 18),
    # the result schema surfaces the aggregate key ("sum") via from_aggregate,
    # and a second next() reports exhaustion -- the blocking operator emits its
    # one reduced row and nothing more.
    assert first is not None
    assert len(first) == 1
    assert {"number": 18} in first[0]
    assert [entry[0] for entry in agg.result_schema.as_sequence()] == ["sum"]
    assert second is None


def test_aggregate_operator_over_zero_rows_still_emits_exactly_one_row():
    # THE BLOCKING-DRAIN-IS-STRUCTURAL PIN (ADR-0011: rowcount == 1
    # unconditionally, even over an empty store). A `select(func.sum(headcount))`
    # whose child yields NO rows -- the shape a scan of an empty data source
    # takes: next() returns None immediately. Because Aggregate BLOCKS, it must
    # drain to [] and STILL emit ONE synthetic row (the reduced row for the empty
    # set), not zero rows. Downstream, `_execute_query_plan`'s drain loop then
    # collects that single row -> the result cursor has one row -> rowcount 1.
    #
    # An operator that forwarded child exhaustion (returned None when the child
    # is empty) would emit zero rows and rowcount would collapse to 0 -- the bug
    # this pins out. `sum` of the empty set is None (raw cell None, NOT
    # {"number": None} and NOT a row count).
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("team", String(is_title=True)),
        Column("headcount", Integer()),
    )
    stmt = select(func.sum(accounts.c.headcount))
    raw_columns = stmt._raw_columns

    # An empty child: open/next/close with no rows to drain.
    source = _RowSource([])

    # Act: drive the blocking operator over the empty source.
    agg = Aggregate(source, raw_columns)
    agg.open(None)
    first = agg.next()
    second = agg.next()
    agg.close()

    # Assert: exactly ONE synthetic row survives the empty drain -- the sum of no
    # rows is None -- and a second next() reports exhaustion. One row, not zero:
    # this is what keeps rowcount at 1 through the plan path.
    assert first is not None
    assert len(first) == 1
    assert first[0] == (None,)
    assert [entry[0] for entry in agg.result_schema.as_sequence()] == ["sum"]
    assert second is None
