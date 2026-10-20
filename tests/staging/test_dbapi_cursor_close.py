"""`Cursor.close()` must finish closing even when the batch source close() raises.

A DBAPI cursor that cannot be closed leaks its state forever: `closed` stays
`False`, the result sets and the batch source stay attached, and every further
`close()` re-raises on the same batch source. These tests drive `Cursor.close()`
directly with stand-in batch sources, so they need no Notion client, no
fixtures and no network.
"""
import pytest

from normlite.notiondbapi.dbapi2 import Connection, Cursor


class _NotionClientStub:
    """Just enough client for a cursor to exist; close() never needs it."""

    def __getattr__(self, name):
        raise AssertionError(f"Cursor.close() must not talk to the client (got: {name})")


def _cursor() -> Cursor:
    connection = Connection(_NotionClientStub())
    return Cursor(connection)


class _Source:
    """Minimal batch source: close() succeeds."""

    def __init__(self) -> None:
        self.close_calls = 0

    def next(self):
        return None

    def close(self) -> None:
        self.close_calls += 1


class _FailingSource(_Source):
    """Minimal batch source: close() raises."""

    def close(self) -> None:
        self.close_calls += 1
        raise RuntimeError("boom: batch source refused to close")


def _executed_cursor(source) -> Cursor:
    """A cursor as it looks after execute() attached a batch source."""
    cursor = _cursor()
    cursor._batch_source = source
    cursor._result_sets = [["row"]]
    return cursor


def test_close_resets_state_when_batch_source_close_raises():
    cursor = _executed_cursor(_FailingSource())

    with pytest.raises(RuntimeError, match="boom"):
        cursor.close()

    assert cursor.closed is True
    assert cursor._result_sets == []
    assert cursor._batch_source is None


def test_error_from_batch_source_close_is_not_swallowed():
    cursor = _executed_cursor(_FailingSource())

    with pytest.raises(RuntimeError, match="boom"):
        cursor.close()


def test_second_close_is_a_no_op_after_a_failing_source_close():
    source = _FailingSource()
    cursor = _executed_cursor(source)

    with pytest.raises(RuntimeError):
        cursor.close()

    # No further exception and no second close() on the same batch source.
    cursor.close()
    assert source.close_calls == 1


def test_normal_close_closes_the_batch_source_once():
    source = _Source()
    cursor = _executed_cursor(source)

    cursor.close()

    assert cursor.closed is True
    assert source.close_calls == 1
    cursor.close()
    assert source.close_calls == 1


def test_close_without_batch_source_still_works():
    cursor = _cursor()

    cursor.close()

    assert cursor.closed is True
