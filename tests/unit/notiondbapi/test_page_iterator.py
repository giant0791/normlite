import pytest

# Adjust the import to wherever you site the deep module.
from normlite.notiondbapi.page_iterator import PageIterator


def test_eager_drain_walks_every_page_in_token_order_then_reports_exhausted():
    # Arrange: three pages chained by opaque cursor tokens. The row payloads
    # ("r1".."r5") are deliberately opaque — the iterator must follow the
    # token protocol without understanding what a row is.
    pages = {
        None:    {"results": ["r1", "r2"], "has_more": True,  "next_cursor": "cur-1"},
        "cur-1": {"results": ["r3", "r4"], "has_more": True,  "next_cursor": "cur-2"},
        "cur-2": {"results": ["r5"],       "has_more": False, "next_cursor": None},
    }
    requested_cursors = []

    def fetch_page(start_cursor):
        requested_cursors.append(start_cursor)
        return pages[start_cursor]

    iterator = PageIterator(fetch_page, page_size=2)

    # Act: eager drain — pull every page now.
    drained = list(iterator)

    # Assert: every page was fetched, in token order, starting from None and
    # following next_cursor; the walk stopped exactly when has_more went false.
    assert requested_cursors == [None, "cur-1", "cur-2"]
    assert [page["results"] for page in drained] == [["r1", "r2"], ["r3", "r4"], ["r5"]]
    assert iterator.exhausted is True


def test_page_claiming_more_with_null_cursor_raises_instead_of_looping_forever():
    # Arrange: a single malformed page that violates the token protocol — it
    # claims another page exists (has_more) but hands back no cursor to fetch it.
    # Following this naively re-fetches start_cursor=None forever.
    malformed = {"results": ["r1"], "has_more": True, "next_cursor": None}
    requested_cursors = []

    def fetch_page(start_cursor):
        requested_cursors.append(start_cursor)
        return malformed

    iterator = PageIterator(fetch_page, page_size=2)

    # Act / Assert: the contradiction is caught at the first pull and surfaced
    # loudly, rather than the iterator spinning on a null cursor. (Exception type
    # is a placeholder — swap for a domain error if the module grows one.)
    with pytest.raises(ValueError):
        next(iterator)

    # And it stopped after exactly one fetch — no silent re-walk from the top.
    assert requested_cursors == [None]
