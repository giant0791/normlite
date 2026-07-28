# tests/unit/utils/test_reference_evaluator.py
# Copyright (C) 2025 Gianmarco Antonini
#
# This module is part of normlite and is released under the GNU Affero General Public License.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Tests for the reference evaluator itself.

:mod:`tests.utils.evaluator` is test infrastructure everywhere else in the
suite, but here it is the artifact under test: it is the oracle the ``eval3``
differential is measured against, so a defect in it silently caps what the
differential can prove.
"""

import pytest

from tests.utils.evaluator import extract_page_value, reference_eval


def _page(prop_type: str, value) -> dict:
    return {"properties": {"note": {"type": prop_type, prop_type: value}}}


@pytest.mark.parametrize("prop_type", ["rich_text", "title"])
def test_an_absent_text_value_decodes_differently_from_a_blank_one(prop_type):
    """Two distinct text cells must not decode to one and the same value.

    ``{"rich_text": []}`` carries no text value at all; ``[{"text":
    {"content": ""}}]`` carries one whose content happens to be the empty
    string. They are different cells, so they must decode to different values:
    the oracle's job is to hand an operator the cell it was really given, and
    ``[]`` *is* that cell. Collapsing the pair into one blank string forges a
    value the page never held.

    An earlier version of this docstring justified the split by claiming the
    two differ under ``is_empty`` -- true of the first, false of the second.
    Measured against the real API they do **not** differ: ``is_empty`` is true
    of both, because it tests the cell's content rather than its array length
    (ADR-0019 Correction 2026-07-27). :class:`~normlite.notion_sdk.client._Filter`
    does answer them differently, but that is the fake client carrying the bug
    ``eval3`` has now shed (**#381**), not evidence about Notion. Nor can the
    empty-literal half of the old claim be checked: Notion **discards** an
    empty-string literal and returns the data source unfiltered (**#382**), so
    it has no opinion to compare against.

    The assertion stands on cell identity alone, which needs no operator to
    adjudicate it.
    """
    absent = _page(prop_type, [])
    blank = _page(prop_type, [{"text": {"content": ""}}])

    absent_value = extract_page_value(absent, "note", prop_type)
    blank_value = extract_page_value(blank, "note", prop_type)

    assert absent_value != blank_value


@pytest.mark.parametrize("prop_type", ["rich_text", "title"])
@pytest.mark.parametrize("op", ["starts_with", "ends_with"])
def test_a_prefix_or_suffix_test_on_an_absent_text_value_is_false(prop_type, op):
    """An absent text cell begins with nothing and ends with nothing.

    ``{"rich_text": []}`` holds no text value, so no non-empty needle can sit
    at either end of it. The answer is **false**, not an error: Notion measures
    ``contains "x"`` at zero rows and ``does_not_contain "x"`` at every row
    against cells of exactly this shape, and both
    :class:`~normlite.notion_sdk.client._Filter` and ``eval3`` already answer
    false here. The oracle is the lone dissenter.

    It dissents by *crashing*. Since ``48c4cea`` an absent text value decodes
    to ``[]`` rather than to a forged ``""`` (see
    :func:`test_an_absent_text_value_decodes_differently_from_a_blank_one`),
    and these two branches are the only ones in the evaluator that reach for a
    string method -- ``list`` has no ``startswith``. Every neighbouring
    operator absorbed ``[]`` unchanged, which is what made that return value
    the right one; these two did not.

    The crash is louder than the silently wrong ``True`` it replaced, so it is
    worth repairing rather than routing around: the moment the generator emits
    an absent text cell, the differential dies of an ``AttributeError`` instead
    of reporting the divergence it was built to find.
    """
    page = _page(prop_type, [])
    filt = {"property": "note", prop_type: {op: "x"}}

    assert reference_eval(page, filt) is False


@pytest.mark.parametrize("prop_type", ["rich_text", "title"])
@pytest.mark.parametrize(
    "items, is_empty_expected",
    [
        ([], True),
        ([{"text": {"content": ""}}], True),
        ([{"text": {"content": "Ada"}}], False),
    ],
    ids=["absent", "blank", "content"],
)
def test_a_text_presence_test_is_the_exact_complement_of_its_emptiness_test(
    prop_type, items, is_empty_expected
):
    """``is_not_empty`` on text answers the negation of ``is_empty``, cell for cell.

    The oracle has no generic ``is_not_empty`` branch at all: ``date`` and
    ``relation`` each answer it inside their own section, and every other type
    falls through to ``raise ValueError``. Text is one of those types, so the
    operator has never been askable of it -- which is why neither differential
    exercises it, and why ``eval3``'s ``is_not_empty`` half has no net under it
    while its ``is_empty`` half now does.

    The two operators are asserted **together, over the same cell**, rather than
    in separate tests. That is the ``32e53a3`` lesson: fixing ``is_empty`` alone
    once left ``eval3`` answering TRUE to *both* on a blank cell, a state no
    single-operator test can see: the cell came out empty *and* not-empty at
    once. A blank cell is empty because its content is blank rather than because
    its array is short, so it is the case that separates the two readings; ``[]``
    and a real name are the controls that keep the complement from being
    satisfied trivially.
    """
    page = _page(prop_type, items)

    empty = reference_eval(page, {"property": "note", prop_type: {"is_empty": "true"}})
    not_empty = reference_eval(
        page, {"property": "note", prop_type: {"is_not_empty": "true"}}
    )

    assert empty is is_empty_expected
    assert not_empty is not is_empty_expected
