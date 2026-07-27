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

from tests.utils.evaluator import extract_page_value


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
