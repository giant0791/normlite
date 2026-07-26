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
    string. They are different cells, and the pushed-down evaluator
    (:class:`normlite.notion_sdk.client._Filter`) answers them differently --
    ``is_empty`` is true of the first and false of the second, and the four
    matching operators (``contains``, ``starts_with``, ``ends_with``,
    ``equals``) with an empty literal are false of the first and true of the
    second.

    Collapsing them here is the lossy decode ADR-0019 exists to forbid, and it
    makes the oracle disagree with the pushed side on exactly the inputs where
    a residual evaluator has to agree with it.
    """
    absent = _page(prop_type, [])
    blank = _page(prop_type, [{"text": {"content": ""}}])

    absent_value = extract_page_value(absent, "note", prop_type)
    blank_value = extract_page_value(blank, "note", prop_type)

    assert absent_value != blank_value
