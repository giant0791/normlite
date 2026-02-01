# tests/axioms/test_eval_axioms.py
# Copyright (C) 2026 Gianmarco Antonini
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

"""Validate the reference evaluator for the Notion-like query engine.

This testsuite bootstraps ground truth using axioms and truth tables.
If these tests fail, **nothing else matters**.
"""
from tests.reference.evaluator import reference_eval

PAGE = {"properties": {"x": {"type": "number", "number": 1}}}

TRUE = {"property": "x", "number": {"equals": 1}}
FALSE = {"property": "x", "number": {"equals": 2}}

def test_leaf_true():
    assert reference_eval(PAGE, TRUE) is True

def test_leaf_false():
    assert reference_eval(PAGE, FALSE) is False

def test_and_truth_table():
    assert reference_eval(PAGE, {"and": [TRUE, TRUE]})
    assert not reference_eval(PAGE, {"and": [TRUE, FALSE]})
    assert not reference_eval(PAGE, {"and": [FALSE, TRUE]})
    assert not reference_eval(PAGE, {"and": [FALSE, FALSE]})

def test_or_truth_table():
    assert reference_eval(PAGE, {"or": [TRUE, FALSE]})
    assert not reference_eval(PAGE, {"or": [FALSE, FALSE]})

def test_not_truth_table():
    assert reference_eval(PAGE, {"not": FALSE})
    assert not reference_eval(PAGE, {"not": TRUE})
