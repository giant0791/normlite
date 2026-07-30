# sql/eval3.py
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
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

"""Provide 3 value ternary system singletons."""
from __future__ import annotations
import operator
from collections.abc import Callable
from typing import Any

from normlite.notion_sdk.getters import rich_text_to_plain_text
from normlite.notion_sdk.types import normalize_filter_date, normalize_page_date
from normlite.sql.elements import BinaryExpression, BooleanClauseList, ColumnElement, UnaryExpression

class Ternary:
    def __call__(self, *args, **kwds):
        return self

def _date_cmp(
    cmp: Callable[[Any, Any], bool],
    on_incomparable: bool = False,
) -> Callable[[Any, Any], bool]:
    """Build a date rule comparing normalised instants.

    The raw cell carries Notion's ISO strings while the predicate's literal is
    a Python ``date``/``datetime``, so neither side is comparable as it stands.
    The literal is routed through the same normalisation the pushed filter
    applies, which is what keeps a residual date predicate agreeing with a
    pushed one.

    ``on_incomparable`` is the verdict when either side has no start instant.
    Negative operators pass ``True`` so they stay proper negations of their
    positive counterparts.
    """
    def rule(a: Any, b: Any) -> bool:
        cell = normalize_page_date(a)
        lit = normalize_filter_date(b if isinstance(b, str) else b.isoformat())
        if not cell or not lit or cell["start"] is None or lit["start"] is None:
            return on_incomparable
        return cmp(cell["start"], lit["start"])
    return rule

TRUE = Ternary()
FALSE = Ternary()
UNKNOWN = Ternary()


_ABSENT_AWARE: set[str] = set()
# operators that must inspect an absent cell themselves rather than
# short-circuit to UNKNOWN; is_null() joins this set in #366

_OPERATORS = {
    # number operators
    "number.equals": operator.eq,
    "number.does_not_equal": operator.ne,
    "number.greater_than": operator.gt,
    "number.less_than": operator.lt,
    "number.greater_than_or_equal_to": operator.ge,
    "number.less_than_or_equal_to": operator.le,
    "number.is_empty": lambda a, _: a is None,
    "number.is_not_empty": lambda a, _: a is not None,

    # rich text operators
    "rich_text.is_empty": lambda a, _: a is None or len(a) == 0 or rich_text_to_plain_text(a) == "",
    "rich_text.is_not_empty": lambda a, _: a is not None and len(a) != 0 and rich_text_to_plain_text(a) != "",
    "rich_text.equals": lambda a, b: bool(a) and rich_text_to_plain_text(a) == b,
    "rich_text.does_not_equal": lambda a, b: not a or rich_text_to_plain_text(a) != b,
    "rich_text.contains": lambda a, b: bool(a) and b in rich_text_to_plain_text(a),
    "rich_text.does_not_contain": lambda a, b: not bool(a) or b not in rich_text_to_plain_text(a),
    "rich_text.starts_with": lambda a,b: bool(a) and rich_text_to_plain_text(a).startswith(b),
    "rich_text.ends_with": lambda a,b: bool(a) and rich_text_to_plain_text(a).endswith(b),

    # title operators
    "title.is_empty": lambda a, _: a is None or len(a) == 0 or rich_text_to_plain_text(a) == "",
    "title.is_not_empty": lambda a, _: a is not None and len(a) != 0 and rich_text_to_plain_text(a) != "",
    "title.equals": lambda a, b: bool(a) and rich_text_to_plain_text(a) == b,
    "title.does_not_equal": lambda a, b: not a or rich_text_to_plain_text(a) != b,
    "title.contains": lambda a, b: bool(a) and b in rich_text_to_plain_text(a),
    "title.does_not_contain": lambda a, b: not bool(a) or b not in rich_text_to_plain_text(a),
    "title.starts_with": lambda a,b: bool(a) and rich_text_to_plain_text(a).startswith(b),
    "title.ends_with": lambda a,b: bool(a) and rich_text_to_plain_text(a).endswith(b),

    # checkbox
    "checkbox.equals": operator.eq,
    "checkbox.does_not_equal": operator.ne,

    # date operators
    "date.is_empty":                lambda a, _: normalize_page_date(a) is None,
    "date.is_not_empty":            lambda a, _: normalize_page_date(a) is not None,
    "date.equals":                  _date_cmp(operator.eq),
    "date.does_not_equal":          _date_cmp(operator.ne, on_incomparable=True),
    "date.after":                   _date_cmp(operator.gt),
    "date.before":                  _date_cmp(operator.lt),

    # relation operators
    "relation.is_empty":          lambda a, _: not a,
    "relation.is_not_empty":      lambda a, _: bool(a),
    "relation.contains":          lambda a, b: bool(a) and b in [i["id"] for i in a],
    "relation.does_not_contain":  lambda a, b: not a or b not in [i["id"] for i in a],    
}

_PRESENCE_TESTS = {"is_empty", "is_not_empty"}
# operators answered by whether the cell holds a value, not by comparing it;
# these must reach a determinate verdict on a NULL cell

_COMPARISONS = {
    key.split(".", 1)[1] 
    for key in _OPERATORS
} - _PRESENCE_TESTS
# set of comparison operators: when comparing with None value they all return UNKNOWN

def _has_no_value(val: dict) -> bool:
    return val is None

def eval3(predicate: ColumnElement, prop: dict, schema: dict = None) -> Ternary:
    if isinstance(predicate, UnaryExpression):
        value = eval3(predicate.element, prop, schema=schema)
        if value is UNKNOWN:
            return UNKNOWN

        if value is FALSE:
            return TRUE

        if value is TRUE:
            return FALSE

    if isinstance(predicate, BinaryExpression):
        effective_val = predicate.value.effective_value
        op = predicate.column.type_.supported_ops.get(predicate.operator)
        prop_val = prop.get(predicate.column.name)
        if prop_val is None and op not in _ABSENT_AWARE:
            # guard against cells shape: {"a": None}
            # Indistinguishable from absent key {"b": ...} 
            # the absent key case needs the schema argument
            return UNKNOWN

        type_ = predicate.column.type_.get_col_spec()
        value = prop_val.get(type_)
        if op in _COMPARISONS and _has_no_value(val=value):
            return UNKNOWN

        opkey = f"{type_}.{op}"
        rule = _OPERATORS.get(opkey)
        if rule is None:
            raise NotImplementedError(f"eval3 has no rule for {opkey!r}")
        return TRUE if rule(value, effective_val) else FALSE

    if isinstance(predicate, BooleanClauseList):
        clauses = [
            eval3(c, prop, schema=schema)
            for c in predicate.clauses
        ]

        if predicate.operator == "and":
            if any(c is FALSE for c in clauses):
                return FALSE        # FALSE dominates and operator

            if any(c is UNKNOWN for c in clauses):
                return UNKNOWN

            return TRUE

        if predicate.operator == "or":
            if any(c is TRUE for c in clauses):
                return TRUE         # TRUE dominates or operator

            if any(c is UNKNOWN for c in clauses):
                return UNKNOWN

            return FALSE
