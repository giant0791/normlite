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

from normlite.notion_sdk.getters import rich_text_to_plain_text
from normlite.sql.elements import BinaryExpression, BooleanClauseList, ColumnElement, UnaryExpression

class Ternary:
    def __call__(self, *args, **kwds):
        return self

TRUE = Ternary()
FALSE = Ternary()
UNKNOWN = Ternary()

_COMPARISONS = {"equals", "does_not_equal", "greater_than", "less_than"}
# set of comparison operators: UNKNOWN is returned in comparisons with None value

_ABSENT_AWARE: set[str] = set()
# operators that must inspect an absent cell themselves rather than
# short-circuit to UNKNOWN; is_null() joins this set in #366

_OPERATORS = {
    "number.equals": operator.eq,
    "number.does_not_equal": operator.ne,
    "number.greater_than": operator.gt,
    "number.less_than": operator.lt,
    "number.is_empty": lambda a, _: a is None,

    "rich_text.is_empty": lambda a, _: a is None or len(a) == 0,
    "rich_text.equals": lambda a, b: rich_text_to_plain_text(a) == b,
}

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
        if op in _COMPARISONS and value is None:
            return UNKNOWN

        opkey = f"{type_}.{op}"
        return TRUE if _OPERATORS[opkey](value, effective_val) else FALSE

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
