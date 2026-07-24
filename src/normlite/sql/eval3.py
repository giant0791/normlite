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

from normlite.sql.elements import BinaryExpression, ColumnElement, UnaryExpression


class Ternary:
    def __call__(self, *args, **kwds):
        return self

TRUE = Ternary()
FALSE = Ternary()
UNKNOWN = Ternary()

def eval3(predicate: ColumnElement, prop: dict, schema: dict = None) -> Ternary:
    if isinstance(predicate, UnaryExpression):
        value = eval3(predicate.element, prop, schema=schema)
        if value is UNKNOWN:
            return UNKNOWN

    if isinstance(predicate, BinaryExpression):
        effective_val = predicate.value.effective_value
        prop_val = prop.get(predicate.column.name)
        value = prop_val.get(predicate.column.type_.get_col_spec())

        op = predicate.column.type_.supported_ops.get(predicate.operator)
        if op == "equals":
            if value is None:
                return UNKNOWN

            return TRUE if effective_val == value else FALSE
    else:
        raise NotImplementedError(f"{type(predicate).__name__} not supported.")
            
    

    