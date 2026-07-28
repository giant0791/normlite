# tests/utils/ast_bridge.py
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

"""Rebuild a Notion filter JSON as a residual predicate AST — **test-only**.

:func:`eval3` and :func:`reference_eval` take different shapes on both sides,
so neither can be handed a generated predicate without a translation:

===================  ===========================  =========================  ==========
evaluator            predicate                    row                        returns
===================  ===========================  =========================  ==========
``eval3``            residual AST                 ``{name: raw_cell}``       ``Ternary``
``reference_eval``   Notion filter JSON           ``{"properties": {...}}``  ``bool``
===================  ===========================  =========================  ==========

The row half needs no bridge: ``page["properties"]`` *is* the ``prop`` mapping
``eval3`` expects, and the extra ``"type"`` key each generated cell carries is
inert (``eval3`` reads ``prop_val.get(type_)``, taking ``type_`` from
``predicate.column.type_``). It is deliberately not stripped.

The predicate half is this module, and it goes **JSON to AST** rather than the
reverse. Driving from :class:`ASTGenerator` instead was considered and rejected:
its ``_gen_type`` has no relation-capable type, so relation would lose coverage
entirely, and it would still need a raw-cell page generator to produce rows.
Generating from the JSON side also serves the pushdown-soundness fuzz, where the
same generated filter has to be pushed into a ``Scan`` payload as JSON *and*
evaluated residually as an AST.

Resurrecting a renderer here is legitimate precisely because it is a test
fixture: ``3681c2d`` deleted the equivalent from ``sql/``, and #365's acceptance
criterion is that production never grows one back.
"""
from __future__ import annotations

from normlite.sql import type_api
from normlite.sql.elements import (
    BinaryExpression,
    BindParameter,
    BooleanClauseList,
    ColumnElement,
    UnaryExpression,
)
from normlite.sql.schema import Column

TYPE_ENGINES: dict[str, type_api.TypeEngine] = {
    "title": type_api.String(is_title=True),
    "rich_text": type_api.String(),
    "number": type_api.Integer(),
    "checkbox": type_api.Boolean(),
    "date": type_api.Date(),
    "relation": type_api.Relation(),
}
"""Notion property type name -> the ``TypeEngine`` a column of it would carry.

``eval3`` dispatches on ``column.type_``, so the type has to be rebuilt before
the operator can be: ``get_col_spec()`` selects the per-type rule family and
``supported_ops`` translates the operator token back to its
:class:`~normlite.sql.elements.Operator`.
"""


def filter_to_ast(filt: dict) -> ColumnElement:
    """Rebuild ``filt`` — a Notion filter JSON — as a residual predicate AST.

    Boolean nodes become the AST's own compound elements, so a generated
    ``and``/``or``/``not`` exercises ``eval3``'s Kleene logic rather than being
    flattened away.
    """
    if "and" in filt:
        return BooleanClauseList("and", [filter_to_ast(f) for f in filt["and"]])

    if "or" in filt:
        return BooleanClauseList("or", [filter_to_ast(f) for f in filt["or"]])

    if "not" in filt:
        return UnaryExpression("not", filter_to_ast(filt["not"]))

    name = filt["property"]
    type_name, condition = next((k, v) for k, v in filt.items() if k != "property")
    token, value = next(iter(condition.items()))

    try:
        type_ = TYPE_ENGINES[type_name]
    except KeyError:
        raise NotImplementedError(
            f"ast_bridge cannot rebuild a {type_name!r} property; "
            f"add it to TYPE_ENGINES"
        ) from None

    operators = {token: op for op, token in type_.supported_ops.items()}
    try:
        operator = operators[token]
    except KeyError:
        raise NotImplementedError(
            f"{type_name!r} declares no operator for {token!r}; "
            f"supported: {sorted(operators)}"
        ) from None

    # is_empty / is_not_empty carry a filler value ("true"/True) that both
    # evaluators ignore, so it rides through rather than being special-cased.
    return BinaryExpression(
        column=Column(name, type_),
        operator=operator,
        value=BindParameter(key=None, value=value, type_=type_),
    )
