# sql/context.py
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

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from normlite.sql.elements import ColumnElement
    from normlite.sql.dml import OrderByClause

@dataclass
class PlanningContext:
    """Carry what the compiler held back for the query plan to answer client-side.

    A Notion filter is a lossy probe, so pushing never decides the answer: what
    is pushed is re-applied over raw cells by the plan (ADR-0022).

    .. versionadded:: 0.13.0
    """
    recheck_where: Optional[ColumnElement] = None
    """The held WHERE as AST, decided client-side by :class:`~normlite.sql.queryplan.Filter`."""

    residual_sorts: Optional[OrderByClause] = None
    """The ORDER BY keys with no pushed form, applied once by :class:`~normlite.sql.queryplan.Sort`."""

    pre_widening_fetch_columns: list[str] = field(default_factory=list)
    """The ``fetch_columns`` before the recheck widened them, specials included.

    :class:`~normlite.sql.queryplan.Project` trims back to these, never to
    ``result_columns()``, which drops the specials the row still needs.
    """

