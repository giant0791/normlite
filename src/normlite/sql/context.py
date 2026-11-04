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

    widened_left_columns: list[str] = field(default_factory=list)
    """The left-table columns that only the recheck needs.

    The compiler fills this field only for a join. It holds only the names
    that the recheck adds to the left ``Scan``, never the base list. The base
    list is :attr:`pre_widening_fetch_columns`. The planner appends these
    columns to the join row.

    .. versionadded:: 0.14.0
    """

    widened_right_columns: list[str] = field(default_factory=list)
    """The right-table columns that only the recheck needs.

    The recheck can read a right-table column that the join row does not
    carry. The compiler adds that column to the right ``Scan`` and records
    its name here. The join row starts from a base list:

    * a plain join: the right-table columns of the projection;
    * an aggregate join: the right-table operands.

    This field holds only the names that the compiler adds after the base
    list, never the base list itself. The planner appends these columns to
    the join row, so a base column here would appear two times.

    The compiler builds this field with
    :meth:`normlite.sql.compiler.NotionCompiler._widen_fetch_columns` and
    no ``query_params``, so the right ``Scan`` sends no ``filter_properties``.

    .. versionadded:: 0.14.0
    """

