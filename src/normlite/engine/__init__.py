# notiondbapi/__init__.py
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
"""Engine abstraction and connection Management.

The engine package defines the basic components used to interface
DBAPI modules with higher-level statement construction,
connection-management, execution and result contexts.  The primary
"entry point" class into this package is the :class:`normlite.engine.base.Engine` and its public
constructor :func:`normlite.engine.base.create_engine`.
"""