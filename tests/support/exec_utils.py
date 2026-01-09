# tests/support/generators.py
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
from datetime import date
from normlite.sql.elements import ColumnElement

def exec_expression(source: str, namespace: dict) -> ColumnElement:
    globals_ = {
        "__builtins__": {},
        "date": date,
    }
    locals_ = dict(namespace)
    exec(f"result = ({source})", globals_, locals_)
    return locals_["result"]