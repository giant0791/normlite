# sql/base.py
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

from typing import Optional, Protocol

class Executable(Protocol):
    """Provide the interface for all executable SQL statements."""

    def prepare(self) -> None:
        """Prepare this executable for execution.
        
        This method is used to populate the internal structures needed for execution.
        That is compiling and constructing the operation and parameters dictionaries.
        """
        ...

    def bindparams(self, parameters: Optional[dict]) -> None:
        ...

    def operation(self) -> dict:
        ...

    def parameters(self) -> dict:
        ...

