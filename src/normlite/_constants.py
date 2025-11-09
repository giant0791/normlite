# normlite/_constants.py
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
from enum import StrEnum

class SpecialColumns(StrEnum):
    NO_ID = "_no_id"
    NO_TITLE = "_no_title"
    NO_ARCHIVED = "_no_archived"
    NO_IN_TRASH = "_no_in_trash"

    @classmethod
    def values(cls) -> tuple[str, ...]:
        return tuple(m.value for m in cls)
