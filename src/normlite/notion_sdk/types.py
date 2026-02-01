# notion_sdk/client.py 
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
 
from datetime import datetime
import pdb
from typing import Optional, TypedDict


class NormalizedDate(TypedDict):
    start: Optional[datetime]
    end: Optional[datetime]


def parse_iso_date(value: str) -> datetime:
    """
    Parse an ISO date or datetime string into a datetime.
    """
    return datetime.fromisoformat(value)


def normalize_page_date(date_obj: dict | None) -> NormalizedDate | None:
    """
    Normalize a Notion page date property.

    Returns:
    - None          → empty date ({} or missing)
    - NormalizedDate → {'start': datetime | None, 'end': datetime | None}
    """
    if not date_obj or not isinstance(date_obj, dict):
        return None

    start = date_obj.get("start")
    end = date_obj.get("end")

    return {
        "start": parse_iso_date(start) if start else None,
        "end": parse_iso_date(end) if end else None,
    }


def normalize_filter_date(value: str | None) -> NormalizedDate | None:
    """
    Normalize a date value coming from a filter operand.

    Notion semantics:
    - Scalar date → start date
    - end is implicitly None
    """

    if value is None:
        return None

    return {
        "start": parse_iso_date(value),
        "end": None,
    }
