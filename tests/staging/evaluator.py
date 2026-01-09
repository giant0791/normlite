# tests/reference/evaluator.py
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

"""Reference evaluator for the Notion-like query engine.

This is a very simple and dump implementation of the query engine used by :class:`normlite.notionsdk.InMemory` class.
It represents the ground-truth for differential testing.
"""

import pdb

from normlite.notion_sdk.types import normalize_filter_date, normalize_page_date


def extract_page_value(page, prop, typ):
    try:
        prop_obj = page["properties"][prop]
    except KeyError:
        return None

    if typ in ("title", "rich_text"):
        items = prop_obj.get(typ, [])
        if not items:
            return ""
        return items[0]["text"]["content"]

    if typ == "date":
        # Always return the date object or {}
        return prop_obj.get("date", {})

    return prop_obj.get(typ)

def is_empty_value(val):
    return val in ("", None, [], {})

def reference_eval(page: dict, filt: dict) -> bool:
    if "and" in filt:
        return all(reference_eval(page, f) for f in filt["and"])

    if "or" in filt:
        return any(reference_eval(page, f) for f in filt["or"])

    if "not" in filt:
        return not reference_eval(page, filt["not"])

    prop = filt["property"]
    typ, cond = next((k, v) for k, v in filt.items() if k != "property")
    op, val = next(iter(cond.items()))

    page_val = extract_page_value(page, prop, typ)

    # --- DATE HANDLING ---
    if typ == "date":
        page_date = normalize_page_date(page_val)

        # unary operators
        if op == "is_empty":
            return page_date is None

        if op == "is_not_empty":
            return page_date is not None
        
        # binary operators
        filter_date = normalize_filter_date(val)

        if page_date is None or filter_date is None:
            return False

        if op == "equals":
            return page_date == filter_date

        if op == "does_not_equal":
            return page_date != filter_date

        if op == "after":
            return (
                page_date["start"] is not None
                and filter_date["start"] is not None
                and page_date["start"] > filter_date["start"]
            )

        if op == "before":
            return (
                page_date["start"] is not None
                and filter_date["start"] is not None
                and page_date["start"] < filter_date["start"]
            )

    # --- OTHER TYPES ---
    if op == "equals":
        return page_val == val
    if op == "contains":
        return val in page_val
    if op == "does_not_contain":
        return val not in page_val
    if op == "starts_with":
        return page_val.startswith(val)
    if op == "ends_with":
        return page_val.endswith(val)
    if op == "greater_than":
        return page_val > val
    if op == "less_than":
        return page_val < val
    if op == "is_empty":
        return page_val in ("", None, [], {})

    raise ValueError(f"Unsupported operator: {op}")
