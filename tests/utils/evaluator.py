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

"""Reference evaluator: an independent model of **Notion's** filter engine.

This models the thing normlite pushes filters *to* -- Notion's row-selection
semantics as measured on the wire -- and deliberately not the fake client, and
deliberately not SQL. It exists to be a second opinion, written in a plain
top-to-bottom style so that it fails differently from the code under test.

An earlier docstring called it "the query engine used by
``normlite.notionsdk.InMemory``". That was the wrong target on both counts. A
model of the fake client would make this a model of a model, and
``test_pushdown_soundness.py`` already compares ``eval3`` against ``_Filter``
directly; a model of SQL would make it a second implementation of ``eval3``,
and a differential against a copy of yourself proves nothing.

Being Notion-shaped, it does **not** agree with ``eval3`` everywhere, and that
is the point rather than a defect: ``eval3`` is SQL three-valued. Where they
part is #384, and it is pinned in ``test_eval3_differential.py`` rather than
smoothed over here.
"""

from normlite.notion_sdk.types import normalize_filter_date, normalize_page_date

def extract_page_value(page, prop, typ):
    try:
        prop_obj = page["properties"][prop]
    except KeyError:
        return None

    if typ in ("title", "rich_text"):
        items = prop_obj.get(typ, [])
        if not items:
            return []
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

    # --- RELATION HANDLING ---
    if typ == "relation":
        rel = page_val or []
        if op == "is_empty":
            return len(rel) == 0
        if op == "is_not_empty":
            return len(rel) > 0
        if op == "contains":
            return any(item["id"] == val for item in rel)
        if op == "does_not_contain":
            return not any(item["id"] == val for item in rel)

    # --- OTHER TYPES ---
    if op == "equals":
        return page_val == val
    # Written as the literal complement of the branch above, not as an
    # independent ``!=``, because that is exactly what it is: Notion's negative
    # operators are the set complement of their positive twins, measured for
    # number and text alike (2026-07-30). A cell with no value falls on the
    # negative side *because it failed the positive test* -- there is no third
    # truth value here, and the API has no ``not`` to compose one with (#383).
    # Spelling it structurally is the same defence ``is_not_empty`` uses below:
    # a complement written independently is a complement free to drift.
    #
    # This is where the oracle stops agreeing with ``eval3``, which answers
    # UNKNOWN on a valueless cell under SQL three-valued logic. The divergence
    # is real, permanent, and pinned in test_eval3_differential.py.
    if op == "does_not_equal":
        return not (page_val == val)
    if op == "contains":
        return val in page_val
    if op == "does_not_contain":
        return val not in page_val
    # An absent text value decodes to ``[]`` (or to ``None`` when the property
    # is missing outright), and nothing sits at either end of a value that is
    # not there: the answer is False. These two branches are the only ones that
    # reach for a string method, so they are the only ones that have to say so.
    if op == "starts_with":
        return isinstance(page_val, str) and page_val.startswith(val)
    if op == "ends_with":
        return isinstance(page_val, str) and page_val.endswith(val)
    # A valueless number cell -- ``{"number": null}``, the only shape one takes
    # on the wire -- has no end to order against, so nothing sits above or
    # below it. This is the date arm's early return (line 78) applied to the
    # other ordered type rather than a second rule, and it is spelled inline
    # for the same reason ``starts_with`` is: these are the only branches that
    # reach for an operation the absent value cannot answer. ``equals`` needs
    # no guard -- ``None == val`` is already False, and False is the answer.
    if op == "greater_than":
        return page_val is not None and page_val > val
    if op == "less_than":
        return page_val is not None and page_val < val
    # The inclusive pair carries the same guard for the same reason, and the
    # boundary is the only thing that separates them from the strict pair.
    # Both were probed live against a ``{"number": 0}`` row and a null one:
    # ``>=(0)`` and ``<=(0)`` each match the zero, and neither matches the
    # valueless cell. Inclusive of the boundary, exclusive of the absent value.
    if op == "greater_than_or_equal_to":
        return page_val is not None and page_val >= val
    if op == "less_than_or_equal_to":
        return page_val is not None and page_val <= val
    # Three members, and each answers for a different state (#390). ``None`` is
    # an absent property; ``[]`` is the only blank text cell Notion stores, which
    # it normalises every accepted spelling to (``264ec8e``, 2026-07-30); ``""``
    # is the decoded blank-content cell -- a shape Notion never returns but
    # ``String.bind_processor`` emits for ``values(col='')`` and the fake client
    # stores verbatim. The last is therefore a real state of the simulated store,
    # not a leftover agreeing with the generator.
    if op == "is_empty":
        return page_val in ("", None, [])
    # Term for term the negation of the branch above, and written that way on
    # purpose: the pair must not be able to drift apart when either side is
    # extended. Spelling this one independently is exactly what once let eval3
    # answer True to both on a blank text cell (32e53a3).
    if op == "is_not_empty":
        return page_val not in ("", None, [])

    raise ValueError(f"Unsupported operator: {op}")
