# src/tools/notion_probe.py
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

"""Probe the real Notion API for filter semantics normlite models locally.

normlite carries three evaluators of Notion filter semantics -- ``eval3``
(``src/normlite/sql/eval3.py``), the fake client's ``_Filter``
(``src/normlite/notion_sdk/client.py``) and the test oracle
``reference_eval`` (``tests/utils/evaluator.py``). All three are *models*, so no
differential between them can be evidence about Notion itself. This tool asks
Notion.

It exists because that ceiling was reached: the ``eval3`` differential was green
while two real defects sat inside it, and ADR-0019's motivating example turned
out to be factually wrong about ``is_empty``. See #382 for what the first run
found.

Read-only **except for one explicitly gated command**. ``schema``, ``pages`` and
``query`` only ``GET`` the data source or ``POST`` to its ``/query`` endpoint;
they never create, update or delete. ``create`` does write, and refuses to run
without ``--i-mean-it`` -- see its section below. Needs network and credentials,
so it must never run in CI and must never be imported by ``tests/unit``
(``testpaths`` already excludes ``src/tools/``).

Credentials
-----------
Create a file **outside the repo**, mode 600::

    ~/.normlite-notion-probe.env
        NOTION_TOKEN=ntn_...
        NOTION_DATA_SOURCE_ID=...

Override its location with ``NOTION_PROBE_ENV``. The token is never printed, and
HTTP errors report the response body only -- never the request, whose headers
carry it.

Usage
-----
::

    uv run python src/tools/notion_probe.py schema   # the data source's properties
    uv run python src/tools/notion_probe.py pages    # inventory + classify every cell shape
    uv run python src/tools/notion_probe.py query    # which shapes does each filter select?

    uv run python src/tools/notion_probe.py create --i-mean-it   # WRITES. see below

``pages`` writes an inventory to a temp directory (``NOTION_PROBE_OUT`` to
override) which ``query`` reads back, so results group by *cell shape* rather
than by page.

The ``create`` command -- the one writer
----------------------------------------
It answers one question the other three structurally cannot: **which wire
spellings of "this number cell has no value" does ``POST /v1/pages`` accept?**
That cannot be read off an existing row, because a stored cell says nothing
about which request shapes were legal on the way in.

It matters because normlite emits ``{"number": null}`` on the write path
(``f27b338``) and that had only ever been validated against the *fake* client --
a model, and one that structurally cannot represent the rival spelling
(omitting the property), since ``client.py:862`` requires the page's properties
to match the schema's exactly. So the local suite can never catch this class of
error in either direction. The adjacent datum cuts against assuming it is fine:
``{"date": {}}`` is a **measured 400** on this same endpoint, so it does
validate property value shapes rather than accepting anything.

Three cases per run, and the third is a control: without it, a 200 on
``{"number": null}`` might only mean numbers are not validated at all. Each
created page is read back with a separate ``GET``, because the POST response
could be echoing the request rather than reporting what was stored.

Every page it creates is titled ``probe: ...`` and its id is printed, so the
rows can be found and deleted. Leaving the ``null`` one **is useful** -- the
data source still lacks an empty-number control (#384).


Preparing a data source
-----------------------
Point it at a **scratch** data source holding a handful of rows that differ only
in the cell shape under test -- an empty array, a blank string, a real value, a
multi-item text. ``pages`` reports which shapes it found, so it adapts to
whatever is there rather than requiring a fixed layout. Formula columns are read
back too, which is how the formula-vs-filter divergence was caught; note that
formula behaviour is *not* evidence about filter behaviour.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections import defaultdict
from functools import partial
from pathlib import Path

API = "https://api.notion.com/v1"
VERSION = "2026-03-11"
PACE = 0.35  # Notion allows ~3 req/s

INVENTORY = (
    Path(os.environ.get("NOTION_PROBE_OUT", tempfile.gettempdir()))
    / "normlite-notion-probe-inventory.json"
)


def _load_env(path: Path) -> tuple[str, str]:
    if not path.exists():
        sys.exit(f"missing credentials file: {path}")
    values = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        values[key.strip()] = val.strip().strip("'\"")
    try:
        return values["NOTION_TOKEN"], values["NOTION_DATA_SOURCE_ID"]
    except KeyError as exc:
        sys.exit(f"{path} is missing {exc}")


TOKEN, DSID = _load_env(
    Path(os.environ.get("NOTION_PROBE_ENV", "~/.normlite-notion-probe.env")).expanduser()
)


def _request(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    """Return ``(status, payload)``; never raises for an HTTP error status."""
    req = urllib.request.Request(
        f"{API}{path}",
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Notion-Version": VERSION,
            "Content-Type": "application/json",
        },
    )
    time.sleep(PACE)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.load(resp)
    except urllib.error.HTTPError as exc:
        # the response only -- the request's headers carry the token
        try:
            return exc.code, json.loads(exc.read().decode())
        except ValueError:
            return exc.code, {}


def _call(method: str, path: str, body: dict | None = None) -> dict:
    status, payload = _request(method, path, body)
    if status != 200:
        sys.exit(f"{method} {path} -> HTTP {status}: {json.dumps(payload)[:600]}")
    return payload


def _resolve_data_source(ident: str) -> str:
    """Accept a data_source_id OR a database_id.

    Since Notion 2025-09-03 a database and its data sources are distinct objects
    with distinct ids, and the id in a Notion URL is the **database**. Passing it
    to /data_sources gives a 404 whose message blames sharing, which sends you
    hunting for the wrong problem -- so resolve it here instead.
    """
    status, _ = _request("GET", f"/data_sources/{ident}")
    if status == 200:
        return ident

    status, db = _request("GET", f"/databases/{ident}")
    if status != 200:
        sys.exit(
            f"{ident} is neither a data source nor a database this integration can read.\n"
            "Check that the page or database is shared with the integration."
        )

    sources = db.get("data_sources") or []
    if len(sources) == 1:
        print(f"note: {ident} is a DATABASE id; using its data source {sources[0]['id']}\n")
        return sources[0]["id"]
    if not sources:
        sys.exit(f"database {ident} has no data sources")
    listing = "\n".join(f"  {s['id']}  {s.get('name')!r}" for s in sources)
    sys.exit(
        f"database {ident} has {len(sources)} data sources; set NOTION_DATA_SOURCE_ID "
        f"to one of them:\n{listing}"
    )


_RESOLVED: str | None = None


def _dsid() -> str:
    global _RESOLVED
    if _RESOLVED is None:
        _RESOLVED = _resolve_data_source(DSID)
    return _RESOLVED


def _schema() -> dict[str, str]:
    ds = _call("GET", f"/data_sources/{_dsid()}")
    props = ds.get("properties") or {}
    return {name: spec.get("type", "?") for name, spec in props.items()}


def _pick(schema: dict, wanted: str) -> str | None:
    return next((name for name, typ in schema.items() if typ == wanted), None)


def _pick_all(schema: dict, wanted: str) -> list[str]:
    return [name for name, typ in schema.items() if typ == wanted]


def _formula_value(spec: dict):
    """Unwrap a formula property value to its Python payload."""
    f = spec.get("formula") or {}
    return f.get(f.get("type"))


def cmd_schema() -> None:
    schema = _schema()
    print(json.dumps(schema, indent=2))
    print()
    for typ in ("title", "rich_text", "number", "date", "relation", "checkbox"):
        print(f"  {typ:11} -> {_pick(schema, typ)!r}")


# ---------------------------------------------------------------------------
# Shape classification. These are DIFFERENT cells that a decode to a plain
# Python value collapses -- which is the whole reason residuals read raw cells.
# ---------------------------------------------------------------------------

def _classify_text(props: dict, prop: str, typ: str = "rich_text") -> str:
    if prop not in props:
        return "absent-property"
    cell = props[prop].get(typ)
    if cell is None:
        return "null"
    if cell == []:
        return "empty-array"
    if len(cell) == 1:
        content = cell[0].get("text", {}).get("content")
        return "blank" if content == "" else f"value({content!r})"
    return f"multi({len(cell)} items)"


def _classify_number(props: dict, prop: str) -> str:
    if prop not in props:
        return "absent-property"
    cell = props[prop].get("number")
    if cell is None:
        return "null"  # {"number": null} -- the #381 case
    if cell == 0:
        return "zero"
    return "value"


def _classify_date(props: dict, prop: str) -> str:
    if prop not in props:
        return "absent-property"
    cell = props[prop].get("date")
    if cell is None:
        return "null"
    if cell == {}:
        return "empty-mapping"  # the shape 4abe0d1 fixed
    if cell.get("start") is None:
        return "no-start"
    return "range" if cell.get("end") else "start-only"


def _classify_relation(props: dict, prop: str) -> str:
    if prop not in props:
        return "absent-property"
    cell = props[prop].get("relation")
    if cell is None:
        return "null"
    if cell == []:
        return "empty-array"
    return f"{len(cell)}-item" if len(cell) > 1 else "1-item"


def _title_of(props: dict) -> str:
    for spec in props.values():
        if spec.get("type") == "title":
            items = spec.get("title") or []
            return items[0]["text"]["content"] if items else "(untitled)"
    return "(no title prop)"


def _drain(body: dict) -> list[dict]:
    rows, cursor = [], None
    while True:
        payload = dict(body, page_size=100)
        if cursor:
            payload["start_cursor"] = cursor
        page = _call("POST", f"/data_sources/{_dsid()}/query", payload)
        rows.extend(page["results"])
        if not page.get("has_more"):
            return rows
        cursor = page["next_cursor"]


# type -> (classifier, raw-cell key)
#
# title reuses the rich_text classifier, but only through a partial: the call
# site passes (props, prop) and nothing else, so a bare _classify_text
# registered here would read props[prop]["rich_text"] off a title cell, find
# nothing, and report every title as "null". The two types differ by the key
# their content hides behind and by nothing else.
CLASSIFIERS = {
    "title": (partial(_classify_text, typ="title"), "title"),
    "rich_text": (_classify_text, "rich_text"),
    "number": (_classify_number, "number"),
    "date": (_classify_date, "date"),
    "relation": (_classify_relation, "relation"),
}


def cmd_pages() -> None:
    schema = _schema()
    probed = {
        typ: _pick_all(schema, typ) for typ in CLASSIFIERS
    }
    formula_props = _pick_all(schema, "formula")
    rows = _drain({})

    inventory = {}
    for row in rows:
        props = row["properties"]
        entry = {"title": _title_of(props)}
        for typ, names in probed.items():
            classify, key = CLASSIFIERS[typ]
            for prop in names:
                entry[f"shape:{prop}"] = classify(props, prop)
                entry[f"raw:{prop}"] = props.get(prop, {}).get(key)
        for prop in formula_props:
            entry[f"formula:{prop}"] = _formula_value(props.get(prop, {}))
        inventory[row["id"]] = entry

    INVENTORY.write_text(
        json.dumps(
            {"probed": probed, "formula_props": formula_props, "pages": inventory}, indent=2
        )
    )

    print(f"{len(rows)} pages\n")
    for typ, names in probed.items():
        for prop in names:
            by_shape = defaultdict(list)
            for entry in inventory.values():
                by_shape[entry[f"shape:{prop}"]].append(entry["title"])
            print(f"{typ} {prop!r}:")
            for shape, titles in sorted(by_shape.items()):
                sample = ", ".join(titles[:3]) + (" ..." if len(titles) > 3 else "")
                print(f"  {shape:24} x{len(titles):<3}  e.g. {sample}")
            print()

    if formula_props:
        print("formula columns, per page -- NOT evidence about filter behaviour:")
        for entry in inventory.values():
            vals = " | ".join(f"{p}={entry[f'formula:{p}']!s}" for p in formula_props)
            print(f"  {entry['title'][:24]:26} | {vals}")

    print(f"\nwrote {INVENTORY}")


# Filter matrices. These deliberately probe EVERY operator normlite's type layer
# declares in `supported_ops`, including the ones the fake client refuses -- which
# is the question #381 asks and could not answer locally.

def text_filters(literal: str) -> list:
    """Text operators probed twice: against ``""`` and against a real literal.

    The empty-string half is what the first run used throughout, and it
    measured nothing -- Notion discards an empty-string literal, so a filter
    and its complement both returned every row and the CONDITION IGNORED guard
    fired. That is #382, and it is why ``does_not_equal`` on text was recorded
    as *accepted* (200) with its semantics still unknown.

    Keeping both halves in one table is the point. A row that is ignored with
    ``""`` and answered with a real literal shows the discard is a property of
    the *literal*, not of the operator -- which is the reading #382 needs and
    could not get from a table where every text row was ignored.

    The literal is drawn from the data source at run time rather than hardcoded,
    so ``equals`` is guaranteed a row to match and the ``equals`` /
    ``does_not_equal`` partition is observable, the way literal ``0`` made it
    observable for number.
    """
    return [
        ("is_empty", True),
        ("is_not_empty", True),
        ("equals", ""),
        ("does_not_equal", ""),
        ("equals", literal),
        ("does_not_equal", literal),
        ("contains", literal),
        ("does_not_contain", literal),
        ("starts_with", literal),
        ("ends_with", literal),
    ]

NUMBER_FILTERS = [
    ("is_empty", True),
    ("is_not_empty", True),
    ("equals", 0),
    ("does_not_equal", 0),
    ("greater_than", 0),
    ("less_than", 0),
    ("greater_than_or_equal_to", 0),
    ("less_than_or_equal_to", 0),
]

DATE_FILTERS = [
    ("is_empty", True),
    ("is_not_empty", True),
    ("equals", "2026-01-01"),
    ("does_not_equal", "2026-01-01"),
    ("after", "2026-01-01"),
    ("before", "2026-01-01"),
    ("on_or_after", "2026-01-01"),
    ("on_or_before", "2026-01-01"),
]

# a syntactically valid id that matches nothing: `contains` must be false of every
# row and `does_not_contain` true of every row, so the pair doubles as a control
NO_SUCH_PAGE = "00000000-0000-4000-8000-000000000000"

RELATION_FILTERS = [
    ("is_empty", True),
    ("is_not_empty", True),
    ("contains", NO_SUCH_PAGE),
    ("does_not_contain", NO_SUCH_PAGE),
]

# A list is a fixed matrix; a callable needs a literal drawn from the data
# source and is resolved in cmd_query.
FILTERS = {
    "title": text_filters,
    "rich_text": text_filters,
    "number": NUMBER_FILTERS,
    "date": DATE_FILTERS,
    "relation": RELATION_FILTERS,
}

# Operators that are exact complements. If a filter and its complement BOTH
# select every row, neither ran -- Notion discarded the condition. Detecting
# this automatically is not a nicety: the first run of this tool reported six
# such filters as meaningful "MATCH" results before the contradiction was
# spotted by hand. That is #382.
COMPLEMENTS = [
    ("is_empty", "is_not_empty"),
    ("equals", "does_not_equal"),
    ("contains", "does_not_contain"),
]


def _verdicts(matched: set[str], inventory: dict, shape_key: str) -> dict[str, str]:
    """Per shape: did every page of that shape agree?

    A SPLIT means the shape classification is hiding a distinction that matters.
    """
    hits = defaultdict(lambda: [0, 0])
    for pid, entry in inventory.items():
        shape = entry.get(shape_key)
        if shape is None:
            continue
        hits[shape][0 if pid in matched else 1] += 1

    out = {}
    for shape, (yes, no) in sorted(hits.items()):
        out[shape] = f"SPLIT {yes}/{yes + no}" if (yes and no) else ("MATCH" if yes else "-")
    return out


def _run(prop: str, typ: str, filters: list, shape_key: str, inventory: dict) -> None:
    all_ids = set(inventory)
    matched: dict[str, set[str]] = {}
    rejected: dict[str, str] = {}
    # label -> the literal it was sent with. Kept explicitly because a rejected
    # filter never enters `matched`, so pairing the two by position drifts by
    # one from the first rejection onward -- which happens on every date run,
    # where does_not_equal is refused (#383).
    literals: dict[str, object] = {}
    for op, value in filters:
        label = f"{op}({value!r})"
        literals[label] = value
        status, payload = _request(
            "POST",
            f"/data_sources/{_dsid()}/query",
            {"filter": {"property": prop, typ: {op: value}}, "page_size": 100},
        )
        if status != 200:
            # an operator the real API does not accept for this type -- a finding,
            # not a failure, so keep going and report it
            rejected[label] = payload.get("code", str(status))
            continue
        if payload.get("has_more"):
            rows = _drain({"filter": {"property": prop, typ: {op: value}}})
            matched[label] = {r["id"] for r in rows}
        else:
            matched[label] = {r["id"] for r in payload["results"]}

    # a filter and its complement both selecting everything means neither ran
    noop = set()
    for pos, neg in COMPLEMENTS:
        for label in matched:
            if not label.startswith(f"{pos}("):
                continue
            arg = label[len(pos):]
            twin = f"{neg}{arg}"
            if twin in matched and matched[label] == all_ids and matched[twin] == all_ids:
                noop.update({label, twin})

    shapes = sorted({e[shape_key] for e in inventory.values() if e.get(shape_key)})
    print(f"\n{typ} property {prop!r}  ({len(all_ids)} pages)")
    head = f"{'filter':30} | " + " | ".join(f"{s:>13}" for s in shapes)
    print(head)
    print("-" * len(head))
    ambiguous = {
        label
        for label in matched
        if literals[label] == "" and label not in noop and matched[label] == all_ids
    }

    for label, hit in matched.items():
        if label in noop:
            print(f"{label:30} | !! CONDITION IGNORED by Notion (all rows returned)")
            continue
        if label in ambiguous:
            print(f"{label:30} | ?? ALL ROWS -- ignored, or vacuously true? indistinguishable")
            continue
        verdicts = _verdicts(hit, inventory, shape_key)
        print(f"{label:30} | " + " | ".join(f"{verdicts.get(s, '-'):>13}" for s in shapes))

    if noop:
        print(
            "\n  !! A filter and its complement both returned every row, so neither was\n"
            "     evaluated. Those rows say nothing about Notion's semantics -- and a\n"
            "     predicate like that must never be pushed down (see #382)."
        )
    for label, code in rejected.items():
        print(f"{label:30} | XX REJECTED by the API ({code})")

    if ambiguous:
        print(
            "\n  ?? An empty-string literal that returned every row, with no complement to\n"
            "     cross-check. `starts_with(\"\")` is vacuously true of every string, so a\n"
            "     discarded condition and a satisfied one look identical here. Treat as NO\n"
            "     evidence either way -- do not record a semantics finding from this row."
        )
    if rejected:
        print(
            "\n  XX The real API refuses these operators for this type. normlite's\n"
            "     `supported_ops` declares them, so anything it can compile it cannot push."
        )


# ---------------------------------------------------------------------------
# The one writer. Everything above this line only reads.
# ---------------------------------------------------------------------------

# Each case is the *inner* property-value object POSTed for the number
# property, or NO_PROPERTY to leave the property out of the request entirely.
# The label becomes the page title, so a leftover row is identifiable in Notion.
NO_PROPERTY = object()

CREATE_CASES = [
    (
        "number null",
        {"number": None},
        "what normlite emits today (f27b338). THE question.",
    ),
    (
        "number omitted",
        NO_PROPERTY,
        "the rival spelling; normlite cannot express it (INSERT rejects "
        "omission) and the fake client cannot represent it (client.py:862).",
    ),
    (
        "number empty-map",
        {"number": {}},
        "CONTROL. The analogue of the measured {\"date\": {}} 400. If this is "
        "accepted too, the endpoint does not validate number shapes and a 200 "
        "above is weak evidence.",
    ),
]

# Text is the one type whose EMPTY shapes no filter can reach: is_empty needs a
# blank cell to match, and the data source has none, so every negative text
# operator's behaviour over an absent value is currently inference. That
# includes one already load-bearing in production -- rich_text.does_not_contain
# returns True on EMPTY_TEXT (client.py:1637) and nobody measured it.
#
# Three spellings, because CONTEXT.md's "Three value shapes (do not conflate)"
# says they are three and the fake client already treats two of them
# differently: [] decodes to the EMPTY_TEXT sentinel, [{"text":{"content":""}}]
# decodes to "". Whether Notion agrees they differ is the question.
TITLE_CREATE_CASES = [
    (
        "title empty-array",
        {"title": []},
        "the sentinel shape. _Filter decodes this to EMPTY_TEXT and branches on "
        "it by identity (client.py:1633-1639).",
    ),
    (
        "title blank-content",
        {"title": [{"text": {"content": ""}}]},
        "what String.bind_processor emits for \"\". _Filter decodes it to \"\" "
        "-- a DIFFERENT branch from the one above (d9fc94e, 32e53a3).",
    ),
    (
        "title omitted",
        NO_PROPERTY,
        "the rival spelling. For number, omission and explicit null stored "
        "IDENTICALLY; whether text collapses the same way is unmeasured.",
    ),
    (
        "title null",
        {"title": None},
        "CONTROL. If a bare null is accepted for a title the endpoint is not "
        "validating text shapes, and the 200s above are weak evidence.",
    ),
]

# Blank-title pages are untitled, so unlike the number probe they cannot be
# found by name in the Notion UI. Every one gets this in the number property so
# a leftover row is still identifiable and deletable.
TITLE_PROBE_MARKER = 384


def cmd_create(argv: list[str]) -> None:
    if "--i-mean-it" not in argv:
        sys.exit(
            "create WRITES to the Notion data source -- it is the only command here\n"
            "that does.\n\n"
            "  create --i-mean-it              up to 3 pages titled 'probe: ...',\n"
            "                                  measuring which spellings of an empty\n"
            "                                  NUMBER cell POST /v1/pages accepts\n"
            "  create blank-title --i-mean-it  up to 4 pages with NO title, measuring\n"
            "                                  the same for an empty TEXT cell. They\n"
            "                                  are untitled by construction, so each\n"
            "                                  carries effort=384 to stay findable\n\n"
            "Re-run with --i-mean-it if that is what you want."
        )

    if "blank-title" in argv:
        _create_blank_titles()
        return

    schema = _schema()
    title_prop = _pick(schema, "title")
    number_prop = _pick(schema, "number")
    if title_prop is None or number_prop is None:
        sys.exit(
            f"need a title and a number property; found title={title_prop!r} "
            f"number={number_prop!r}"
        )

    print(f"data source {_dsid()}")
    print(f"title property {title_prop!r}, number property {number_prop!r}\n")

    results = []
    for label, value, why in CREATE_CASES:
        props = {
            title_prop: {"title": [{"text": {"content": f"probe: {label}"}}]},
        }
        if value is not NO_PROPERTY:
            props[number_prop] = value

        body = {
            "parent": {"type": "data_source_id", "data_source_id": _dsid()},
            "properties": props,
        }
        status, payload = _request("POST", "/pages", body)

        sent = "(property omitted)" if value is NO_PROPERTY else json.dumps(value)
        print(f"{label:18} POST {sent}")
        print(f"{'':18}   {why}")

        if status != 200:
            code = payload.get("code", "?")
            msg = (payload.get("message") or "")[:200]
            print(f"{'':18}   -> HTTP {status}  {code}: {msg}\n")
            results.append((label, status, code, None))
            continue

        page_id = payload["id"]
        # Read it back with a separate GET: the POST response could be echoing
        # the request rather than reporting what Notion actually stored, and
        # that difference is the entire point of asking the API instead of a
        # model.
        readback = _call("GET", f"/pages/{page_id}")
        shape = _classify_number(readback["properties"], number_prop)
        raw = readback["properties"].get(number_prop, {}).get("number")
        print(f"{'':18}   -> HTTP 200  {page_id}")
        print(f"{'':18}      reads back as {shape} (raw {json.dumps(raw)})\n")
        results.append((label, status, None, shape))

    print("-" * 72)
    for label, status, code, shape in results:
        verdict = f"ACCEPTED, stored as {shape}" if status == 200 else f"REJECTED {status} {code}"
        print(f"  {label:18} {verdict}")

    accepted = {label: shape for label, status, _, shape in results if status == 200}
    print()
    if "number null" not in accepted:
        print(
            "  f27b338 IS A WRITE-PATH BUG. normlite emits a shape the API refuses.\n"
            "  The fix is not a one-liner: omitting the property breaks both\n"
            "  client.py:862's exact-match invariant and normlite's 'INSERT rejects\n"
            "  omission' rule."
        )
    elif "number empty-map" in accepted:
        print(
            "  WEAK EVIDENCE. The control was accepted too, so this endpoint does not\n"
            "  validate number value shapes and a 200 on null does not confirm much.\n"
            "  Judge f27b338 on the read-back shapes above, not on the status codes."
        )
    else:
        print(
            "  f27b338 CONFIRMED. The null spelling is accepted, and the control shows\n"
            "  the endpoint does validate number shapes rather than accepting anything."
        )
    omitted_shape = accepted.get("number omitted")
    if omitted_shape is not None and omitted_shape == accepted.get("number null"):
        print(
            "  Note: omission and explicit null read back IDENTICALLY, so the two\n"
            "  spellings are indistinguishable once stored."
        )
    print("\n  Pages titled 'probe: ...' were created. Delete the ones you do not want;\n"
          "  keeping 'probe: number null' fills the missing empty-number control (#384).")


def _create_blank_titles() -> None:
    schema = _schema()
    title_prop = _pick(schema, "title")
    number_prop = _pick(schema, "number")
    if title_prop is None or number_prop is None:
        sys.exit(
            f"need a title property to blank and a number property to mark with; "
            f"found title={title_prop!r} number={number_prop!r}"
        )

    print(f"data source {_dsid()}")
    print(f"title property {title_prop!r}, marker {number_prop}={TITLE_PROBE_MARKER}\n")

    results = []
    for label, value, why in TITLE_CREATE_CASES:
        props = {number_prop: {"number": TITLE_PROBE_MARKER}}
        if value is not NO_PROPERTY:
            props[title_prop] = value

        status, payload = _request(
            "POST",
            "/pages",
            {
                "parent": {"type": "data_source_id", "data_source_id": _dsid()},
                "properties": props,
            },
        )

        sent = "(property omitted)" if value is NO_PROPERTY else json.dumps(value)
        print(f"{label:20} POST {sent}")
        print(f"{'':20}   {why}")

        if status != 200:
            code = payload.get("code", "?")
            msg = (payload.get("message") or "")[:200]
            print(f"{'':20}   -> HTTP {status}  {code}: {msg}\n")
            results.append((label, status, code, None))
            continue

        page_id = payload["id"]
        # Separate GET, for the reason the number probe does it: the POST
        # response can echo the request rather than report what was stored, and
        # here the three spellings are meant to be told apart by what comes back.
        readback = _call("GET", f"/pages/{page_id}")
        shape = _classify_text(readback["properties"], title_prop, "title")
        raw = readback["properties"].get(title_prop, {}).get("title")
        print(f"{'':20}   -> HTTP 200  {page_id}")
        print(f"{'':20}      reads back as {shape} (raw {json.dumps(raw)})\n")
        results.append((label, status, None, shape))

    print("-" * 72)
    for label, status, code, shape in results:
        verdict = (
            f"ACCEPTED, stored as {shape}" if status == 200 else f"REJECTED {status} {code}"
        )
        print(f"  {label:20} {verdict}")

    accepted = {label: shape for label, status, _, shape in results if status == 200}
    print()
    if "title null" in accepted:
        print(
            "  WEAK EVIDENCE. The control was accepted, so this endpoint does not\n"
            "  validate title value shapes and the 200s above confirm little. Judge\n"
            "  by the read-back shapes, not the status codes."
        )
    distinct = {s for s in accepted.values() if s is not None}
    if len(distinct) == 1 and len(accepted) > 1:
        print(
            f"  All accepted spellings read back IDENTICALLY as {distinct.pop()}, so\n"
            "  Notion does not preserve the distinction the fake client branches on.\n"
            "  EMPTY_TEXT vs \"\" would then be a distinction only normlite makes."
        )
    elif len(distinct) > 1:
        print(
            "  The spellings read back DIFFERENTLY, so the distinction survives the\n"
            "  round trip and _Filter is right to branch on it. Which branch is\n"
            "  correct is still a question for `query`, not for this command.\n"
            f"  shapes: {sorted(distinct)}"
        )

    print(
        f"\n  These pages have NO title. Find them by {number_prop}={TITLE_PROBE_MARKER}.\n"
        "  Keep ONE to serve as the blank-text control that this data source has\n"
        "  never had; delete the rest. Then re-run `pages` and `query` -- the title\n"
        "  table will finally have an empty column to report against."
    )


def _text_literal(prop: str, inventory: dict) -> str | None:
    """A text value that really is in the data source, for `equals` to match.

    Drawn from the inventory rather than hardcoded so the run cannot silently
    degrade into the #382 case: a literal no row holds makes `equals` match
    nothing and `does_not_equal` match everything, which is the same table an
    ignored condition produces and is not distinguishable from it.
    """
    for entry in inventory.values():
        cell = entry.get(f"raw:{prop}") or []
        content = cell[0].get("text", {}).get("content") if cell else None
        if content:
            return content
    return None


def cmd_query() -> None:
    if not INVENTORY.exists():
        sys.exit(f"run `pages` first to build {INVENTORY}")
    data = json.loads(INVENTORY.read_text())
    inventory = data["pages"]

    for typ, names in data["probed"].items():
        for prop in names:
            filters = FILTERS[typ]
            if callable(filters):
                literal = _text_literal(prop, inventory)
                if literal is None:
                    print(
                        f"\n{typ} property {prop!r}: every cell is blank or absent, so "
                        "there is no literal to draw -- skipped"
                    )
                    continue
                filters = filters(literal)
            _run(prop, typ, filters, f"shape:{prop}", inventory)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "schema"
    if cmd == "create":
        # dispatched separately, and last: it is the only command that writes
        cmd_create(sys.argv[2:])
    else:
        try:
            handler = {"schema": cmd_schema, "pages": cmd_pages, "query": cmd_query}[cmd]
        except KeyError:
            sys.exit(f"unknown command {cmd!r}; expected schema | pages | query | create")
        handler()
