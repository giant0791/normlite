# tests/unit/sql/test_pushdown_soundness.py
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

"""Pushdown soundness: the pushed filter must never drop a row the residual keeps.

ADR-0019 states the invariant as ``{pushed} == {residual}``, and that form is
**known false**. Notion's ``does_not_equal`` *matches* a valueless cell while
SQL's ``<>`` against NULL is UNKNOWN and drops it (#384, measured against the
live API), so the same predicate selects different rows depending on a planner
choice the user never sees. No amount of tuning either evaluator removes that:
Notion's negation is boolean, ``eval3``'s is Kleene.

The resolution being adopted is the standard pushdown discipline: **the pushed
filter is a hint, never the final word.** It may over-match, and the residual is
always re-applied client-side, so what the user gets is whatever the residual
says. That makes the pushed filter's obligation one-directional:

    ``{rows the pushed filter keeps}`` **⊇** ``{rows the residual keeps}``

and this module fuzzes exactly that.

**Only one direction can hurt.** A row the push keeps and the residual drops is
*slack* — the re-check removes it, at the cost of having transferred it. A row
the push **drops** while the residual would have kept it is **unrecoverable**:
it never reaches the client, so no re-check can resurrect it, and the query
silently returns too few rows. That asymmetry is the whole content of ⊇, and it
is why this is not merely the existing differential with a weaker assertion.

**What models "pushed" here.** ``_Filter`` is the simulated client's evaluator,
the same one its query path applies (``client.py:242``), so it is normlite's
model of Notion — not Notion. Every deviation measured against the live API so
far either *widens* the pushed set (#382: an empty-string literal is discarded
and the whole data source comes back; #384: ``does_not_equal`` keeps valueless
cells) or rejects the request outright with HTTP 400 (#383). Widening preserves
⊇; a 400 is not a wrong answer. So the real API is, as far as it has been
probed, **more** permissive than this model, which is the safe side of the
invariant. Nothing measured so far shows Notion dropping a row SQL would keep —
and that, not agreement, is the property being defended.

**Measured** (seed 28, 24 000 evaluations per test), now that #381 has widened
the generator to all **36** declared pairs: leaves show **104** slack rows
(0.43%) and compounds **387** (1.61%), with zero violations either way.

Leaf slack left zero for the first time here, and it has exactly one source:
``number.does_not_equal`` over ``{"number": null}``, which is #384 itself. That
is the case this module was written for and could not reach — ⊇ and ``==`` are
no longer indistinguishable on leaves, and the gap between them is precisely
the bug. Compound slack remains the Kleene-``NOT``-over-UNKNOWN shape.

The count is reported rather than asserted, deliberately. The ``eval3``
differential already pins this same divergence from the other side, keyed on
the cell shape; asserting it here as well would couple two instruments to one
fact and make a future *correct* change to which leaves are generated (#366's
``is_null``, or narrowing ``date`` per #383) look like a regression.

**This fuzz is one-directional by construction, and that is not a weakness to be
fixed — it is why it does not duplicate the ``eval3`` differential.** Verified by
mutation: reverting ``32e53a3`` (``is_not_empty`` back to ``bool(a)``) makes
``eval3`` keep rows the push drops and reds this module with 182 violations,
while reverting ``d9fc94e`` (``is_empty`` back to array length) makes ``eval3``
keep *fewer* rows, reds both ``eval3`` differentials, and leaves this module
**green** — correctly, because under-keeping is slack. The two instruments cover
different halves of the same net and neither subsumes the other.

**The limitation that has now been lifted**: #384's own operator used to be out
of reach here. ``_Condition._allowed_ops`` refused ``number.does_not_equal``
(#381) and the generator was capped to what the fake client could answer, so
the case that motivated the rule could not be generated. #381 widened both, and
this fuzz reached it with no edit here — the pair set is derived, exactly as
that note predicted.
"""
from collections import Counter

from normlite.notion_sdk.client import _Filter  # production: the pushed side
from normlite.sql.eval3 import TRUE, eval3

from tests.unit.sql.test_eval3_differential import GENERATABLE_PAIRS, _leaf_key
from tests.utils.ast_bridge import filter_to_ast
from tests.utils.generators import ReferenceGenerator

SEED = 28


def _pushed_keeps(page: dict, filt: dict) -> bool:
    """Answer whether the filter, pushed to the server, would return ``page``."""
    return _Filter(page, {"filter": filt}).eval()


def _residual_keeps(page: dict, filt: dict) -> bool:
    """Answer whether the residual re-check would keep ``page``.

    ``is TRUE`` is the WHERE policy — UNKNOWN drops the row along with FALSE —
    the same narrowing ``Filter._right_side_passes`` applies at its ``-> bool``
    boundary.
    """
    return eval3(filter_to_ast(filt), page["properties"]) is TRUE


def test_a_pushed_leaf_never_drops_a_row_its_residual_would_keep():
    """⊇ holds leaf by leaf, so a violation names one operator.

    Leaves only (``gen_condition``), for the same reason the ``eval3``
    differential splits this way: a compound violation implicates a tree, a leaf
    violation implicates a rule. This is the run that would catch an operator
    whose pushed form is *narrower* than its residual form — the one shape of
    disagreement that the re-check cannot repair.

    The slack count is reported rather than asserted. It is the re-check's
    workload — rows the push hands over that the residual then discards — and
    zero slack is not a failure, it just means the two agreed exactly here.

    The vacuity guard is the assertion that keeps the invariant honest: an
    ``eval3`` regressed to dropping everything satisfies ⊇ trivially, because
    the empty set is a subset of anything. Pinning that the residual keeps a
    substantial share of rows is what stops a green run meaning nothing.
    """
    generator = ReferenceGenerator(SEED)
    exercised = set()
    outcomes = Counter()
    violations = []

    for _ in range(60):
        schema = generator.gen_schema(min_props=3, max_props=10)
        pages = [generator.gen_page(schema) for _ in range(20)]

        for _ in range(20):
            filt = generator.gen_condition(schema)
            exercised.add(_leaf_key(filt))

            for page in pages:
                residual = _residual_keeps(page, filt)
                pushed = _pushed_keeps(page, filt)

                outcomes["residual_kept"] += residual
                outcomes["slack"] += pushed and not residual
                outcomes["total"] += 1

                if residual and not pushed:
                    violations.append(
                        f"{_leaf_key(filt)}: filter={filt} "
                        f"cell={page['properties'][filt['property']]} "
                        f"pushed=DROPS residual=KEEPS"
                    )

    assert not violations, (
        f"{len(violations)} rows would be lost to the push, first 3:\n  "
        + "\n  ".join(violations[:3])
    )
    assert exercised == GENERATABLE_PAIRS, (
        "the fuzz no longer exercises what the generator can produce; "
        f"missed={sorted(GENERATABLE_PAIRS - exercised)} "
        f"unexpected={sorted(exercised - GENERATABLE_PAIRS)}"
    )
    assert outcomes["residual_kept"] > 0.05 * outcomes["total"], (
        "the residual has stopped keeping rows, so ⊇ holds vacuously: "
        f"{outcomes['residual_kept']} of {outcomes['total']} rows kept"
    )


def test_a_pushed_compound_never_drops_a_row_its_residual_would_keep():
    """⊇ survives composition, including over Kleene ``NOT``.

    The leaf test cannot answer this one. ``eval3`` composes leaves with Kleene
    logic and ``_Filter`` composes them with boolean logic, so a compound is
    where the two *disagree by construction* rather than by defect: over an
    unset date, ``NOT (d == x)`` is UNKNOWN for ``eval3`` and True for the push.
    Under ``==`` that is a divergence nobody can act on — the open question of
    whether Kleene-vs-boolean ``NOT`` makes ``NOT``-compounds unpushable.
    Under ⊇ it resolves: the push keeps a row the residual then discards, which
    is exactly the slack the re-check exists to absorb.

    So this test both defends the invariant and *measures* the disagreement it
    tolerates, which is why slack is asserted non-zero here and only reported on
    leaves. Zero slack over compounds would mean the generator had stopped
    producing the UNKNOWN-under-negation shape, and the tolerance would be going
    untested — a carve-out nothing exercises is a carve-out nobody has checked.
    """
    generator = ReferenceGenerator(SEED)
    outcomes = Counter()
    violations = []

    for _ in range(60):
        schema = generator.gen_schema(min_props=3, max_props=10)
        pages = [generator.gen_page(schema) for _ in range(20)]

        for _ in range(20):
            filt = generator.gen_filter(schema, depth=3, max_depth=6)

            for page in pages:
                residual = _residual_keeps(page, filt)
                pushed = _pushed_keeps(page, filt)

                outcomes["residual_kept"] += residual
                outcomes["slack"] += pushed and not residual
                outcomes["total"] += 1

                if residual and not pushed:
                    violations.append(
                        f"filter={filt} cells={page['properties']} "
                        f"pushed=DROPS residual=KEEPS"
                    )

    assert not violations, (
        f"{len(violations)} rows would be lost to the push, first 2:\n  "
        + "\n  ".join(violations[:2])
    )
    assert outcomes["residual_kept"] > 0.05 * outcomes["total"], (
        "the residual has stopped keeping rows, so ⊇ holds vacuously: "
        f"{outcomes['residual_kept']} of {outcomes['total']} rows kept"
    )
    assert outcomes["slack"] > 0, (
        "no compound over-matched, so the ⊇ tolerance went unexercised and "
        "this run proves no more than the stricter == would have"
    )
