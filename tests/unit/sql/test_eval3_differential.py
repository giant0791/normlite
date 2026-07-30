# tests/unit/sql/test_eval3_differential.py
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

"""Differential: ``eval3`` against the reference evaluator (ADR-0019).

``test_eval3.py`` pins the truth tables case by case and
``test_every_declared_operator_has_an_eval3_rule`` proves every declared pair is
*registered*. Neither shows a registered rule is *right* — that is this file's
job, and it is how the ``date.does_not_equal`` pushdown-soundness bug was found.

The two evaluators do not return the same thing, so the comparison has to be
stated rather than assumed. ``reference_eval`` returns ``bool``; ``eval3``
returns TRUE/FALSE/UNKNOWN. They are compared **under the WHERE policy**
(``is TRUE``), which is lossy in a way that matters: it cannot tell FALSE from
UNKNOWN, precisely the distinction ADR-0019 added.
"""
from collections import Counter

from normlite.sql.eval3 import TRUE, UNKNOWN, eval3

from tests.utils.ast_bridge import filter_to_ast
from tests.utils.evaluator import reference_eval
from tests.utils.generators import ReferenceGenerator

SEED = 28

GENERATABLE_PAIRS = {
    f"{type_name}.{token}"
    for type_name in ReferenceGenerator.TYPES
    for token in ReferenceGenerator.OPERATORS[type_name]
}
"""The 36 ``<type>.<operator>`` pairs the reference generator can emit.

``eval3`` declares 36, and the differential now reaches **all** of them. The
list of unreachable pairs this docstring used to carry is empty, which is what
#381 was for: the generator had been capped at ``_Condition._allowed_ops``, and
that cap was never a statement about Notion — only about what the fake client
could answer at the time. Widening the client (``e8ef480``, ``39914da``,
``292979a``, ``b5f6b98``) and then the generator closed the gap from both ends.

The set is derived from the generator rather than written out, so it widened on
its own; only the count and this prose needed editing. That is the property to
preserve if a type or operator is ever added.
"""

KNOWN_DIVERGENCE = "number.does_not_equal"
"""The one leaf pair where ``eval3`` and Notion genuinely disagree — #384.

Not a bug and not a temporary state: Notion's negative operators are the set
**complement** of their positive twins (measured, both number and text), so a
valueless cell **matches** ``does_not_equal`` because it failed ``equals``.
SQL's ``<>`` against NULL is UNKNOWN and drops the row. ADR-0019 chose SQL, and
#384's option C keeps the residual authoritative, so this gap is permanent by
design. It is pinned here rather than excused because a divergence that is
merely *tolerated* stops being visible the moment it changes.

Measured over 24 000 leaf evaluations with the generator at full width: this is
the **only** divergent pair, in the only direction (Notion keeps, ``eval3``
drops — slack, the safe side of ⊇), on the only shape ``{"number": null}``.

Text is deliberately not on this list. ``title``/``rich_text``
``does_not_equal`` agree exactly, because ``[]`` is a **present** value
(``4abe0d1``) rather than a valueless cell, so ``eval3`` reaches its rule and
answers TRUE just as Notion does.
"""


def _leaf_key(filt: dict) -> str:
    type_name, condition = next((k, v) for k, v in filt.items() if k != "property")
    return f"{type_name}.{next(iter(condition))}"


def test_eval3_agrees_with_the_reference_evaluator_on_every_generated_leaf():
    """Every leaf rule answers what the reference evaluator answers.

    Leaves only — ``gen_condition`` rather than ``gen_filter`` — so a divergence
    names one operator instead of a tree. This is the run that caught
    ``date.does_not_equal`` over ``{"date": {}}``: ``eval3`` said TRUE where both
    the oracle and the pushed ``_Filter`` said False, one divergent leaf in
    ~80 000 evaluations. That was a pushdown-soundness failure, not an oracle
    quibble — the same predicate answered one way Notion-side and the other way
    here, decided by a planner choice the user never sees.

    Leaves answering UNKNOWN need no blanket carve-out. UNKNOWN drops the row
    and the oracle usually says False there too, so the WHERE-policy comparison
    stays exact — with **one** measured exception, ``KNOWN_DIVERGENCE``, which
    is pinned rather than excused.

    Pinning it, instead of skipping every UNKNOWN verdict the way the compound
    test does, is a deliberate choice of the narrow instrument over the wide
    one. A ``verdict is UNKNOWN`` carve-out would exempt ~1400 leaves to admit
    104, and would swallow the very regression it most needs to catch: if
    ``_has_no_value`` ever widened to call a *valued* cell valueless, ``eval3``
    would answer UNKNOWN on ``{"number": 0}``, the oracle would say True, and a
    verdict-keyed exemption could not tell that from #384.

    So the exemption is keyed on the **cell**, and it is an *iff* — divergence
    is permitted only where the cell is valueless and **required** there. Both
    halves are measured: 396 valued cells agree, 104 valueless cells diverge,
    with neither off-diagonal case occurring. A valued cell under the same
    operator still has to agree exactly, and it falls through to do so.

    Requiring the divergence is also what keeps the exemption honest, and it is
    the leaf analogue of the compound test's vacuity guard: an ``eval3``
    quietly regressed to Notion's answer, or a generator that stopped emitting
    valueless number cells, would each make this test's exemption dead code.
    Asserting it fires means it can never become a free pass.

    The coverage assertion is the point of the test as much as the agreement
    one. A differential that silently stopped exercising half the operators
    would still pass, so what was actually exercised is pinned against what the
    generator claims it can produce.
    """
    generator = ReferenceGenerator(SEED)
    exercised = set()
    divergences = []
    pinned = 0

    for _ in range(60):
        schema = generator.gen_schema(min_props=3, max_props=10)
        pages = [generator.gen_page(schema) for _ in range(20)]

        for _ in range(20):
            filt = generator.gen_condition(schema)
            predicate = filter_to_ast(filt)
            key = _leaf_key(filt)
            exercised.add(key)

            for page in pages:
                cell = page["properties"][filt["property"]]
                verdict = eval3(predicate, page["properties"])
                expected = reference_eval(page, filt)
                agrees = (verdict is TRUE) == expected

                # #384, and only over a cell that actually holds no value; a
                # valued cell falls through and must still agree exactly.
                if key == KNOWN_DIVERGENCE and cell["number"] is None:
                    pinned += 1
                    if agrees:
                        divergences.append(
                            f"{key}: #384 divergence has VANISHED over a "
                            f"valueless cell -- eval3 and Notion now agree "
                            f"where they must not. filter={filt} cell={cell} "
                            f"eval3-keeps={verdict is TRUE} oracle={expected}"
                        )
                    continue

                if not agrees:
                    divergences.append(
                        f"{key}: filter={filt} cell={cell} "
                        f"eval3-keeps={verdict is TRUE} oracle={expected}"
                    )

    assert not divergences, (
        f"{len(divergences)} leaf divergences, first 3:\n  "
        + "\n  ".join(divergences[:3])
    )
    assert exercised == GENERATABLE_PAIRS, (
        "differential no longer exercises what the generator can produce; "
        f"missed={sorted(GENERATABLE_PAIRS - exercised)} "
        f"unexpected={sorted(exercised - GENERATABLE_PAIRS)}"
    )
    assert pinned > 0, (
        f"the {KNOWN_DIVERGENCE} exemption was never exercised, so it is "
        "dead code rather than a pinned fact -- the generator has stopped "
        "producing valueless number cells, or the operator is no longer "
        "reachable"
    )


def test_eval3_agrees_with_the_reference_evaluator_wherever_it_is_determinate():
    """Over compound predicates, a *determinate* verdict must match the oracle.

    Compounds cannot be compared as strictly as leaves, and the reason is
    semantic rather than incidental: ``reference_eval`` is a **boolean**
    evaluator, so its ``not`` is boolean negation, while ``eval3``'s is Kleene.
    Over an unset date, ``NOT (d == x)`` is ``not False`` = True for the oracle
    and ``NOT UNKNOWN`` = UNKNOWN for ``eval3``. The oracle keeps the row, WHERE
    drops it. That is ADR-0019 deciding SQL semantics over Notion's, not a bug,
    and no amount of tuning the oracle removes it — it has no third value to
    return.

    So the invariant is narrowed to exactly where the oracle is still an oracle:
    **whenever ``eval3`` reaches TRUE or FALSE, it must agree.** Divergence is
    permitted only under UNKNOWN, the one verdict ``bool`` cannot express.

    That carve-out is a hole, and the second assertion is what stops it becoming
    a free pass: an ``eval3`` regressed to answering UNKNOWN everywhere would
    satisfy the invariant *vacuously*. Pinning that determinate verdicts stay
    the overwhelming majority keeps the exemption narrow rather than load-bearing.
    """
    generator = ReferenceGenerator(SEED)
    outcomes = Counter()
    violations = []

    for _ in range(60):
        schema = generator.gen_schema(min_props=3, max_props=10)
        pages = [generator.gen_page(schema) for _ in range(20)]

        for _ in range(20):
            filt = generator.gen_filter(schema, depth=3, max_depth=6)
            predicate = filter_to_ast(filt)

            for page in pages:
                verdict = eval3(predicate, page["properties"])
                if verdict is UNKNOWN:
                    outcomes["unknown"] += 1
                    continue

                outcomes["determinate"] += 1
                expected = reference_eval(page, filt)
                if (verdict is TRUE) != expected:
                    violations.append(
                        f"filter={filt} cells={page['properties']} "
                        f"eval3-keeps={verdict is TRUE} oracle={expected}"
                    )

    assert not violations, (
        f"{len(violations)} determinate verdicts disagree with the oracle, first 2:\n  "
        + "\n  ".join(violations[:2])
    )

    total = outcomes["determinate"] + outcomes["unknown"]
    assert outcomes["determinate"] > 0.9 * total, (
        "UNKNOWN has stopped being the exception: "
        f"{outcomes['unknown']} of {total} verdicts are UNKNOWN, so the "
        "carve-out is now doing the work the agreement assertion should"
    )
