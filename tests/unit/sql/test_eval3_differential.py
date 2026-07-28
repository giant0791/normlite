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
"""The 28 ``<type>.<operator>`` pairs the reference generator can emit.

``eval3`` declares 36. The 8 it cannot reach — number's ``does_not_equal``,
``greater_than_or_equal_to``, ``less_than_or_equal_to``, ``is_empty``,
``is_not_empty``; ``does_not_equal`` for title and rich_text; checkbox's
``does_not_equal`` — mirror ``_Condition._allowed_ops``, not ``supported_ops``:
the generator was capped to what the fake client could answer. Seven of them are
exactly #381's gap, so widening the generator belongs to #381's landing, and
this set widens with it rather than needing an edit here.

Text's ``is_not_empty`` came off that list once the oracle could answer it: the
cap there was never the fake client, which allows the operator, but the oracle,
which raised on it. Only the count and the list are edited — the set itself is
derived from the generator, so it had already widened.
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

    Leaves may legitimately answer UNKNOWN (an unset date under a comparison),
    and no carve-out is needed for them: UNKNOWN drops the row and the oracle
    says False there too, so the WHERE-policy comparison stays exact. Only
    *compounds* can turn an UNKNOWN into a visible disagreement, which is the
    next test.

    The coverage assertion is the point of the test as much as the agreement
    one. A differential that silently stopped exercising half the operators
    would still pass, so what was actually exercised is pinned against what the
    generator claims it can produce.
    """
    generator = ReferenceGenerator(SEED)
    exercised = set()
    divergences = []

    for _ in range(60):
        schema = generator.gen_schema(min_props=3, max_props=10)
        pages = [generator.gen_page(schema) for _ in range(20)]

        for _ in range(20):
            filt = generator.gen_condition(schema)
            predicate = filter_to_ast(filt)
            exercised.add(_leaf_key(filt))

            for page in pages:
                verdict = eval3(predicate, page["properties"])
                expected = reference_eval(page, filt)
                if (verdict is TRUE) != expected:
                    divergences.append(
                        f"{_leaf_key(filt)}: filter={filt} "
                        f"cell={page['properties'][filt['property']]} "
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
