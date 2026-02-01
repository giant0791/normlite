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

from collections import defaultdict
import math
from typing import Dict, Iterable


class CoverageCounter:
    def __init__(self):
        self._counts = defaultdict(int)

    def hit(self, key: str):
        self._counts[key] += 1

    def update(self, keys: Iterable[str]):
        for k in keys:
            self.hit(k)

    @property
    def counts(self) -> Dict[str, int]:
        return dict(self._counts)

    def describe(self) -> dict:
        values = list(self._counts.values())

        if not values:
            return {
                "count": 0,
                "unique": 0,
                "min": 0,
                "max": 0,
                "mean": 0,
                "std": 0,
                "total": 0,
            }

        n = len(values)
        total = sum(values)
        mean = total / n
        variance = sum((v - mean) ** 2 for v in values) / n
        std = math.sqrt(variance)

        return {
            "count": n,
            "unique": n,
            "min": min(values),
            "max": max(values),
            "mean": round(mean, 3),
            "std": round(std, 3),
            "total": total,
        }

class CoverageRegistry:
    def __init__(self):
        self.types = CoverageCounter()
        self.operators = CoverageCounter()
        self.logical_nodes = CoverageCounter()
        self.schemas = CoverageCounter()

    def report(self) -> dict:
        return {
            "types": self.types.describe(),
            "operators": self.operators.describe(),
            "logical_nodes": self.logical_nodes.describe(),
            "schemas": self.schemas.describe(),
        }

    def pretty_print(self):
        def section(title, counter):
            print(f"\n== {title} ==")
            for k, v in sorted(counter.counts.items()):
                print(f"{k:20} {v}")
            print("describe:", counter.describe())

        section("Types", self.types)
        section("Operators", self.operators)
        section("Logical nodes", self.logical_nodes)
        section("Schemas", self.schemas)
