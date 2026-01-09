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

from __future__ import annotations

from abc import ABC, abstractmethod
import random
from typing import Any, Generic, Protocol, Sequence, TypeVar, List, Literal

from faker import Faker
from decimal import Decimal
from datetime import date

from normlite.sql.schema import (
    Table,
    Column,
    MetaData,
)
from normlite.sql import type_api
from normlite.sql.elements import (
    ColumnElement,
    BinaryExpression,
    UnaryExpression,
    BooleanClauseList,
    BindParameter,
)

# ------------------------------------------------------------------------------
# Typing
# ------------------------------------------------------------------------------

T = TypeVar("T")

BoolOp = Literal["and", "or"]


# ------------------------------------------------------------------------------
# Reference workload generator for filters and pages
# ------------------------------------------------------------------------------

class ReferenceGenerator:
    """Reference workload generator for filters and pages in ``normlite``.

    :class:`ReferenceGenerator` is **single source of truth** for types, operators, and value generation.
    It provides **bi-derectional compatibility** page <--> filters semantics.
    It is exendible: Adding a new type touches *one place* only.
    It is deterministic: Seedable RNG + Faker
    """

    TYPES = {
        "title",
        "rich_text",
        "number",
        "date",
        "checkbox",
    }

    OPERATORS = {
        "title": {
            "equals",
            "contains",
            "does_not_contain",
            "starts_with",
            "ends_with",
            "is_empty",
        },
        "rich_text": {
            "equals",
            "contains",
            "does_not_contain",
            "starts_with",
            "ends_with",
            "is_empty",
        },
        "number": {
            "equals",
            "greater_than",
            "less_than",
        },
        "number_with_commas": {
            "equals",
            "greater_than",
            "less_than",
        },
        "dollar": {
            "equals",
            "greater_than",
            "less_than",
        },
        "date": {
            "equals",
            "does_not_equal",
            "after",
            "before",
            "is_empty",
            "is_not_empty",
        },
        "checkbox": {
            "equals",
        },
    }

    def __init__(self, seed: int | None = None):
        self.rng = random.Random(seed)
        self.faker = Faker()
        if seed is not None:
            self.faker.seed_instance(seed)

        self.metadata = MetaData()

    def gen_schema(self, min_props=1, max_props=6) -> dict:
        schema = {}
        n = self.rng.randint(min_props, max_props)

        for i in range(n):
            name = f"prop_{i}"
            typ = self.rng.choice(tuple(self.TYPES))
            schema[name] = {"type": typ}

        return schema
              
    def gen_page(self, schema: dict) -> dict:
        properties = {}

        for prop, spec in schema.items():
            typ = spec["type"]
            properties[prop] = self._gen_property_value(typ)

        return {
            "object": "page",
            "id": self.faker.uuid4(),
            "properties": properties
        }

    def _gen_property_value(self, typ: str) -> dict:
        if typ in ("title", "rich_text"):
            return {
                "type": typ,
                typ: [{
                    "text": {
                        "content": self.faker.name()
                    }
                }]
            }

        if typ == "number":
            return {
                "type": "number",
                "number": self.rng.randint(0, 1000)
            }

        if typ == "number_with_commas":
            return {
                "type": "number",
                "number": self.faker.pydecimal(
                    left_digits=4,
                    right_digits=3,
                    min_value=-9999.999,
                    max_value=9999.999
                )
            }
        
        if typ == "dollar":
            return {
                "type": "number",
                "number": Decimal(self.rng.randint(-9999, 9999)) / 100
            }

        if typ == "checkbox":
            return {
                "type": "checkbox",
                "checkbox": self.rng.choice([True, False])
            }

        if typ == "date":
            if self.rng.random() < 0.2:
                return {
                    "type": "date",
                    "date": {}
                }

            return {
                "type": "date",
                "date": {
                    "start": self._gen_iso_date(),
                    "end": None
                }
            }

        raise ValueError(f"Unsupported property type: {typ}")

    def _gen_iso_date(self) -> str:
        return self.faker.date_between("-5y", "today").isoformat()

    def gen_filter(self, schema: dict, depth=0, max_depth=5) -> dict:
        if depth >= max_depth:
            return self.gen_condition(schema)

        p = self.rng.random()

        if p < 0.5:
            return self.gen_condition(schema)

        if p < 0.75:
            return {
                "not": self.gen_filter(schema, depth + 1, max_depth)
            }

        op = self.rng.choice(["and", "or"])
        return {
            op: [
                self.gen_filter(schema, depth + 1, max_depth),
                self.gen_filter(schema, depth + 1, max_depth),
            ]
        }

    def gen_condition(self, schema: dict) -> dict:
        prop = self.rng.choice(list(schema))
        typ = schema[prop]["type"]
        op = self.rng.choice(sorted(self.OPERATORS[typ]))

        val = self._gen_condition_value(typ, op)

        return {
            "property": prop,
            typ: {op: val}
        }

    def _gen_condition_value(self, typ: str, op: str):
        if typ in ("title", "rich_text"):
            if op == "is_empty":
                return self.rng.choice(["true", "false"])
            return self.faker.first_name()

        if typ == "number":
            return self.rng.randint(0, 1000)

        if typ == "checkbox":
            return self.rng.choice([True, False])

        if typ == "date":
            if op in ("is_empty", "is_not_empty"):
                return self.rng.choice(["true", "false"])
            return self._gen_iso_date()

        raise ValueError(f"Unsupported type/operator: {typ}/{op}")


# ------------------------------------------------------------------------------
# Generator Protocol (public abstraction)
# ------------------------------------------------------------------------------

class Generator(Protocol[T]):
    def generate(self, max_depth: int = 6) -> T:
        ...


class EntityRandomGenerator(Generic[T]):
    """
    Thin wrapper to unify AST and expression generators behind one API.
    """

    def __init__(self, impl: Generator[T]):
        self._impl = impl

    def generate(self, max_depth: int = 6) -> T:
        return self._impl.generate(max_depth=max_depth)

    @classmethod
    def create_astgen(cls, seed: int | None = None) -> EntityRandomGenerator[ColumnElement]:
        return cls(ASTGenerator(seed))

    @classmethod
    def create_exprgen(
        cls,
        seed: int | None = None,
        bool_style: Literal["function", "operator"] = "function",
    ) -> EntityRandomGenerator[str]:
        return cls(ExpressionGenerator(seed, bool_style))


# ------------------------------------------------------------------------------
# Shared semantic generator core
# ------------------------------------------------------------------------------

class _BaseGenerator(ABC, Generic[T]):
    """
    Shared semantic generator.

    This class decides:
      * structure (depth, boolean composition)
      * operator selection
      * literal generation

    Subclasses decide:
      * how expressions are *emitted* (AST vs Python source)
    """

    def __init__(self, seed: int | None = None):
        self.rng = random.Random(seed)
        self.faker = Faker()
        if seed is not None:
            self.faker.seed_instance(seed)

        self.metadata = MetaData()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def generate(self, max_depth: int = 6) -> T:
        if max_depth < 0:
            raise ValueError("max_depth must be >= 0")

        table = self._gen_table()
        columns = list(table.columns)

        return self._gen_expr(
            table=table,
            columns=columns,
            depth=0,
            max_depth=max_depth,
        )

    # ------------------------------------------------------------------
    # Expression generation
    # ------------------------------------------------------------------

    def _gen_expr(
        self,
        table: Table,
        columns: List[Column],
        depth: int,
        max_depth: int,
    ) -> T:
        # Force leaf at max depth
        if depth >= max_depth:
            return self._gen_leaf(columns)

        roll = self.rng.random()

        # Bias toward leaves
        if roll < 0.45:
            return self._gen_leaf(columns)

        # Unary NOT
        if roll < 0.60:
            inner = self._gen_expr(table, columns, depth + 1, max_depth)
            return self._emit_not(inner)

        # Binary boolean
        left = self._gen_expr(table, columns, depth + 1, max_depth)
        right = self._gen_expr(table, columns, depth + 1, max_depth)
        op: BoolOp = self.rng.choice(["and", "or"])
        return self._emit_bool(op, left, right)

    def _gen_leaf(self, columns: List[Column]) -> T:
        column = self.rng.choice(columns)
        operator = self._choose_operator(column.type_)
        value = self._gen_literal(column.type_)
        return self._emit_binary(column, operator, value)

    # ------------------------------------------------------------------
    # Operator selection
    # ------------------------------------------------------------------

    def _choose_operator(self, typ: type_api.TypeEngine) -> str:
        t = type(typ)

        OPS = {
            type_api.Integer: ["==", "!=", ">", "<"],
            type_api.Numeric: ["==", "!=", ">", "<"],
            type_api.Money: ["==", "!=", ">", "<"],
            type_api.Date: ["after", "before"],
            type_api.Boolean: ["is_", "is_not"],
            type_api.String: ["startswith", "endswith", "in_", "not_in"],
        }

        try:
            return self.rng.choice(OPS[t])
        except KeyError:
            raise ValueError(f"No operators defined for type {t}")

    # ------------------------------------------------------------------
    # Literal generation
    # ------------------------------------------------------------------

    def _gen_literal(self, typ: type_api.TypeEngine) -> Any:
        if isinstance(typ, type_api.Integer):
            return self.rng.randint(0, 1000)

        if isinstance(typ, type_api.Numeric):
            return Decimal(f"{self.rng.uniform(0, 1000):.2f}")

        if isinstance(typ, type_api.Money):
            return Decimal(f"{self.rng.uniform(0, 1000):.2f}")

        if isinstance(typ, type_api.Boolean):
            return self.rng.choice([True, False])

        if isinstance(typ, type_api.String):
            return self.faker.first_name()

        if isinstance(typ, type_api.Date):
            d = self.faker.date_between("-10y", "today")
            return date(d.year, d.month, d.day)

        raise TypeError(f"Unsupported type engine: {typ!r}")

    # ------------------------------------------------------------------
    # Table generation
    # ------------------------------------------------------------------

    def _gen_table(self) -> Table:
        name = f"table_{self.faker.uuid4().hex}"

        cols: list[Column] = []
        for i in range(self.rng.randint(2, 6)):
            typ = self._gen_type()
            cols.append(Column(f"col_{i}", typ))

        return Table(name, self.metadata, *cols)

    def _gen_type(self) -> type_api.TypeEngine:
        return self.rng.choice(
            [
                type_api.Integer(),
                type_api.Numeric(),
                type_api.Money(currency="euro"),
                type_api.Money(currency="dollar"),
                type_api.Date(),
                type_api.Boolean(),
                type_api.String(),
            ]
        )

    # ------------------------------------------------------------------
    # Abstract emission hooks
    # ------------------------------------------------------------------
    @abstractmethod
    def _emit_binary(self, column: Column, op: str, value: Any) -> T:
        raise NotImplementedError

    @abstractmethod
    def _emit_bool(self, op: BoolOp, left: T, right: T) -> T:
        raise NotImplementedError

    @abstractmethod
    def _emit_not(self, expr: T) -> T:
        raise NotImplementedError


# ------------------------------------------------------------------------------
# AST generator (ground truth)
# ------------------------------------------------------------------------------

class ASTGenerator(_BaseGenerator[ColumnElement]):
    """
    Generates real ColumnElement trees.
    """

    def _emit_binary(self, column: Column, op: str, value: Any) -> ColumnElement:
        return BinaryExpression(
            column=column,
            operator=op,
            value=BindParameter(
                key=None,
                value=value,
                type_=column.type_,
            ),
        )

    def _emit_bool(self, op: BoolOp, left: ColumnElement, right: ColumnElement) -> ColumnElement:
        return BooleanClauseList(op, [left, right])

    def _emit_not(self, expr: ColumnElement) -> ColumnElement:
        return UnaryExpression("not", expr)


# ------------------------------------------------------------------------------
# Python expression generator (exec()-able)
# ------------------------------------------------------------------------------

class ExpressionGenerator(_BaseGenerator[str]):
    """
    Generates Python expressions that, when executed,
    produce ColumnElement trees.
    """

    def __init__(
        self,
        seed: int | None = None,
        bool_style: Literal["function", "operator"] = "function",
    ):
        super().__init__(seed)

        if bool_style not in ("function", "operator"):
            raise ValueError("bool_style must be 'function' or 'operator'")

        self.bool_style = bool_style

    def _emit_binary(self, column: Column, op: str, value: Any) -> str:
        col = f"{column.parent.name}.c.{column.name}"

        if op in ("==", "!=", ">", "<"):
            return f"({col} {op} {repr(value)})"

        return f"({col}.{op}({repr(value)}))"

    def _emit_bool(self, op: BoolOp, left: str, right: str) -> str:
        if self.bool_style == "function":
            fn = "and_" if op == "and" else "or_"
            return f"{fn}({left}, {right})"

        symbol = "&" if op == "and" else "|"
        return f"({left} {symbol} {right})"

    def _emit_not(self, expr: str) -> str:
        if self.bool_style == "function":
            return f"not_({expr})"
        return f"(~{expr})"
