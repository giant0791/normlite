from __future__ import annotations
from decimal import Decimal
import pdb
import uuid
from faker import Faker
import random
import math
from collections import defaultdict
from typing import Any, Dict, Iterable, Sequence, Generic, TypeVar, Protocol

from normlite._constants import SpecialColumns
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode
from normlite.sql import type_api
from normlite.sql.elements import BinaryExpression, BindParameter, BooleanClauseList, ColumnElement, UnaryExpression
from normlite.sql.schema import Column, MetaData, Table

def generate_iso_string(include_time: bool = True) -> str:
    """
    Generates a random ISO 8601 string.
    
    :param include_time: If True, returns a full datetime string (YYYY-MM-DDTHH:MM:SS).
                         If False, returns a date-only string (YYYY-MM-DD).
    """
    if include_time:
        # Generates a random datetime object and converts to ISO string
        return fake.date_time().isoformat()
    else:
        # Generates a random date object and converts to ISO string
        return fake.date().isoformat() if hasattr(fake.date(), 'isoformat') else str(fake.date())
    
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

    TYPES = {
        "title",
        "rich_text",
        "number",
        "date",
        "checkbox",
    }

    COL_TYPES = {
        DBAPITypeCode.NUMBER_DOLLAR.value,
        DBAPITypeCode.NUMBER_WITH_COMMAS,
    }

    ALL_COL_TYPES = TYPES | COL_TYPES 

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

class ReferenceGenerator:
    """Reference workload generator for filters and pages in ``normlite``.

    :class:`ReferenceGenerator` is **single source of truth** for types, operators, and value generation.
    It provides **bi-derectional compatibility** page <--> filters semantics.
    It is exendible: Adding a new type touches *one place* only.
    It is deterministic: Seedable RNG + Faker
    """

    def __init__(self, seed: int | None = None):
        self.rng = random.Random(seed)
        self.faker = Faker()
        if seed is not None:
            self.faker.seed_instance(seed)

        self.coverage = CoverageRegistry()
        self.metadata = MetaData()

    def gen_schema(self, min_props=1, max_props=6) -> dict:
        schema = {}
        n = self.rng.randint(min_props, max_props)

        for i in range(n):
            name = f"prop_{i}"
            typ = self.rng.choice(tuple(self.TYPES))
            schema[name] = {"type": typ}

        return schema
    
    def gen_table(self, min_cols=1, max_cols=6) -> Table:
        cols = []
        n = self.rng.randint(min_cols, max_cols)

        for i in range(n):
            typ = self.rng.choice(tuple(self.ALL_COL_TYPES))
            cols.append(
                Column(
                    name=f"col_{i}",
                    type_=type_api.type_mapper[typ],
                    primary_key=(i == 0)
                )
            )
        
        safe_uuid = self.faker.uuid4().replace("-", "_")
        table = Table(f'table_{safe_uuid}', self.metadata, *cols)
        return table
    
    def _gen_bindparam_value(self, typ: str) -> Any:
        value_obj = self._gen_property_value(typ)
        val = None
        if typ == 'date':
            start = value_obj.get('start')
            return start if start is not None else None                   

        elif typ in ('number_with_commas', 'dollar',):
            val = value_obj['number']
        
        elif typ in ('title', 'rich_text',):
            val = value_obj[typ][0]['text']['content'] 

        else:
            val = value_obj[typ]

        return val
    
    def gen_binary_expr(self, columns: Sequence[Column]) -> BinaryExpression:
        col = self.rng.choice(columns)
        typ = col.type_.get_col_spec()

        op = self.rng.choice(tuple(self.OPERATORS[typ]))

        return BinaryExpression(
            column=col,
            operator=op,
            value=BindParameter(
                key=None, 
                value=self._gen_bindparam_value(typ)        # IMPORTANT: This must mimic the coerce_to_bindparam() behavior
            )      
        )

    def gen_expr(
        self,
        columns: Sequence[Column],
        depth: int,
        max_depth: int,
    ) -> tuple[ColumnElement, int]:

        # forced leaf at max depth
        if depth >= max_depth:
            expr = self.gen_binary_expr(columns)
            return expr, 1

        choice = self.rng.random()

        # leaf
        if choice < 0.4:
            expr = self.gen_binary_expr(columns)
            return expr, 1

        # unary
        if choice < 0.55:
            inner, inner_depth = self.gen_expr(
                columns, depth + 1, max_depth
            )
            return UnaryExpression("not", inner), inner_depth + 1

        # boolean
        left, left_depth = self.gen_expr(
            columns, depth + 1, max_depth
        )
        right, right_depth = self.gen_expr(
            columns, depth + 1, max_depth
        )

        expr = BooleanClauseList(
            operator=self.rng.choice(["and", "or"]),
            clauses=[left, right],
        )

        return expr, max(left_depth, right_depth) + 1
    
    def gen_ast(self, min_cols=1, max_cols=16, max_depth=4) -> tuple[Table, ColumnElement, int]:
        table = self.gen_table(min_cols, max_cols)
        columns = table.get_user_defined_colums()
        
        # bias toward extrem depth
        if self.rng.random() < 0.6:
            max_depth = self.rng.randint(20, 60)
        

        expr, exdepth = self.gen_expr(columns, depth=0, max_depth=max_depth)
        return table, expr, exdepth
            
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

class ExpressionGenerator:
    """
    Generates *Python column expressions* like:

        start_on.after(date(2025, 1, 1))
        grade <= Decimal("1.5")
        isactive.is_(True)
        name.startswith("Ann")

    .. attention::
        :class:`ExpressionGenerator` shall only be used for random generation of
        strings containing **Python code**.

    This generator is:
    - type aware
    - deterministic (seedable)
    - safe to exec()
    """

    METHOD_OPS = {
        type_api.Integer: [
            "is_empty",
            "is_not_empty",
        ],
        type_api.Numeric: [
            "is_empty",
            "is_not_empty",
        ],
        type_api.Money: [
            "is_empty",
            "is_not_empty",
        ],
        type_api.Date: [
            "after",
            "before",
            "is_empty",
            "is_not_empty",
        ],
        type_api.Boolean: [
            "is_",
            "is_not",
        ],
        type_api.String: [
            "in_",
            "not_in",
            "startswith",
            "endswith",
            "is_empty",
            "is_not_empty",
        ],
    }

    INFIX_OPS = {
        type_api.Integer: ["==", "!=", "<", ">"],
        type_api.Numeric: ["==", "!=", "<", ">"],
        type_api.Money:   ["==", "!=", "<", ">"],
        type_api.Date:    ["==", "!="],
        type_api.Boolean: ["==", "!="],
        type_api.String:  ["==", "!="],
    }

    BOOL_OPS = {
        "&": "and_",
        "|": "or_",
        "~": "not_",
    }

    TYPES = {
        "title",
        "rich_text",
        "number",
        "date",
        "checkbox",
    }

    COL_TYPES = {
        DBAPITypeCode.NUMBER_DOLLAR.value,
        DBAPITypeCode.NUMBER_WITH_COMMAS,
    }

    ALL_COL_TYPES = TYPES | COL_TYPES 

    def __init__(
        self, 
        seed: int | None = None,
        bool_style: str = 'function' 
    ):
        self.rng = random.Random(seed)
        self.faker = Faker()
        if seed is not None:
            self.faker.seed_instance(seed)

        if bool_style not in ['function', 'operator']:
            raise ValueError(f'Unsupported bool_style value: {bool_style}')
        
        self.bool_style = bool_style
        self.metadata = MetaData()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self, table: Table, max_depth: int = 8) -> str:
        """
        Generate a parenthesized boolean expression with depth <= max_depth.

        Example output:
            ((age > 18) & (name.startswith("A")))
            (created.after(date(2020, 1, 1)) | isactive.is_(True))
        """
        if max_depth < 0:
            raise ValueError("max_depth must be >= 0")

        if not table:
            raise ValueError("generate() requires a Table object")
        
        columns = [col for col in table.columns if col.name not in SpecialColumns.values()]
        return self._gen_expr(columns, max_depth)

    def gen_table(self, min_cols=1, max_cols=6) -> Table:
        cols = []
        n = self.rng.randint(min_cols, max_cols)

        for i in range(n):
            typ = self.rng.choice(tuple(self.ALL_COL_TYPES))
            cols.append(
                Column(
                    name=f"col_{i}",
                    type_=type_api.type_mapper[typ],
                    primary_key=(i == 0)
                )
            )
        
        table = Table(f'table_{self.faker.uuid4()}', self.metadata, *cols)
        return table
 
    def gen_column_expr(self, column: Column) -> str:
        """
        Generate a valid Python expression string for a single Column.
        """
        typ = type(column.type_)

        candidates = []
        if typ in self.METHOD_OPS:
            candidates.extend(("method", op) for op in self.METHOD_OPS[typ])
        if typ in self.INFIX_OPS:
            candidates.extend(("infix", op) for op in self.INFIX_OPS[typ])

        if not candidates:
            raise ValueError(f"No operators available for type {typ}")

        kind, op = self.rng.choice(candidates)

        if kind == "method":
            return self._gen_method_expr(column, op)
        else:
            return self._gen_infix_expr(column, op)

    # ------------------------------------------------------------------
    # Expression builders
    # ------------------------------------------------------------------

    def _gen_expr(self, columns: list, depth: int) -> str:
        """
        Generate an expression with maximum remaining depth.
        """
        # Base case: must emit a column expression
        if depth == 0:
            return self._gen_atomic(columns)

        # Bias toward leaf nodes to avoid degenerate deep trees
        choice = self.rng.choices(
            population=["leaf", "and", "or", "not"],
            weights=[3, 2, 2, 1],
            k=1,
        )[0]

        if choice == "leaf":
            return self._gen_atomic(columns)

        if choice == "not":
            expr = self._gen_expr(columns, depth - 1)
            if self.bool_style == "operator":
                return f"(~({expr}))"
            return f"not_({expr})"        
    
        left = self._gen_expr(columns, depth - 1)
        right = self._gen_expr(columns, depth - 1)

        op = "&" if choice == "and" else "|"

        if self.bool_style == "operator":
            return f"(({left}) {op} ({right}))"

        fn = self.BOOL_OPS[op]
        return f"{fn}({left}, {right})"

    def _gen_atomic(self, columns: list) -> str:
        column = self.rng.choice(columns)
        expr = self.gen_column_expr(column)

        # CRITICAL: always parenthesize atomic expressions
        return f"({expr})"

    def _gen_method_expr(self, column: Column, op: str) -> str:
        if op in ("is_empty", "is_not_empty"):
            return f"{column.parent.name}.c.{column.name}.{op}()"

        value = self._gen_value(column.type_)
        return f"{column.parent.name}.c.{column.name}.{op}({value})"

    def _gen_infix_expr(self, column: Column, op: str) -> str:
        value = self._gen_value(column.type_)
        return f"{column.parent.name}.c.{column.name} {op} {value}"

    # ------------------------------------------------------------------
    # Literal generation
    # ------------------------------------------------------------------

    def _gen_value(self, type_engine: type_api.TypeEngine) -> str:
        """
        Generate a Python literal suitable for the given TypeEngine.
        """
        if isinstance(type_engine, type_api.Integer):
            return str(self.rng.randint(0, 1000))

        if isinstance(type_engine, type_api.Numeric):
            return f"Decimal('{self.rng.uniform(0, 1000):.2f}')"

        if isinstance(type_engine, type_api.Money):
            return f"Decimal('{self.rng.uniform(0, 1000):.2f}')"

        if isinstance(type_engine, type_api.Boolean):
            return "True" if self.rng.choice([True, False]) else "False"

        if isinstance(type_engine, type_api.String):
            return repr(self.faker.first_name())

        if isinstance(type_engine, type_api.Date):
            d = self.faker.date_between("-10y", "today")
            return f"date({d.year}, {d.month}, {d.day})"

        raise TypeError(f"Unsupported type engine: {type_engine!r}")

