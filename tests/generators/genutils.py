import pdb
from faker import Faker
import random
import math
from collections import defaultdict
from typing import Dict, Iterable, List

from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode
from normlite.sql import type_api
from normlite.sql.schema import Column, MetaData, Table

fake = Faker()

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


class ReferenceGenerator:
    """Reference workload gerator for filters and pages in ``normlite``.

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
        
        table = Table(f'table_{self.faker.uuid4()}', self.metadata, *cols)
        return table
            
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
    

