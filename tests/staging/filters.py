import random
from typing import Any
from faker import Faker
from tests.generators.schema import SCHEMA

fake = Faker()

OPS = {
    "title": ["contains", "starts_with", "ends_with"],
    "rich_text": ["equals"],
    "number": ["greater_than", "less_than", "equals"],
    "date": ["after", "before", "is_empty", "is_not_empty"],
}

def gen_date_value(op):
    if op == "is_empty":
        return random.choice(["true", "false"])
    else:
        # ISO 8601 date string
        return fake.date_between(start_date="-5y", end_date="today").isoformat()

def gen_checkbox_value():
    return random.choice([True, False])

def generate_condition(schema):
    prop = random.choice(list(schema))
    typ = schema[prop]
    op = random.choice(OPS[typ])

    if typ == "number":
        val = random.randint(0, 1000)

    elif typ in ("title", "rich_text"):
        val = fake.first_name()

    elif typ == "date":
        val = gen_date_value(op)

    elif typ == "checkbox":
        val = gen_checkbox_value()

    else:
        raise ValueError(f"Unsupported property type: {typ}")

    return {
        "property": prop,
        typ: {op: val},
    }

def generate_filter(schema, depth=0, max_depth=3):
    if depth >= max_depth or random.random() < 0.4:
        return generate_condition(schema)

    op = random.choice(["and", "or", "not"])
    if op == "not":
        return {"not": generate_filter(schema, depth + 1)}
    return {op: [generate_filter(schema, depth + 1),
                 generate_filter(schema, depth + 1)]}
