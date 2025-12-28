import random
from faker import Faker
from tests.generators.schema import SCHEMA

fake = Faker()

OPS = {
    "title": ["contains", "starts_with", "ends_with"],
    "rich_text": ["equals"],
    "number": ["greater_than", "less_than", "equals"],
}

def generate_condition(schema):
    prop = random.choice(list(schema))
    typ = schema[prop]
    op = random.choice(OPS[typ])

    val = fake.first_name() if typ != "number" else random.randint(0, 1000)

    return {"property": prop, typ: {op: val}}

def generate_filter(schema, depth=0, max_depth=3):
    if depth >= max_depth or random.random() < 0.4:
        return generate_condition(schema)

    op = random.choice(["and", "or", "not"])
    if op == "not":
        return {"not": generate_filter(schema, depth + 1)}
    return {op: [generate_filter(schema, depth + 1),
                 generate_filter(schema, depth + 1)]}
