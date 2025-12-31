import pdb
from faker import Faker
import random

fake = Faker()

def generate_page(schema):
    props = {}
    for prop, typ in schema.items():
        if typ == "title":
            props[prop] = {"type": "title", "title": [{"text": {"content": fake.name()}}]}
        elif typ == "rich_text":
            props[prop] = {"type": "rich_text", "rich_text": [{"text": {"content": random.choice(["A","B","C"])}}]}
        elif typ == "number":
            props[prop] = {"type": "number", "number": random.randint(0, 1000)}
    return {"properties": props}

def generate_pages(schema, n=50):
    return [generate_page(schema) for _ in range(n)]
