import pdb
import random

from normlite.sql.elements import ColumnElement, UnaryExpression
from normlite.sql.schema import Column
from normlite.sql.type_api import Integer, String, TypeEngine
from normlite.sql.elements import UnaryExpression, BinaryExpression, BooleanClauseList, BindParameter

SCHEMA = {
    "name": "title",
    "grade": "rich_text",
    "student_id": "number",
}

STRING_TYPES = ["title", "rich_text"]
NUMBER_TYPES = ["number"]
BOOL_TYPES = ["checkbox"]
DATE_TYPES = ["date"]
ALL_TYPES = STRING_TYPES + NUMBER_TYPES + BOOL_TYPES + DATE_TYPES

OPS_BY_TYPE = {
    "title": ["equals", "contains", "starts_with", "ends_with"],
    "rich_text": ["equals", "contains", "starts_with", "ends_with"],
    "number": ["equals", "greater_than", "less_than"],
    "checkbox": []
}

def create_type_engine(typ: str) -> TypeEngine:
    if typ == 'number':
        return Integer()
    
    if typ == 'title':
        return String(is_title=True)
    
    if typ == 'rich_text':
        return String()
    
    raise AssertionError(f'Unsupported type: {typ}')


def gen_schema(
    rng: random.Random,
    min_cols=2,
    max_cols=6,
) -> list[Column]:
    cols = []
    n = rng.randint(min_cols, max_cols)

    for i in range(n):
        typ = rng.choice(ALL_TYPES)
        cols.append(
            Column(
                name=f"col_{i}",
                type_= create_type_engine(typ),
                primary_key=(i == 0)
            )
        )
    return cols

def gen_value_for_type(rng, typ):
    if typ in STRING_TYPES:
        return rng.choice(["Alice", "Bob", "Galilei", "Newton"])
    if typ == "number":
        return rng.randint(0, 1000)
    raise AssertionError("unknown type")

def gen_binary_expr(rng, columns):
    col = rng.choice(columns)
    typ = col.type_.get_col_spec()

    op = rng.choice(OPS_BY_TYPE[typ])
    val = gen_value_for_type(rng, typ)

    return BinaryExpression(
        column=col,
        operator=op,
        value=BindParameter(None, val)      # IMPORTANT: This must mimic the coerce_to_bindparam() behavior
    )

def gen_expr(
    rng: random.Random,
    columns: list[Column],
    depth: int,
    max_depth: int,
) -> tuple[ColumnElement, int]:

    # forced leaf at max depth
    if depth >= max_depth:
        expr = gen_binary_expr(rng, columns)
        return expr, 1

    choice = rng.random()

    # leaf
    if choice < 0.4:
        expr = gen_binary_expr(rng, columns)
        return expr, 1

    # unary
    if choice < 0.55:
        inner, inner_depth = gen_expr(
            rng, columns, depth + 1, max_depth
        )
        return UnaryExpression("not", inner), inner_depth + 1

    # boolean
    left, left_depth = gen_expr(
        rng, columns, depth + 1, max_depth
    )
    right, right_depth = gen_expr(
        rng, columns, depth + 1, max_depth
    )

    expr = BooleanClauseList(
        operator=rng.choice(["and", "or"]),
        clauses=[left, right],
    )

    return expr, max(left_depth, right_depth) + 1

def gen_ast(
    rng: random.Random,
    max_depth=4,
):
    columns = gen_schema(rng)

    # bias toward extrem depth
    if rng.random() < 0.1:
        max_depth = rng.randint(10, 30)

    expr, exdepth = gen_expr(rng, columns, depth=0, max_depth=rng.randint(10, 30))
    return columns, expr, exdepth
