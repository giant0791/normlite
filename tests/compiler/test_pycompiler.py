from decimal import Decimal
from datetime import date
import pdb
import pytest

from normlite.sql.elements import ColumnElement
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, Money, Numeric
from tests.generators.genutils import ExpressionGenerator
from tests.reference.compiler import ast_equal, exec_expression

@pytest.fixture
def col() -> Column:
    return Column('noname', Integer())


metadata = MetaData()

students = Table(
    "students",
    metadata,
    Column("id", Integer()),
    Column("grade", Numeric()),
    Column("balance", Money(currency="euro")),
    Column("start_on", Date()),
    Column("is_active", Boolean()),
)


def test_ast_structural_equality_simple(col: Column):
    a = col == 1
    b = col == 1
    assert ast_equal(a, b)

def test_ast_structural_inequality_simple(col: Column):
    a = col == 1
    b = col == 2
    assert not ast_equal(a, b)

def test_ast_boolean_ordering(col: Column):
    a = (col == 1) & (col == 2)
    b = (col == 2) & (col == 1)
    assert not ast_equal(a, b)

def test_exprgen_smoke():
    gen = ExpressionGenerator(seed=1)

    expr = gen.generate(students)

    assert isinstance(expr, str)
    assert expr.strip()

    namespace = {
        "students": students,
        "Decimal": Decimal,
        "date": date,
    }

    result = exec_expression(expr, namespace)

    assert isinstance(result, ColumnElement)

def test_exprgen_deterministic():
    gen1 = ExpressionGenerator(seed=42)
    gen2 = ExpressionGenerator(seed=42)

    e1 = gen1.generate(students)
    e2 = gen2.generate(students)

    assert e1 == e2

def test_exprgen_operator_legality():
    gen = ExpressionGenerator(seed=10)

    namespace = {
        "students": students,
        "Decimal": Decimal,
        "date": date,
    }

    for _ in range(200):
        expr = gen.generate(students)
        try:
            exec_expression(expr, namespace)
        except Exception as e:
            pytest.fail(f"Illegal expression:\n{expr}\n{e}")

