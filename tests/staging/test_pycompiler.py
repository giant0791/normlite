from decimal import Decimal
from datetime import date
import pdb
import pytest

from normlite.sql.elements import BinaryExpression, ColumnElement, and_, not_, or_
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, Money, Numeric, String
from tests.generators.genutils import ExpressionGenerator
from tests.reference.compiler import ast_equal, exec_expression

@pytest.fixture
def col() -> Column:
    return Column('noname', Integer())

@pytest.fixture
def students_table():
    metadata = MetaData()

    return Table(
        "students",
        metadata,
        Column("age", Integer()),
        Column("grade", Numeric()),
        Column("balance", Money(currency="euro")),
        Column("start_on", Date()),
        Column("isactive", Boolean()),
        Column("name", String()),
    )

@pytest.fixture
def exec_namespace(students_table):
    return {
        "students": students_table,
        "Decimal": Decimal,
        "date": date,
        "and_": and_,
        "or_": or_,
        "not_": not_,
    }

def test_binexpr(students_table, exec_namespace):
    pystr = "(students.c.age > 18)"
    result = exec_expression(pystr, exec_namespace)

    assert isinstance(result, BinaryExpression)

def test_exprgen_smoke(students_table, exec_namespace):
    """Smoke test — does it even run?
    
    Goal: Ensure generated expressions:

    * are strings

    * compile

    * execute

    * return a ColumnElement
    """
    
    gen = ExpressionGenerator(seed=1)

    expr = gen.generate(students_table)

    assert isinstance(expr, str)
    assert expr.strip()

    result = exec_expression(expr, exec_namespace)
    assert isinstance(result, ColumnElement)

def test_exprgen_deterministic(students_table):
    """Determinism test — seed guarantees reproducibility

    Goal: Same seed → same expression
    """

    gen1 = ExpressionGenerator(seed=42)
    gen2 = ExpressionGenerator(seed=42)

    e1 = gen1.generate(students_table, max_depth=1)
    e2 = gen2.generate(students_table, max_depth=1)

    assert e1 == e2

def test_exprgen_operator_legality(students_table, exec_namespace):
    """Operator legality test (no illegal combos)

    Goal: Ensure:

    * no .after() on Numeric

    * no < on Boolean

    * no .contains() on Date
    """

    gen = ExpressionGenerator(seed=10)

    for _ in range(2000):
        expr = gen.generate(students_table, max_depth=1)
        try:
            exec_expression(expr, exec_namespace)
        except Exception as e:
            pytest.fail(f"Illegal expression:\n{expr}\n{e}")

@pytest.mark.parametrize("bool_style", ["operator", "function"])
def test_expression_generator_executes(students_table, exec_namespace, bool_style):
    gen = ExpressionGenerator(seed=42, bool_style=bool_style)

    for _ in range(2000):
        expr = gen.generate(students_table, max_depth=4)
        try:
            result = exec_expression(expr, exec_namespace)
        except Exception as e:
            pytest.fail(
                f"Execution failed for bool_style={bool_style}\n"
                f"Expression:\n{expr}\n"
                f"Error: {e}"
            )

def test_operator_vs_function_style_equivalence(students_table, exec_namespace):
    gen_op = ExpressionGenerator(seed=123, bool_style="operator")
    gen_fn = ExpressionGenerator(seed=123, bool_style="function")

    for _ in range(2000):
        expr_op = gen_op.generate(students_table, max_depth=5)
        expr_fn = gen_fn.generate(students_table, max_depth=5)

        ast_op = exec_expression(expr_op, exec_namespace)
        ast_fn = exec_expression(expr_fn, exec_namespace)

        if not ast_equal(ast_op, ast_fn):
            pytest.fail(
                "AST mismatch between boolean styles\n\n"
                f"OPERATOR:\n{expr_op}\n\n"
                f"FUNCTION:\n{expr_fn}\n"
            )

def test_not_operator_generation(students_table, exec_namespace):
    gen = ExpressionGenerator(seed=99, bool_style="function")

    seen_not = False

    for _ in range(3000):
        expr = gen.generate(students_table, max_depth=4)
        if "not_(" in expr:
            seen_not = True
            result = exec_expression(expr, exec_namespace)
            assert result is not None

    assert seen_not, "NOT operator was never generated"

def test_max_depth_zero_is_atomic(students_table, exec_namespace):
    gen = ExpressionGenerator(seed=7, bool_style="operator")

    for _ in range(50):
        expr = gen.generate(students_table, max_depth=0)
        result = exec_expression(expr, exec_namespace)
        assert result is not None


def test_exprgen_no_bare_columns(students_table):
    """Structural sanity test — no bare columns

    Goal: Generated expressions must not be just students.c.col.
    """

    gen = ExpressionGenerator(seed=5)

    for _ in range(100):
        expr = gen.generate(students_table, max_depth=3)
        assert ".c." in expr
        assert "(" in expr or "&" in expr or "|" in expr

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

def test_ast_inequality_bug():
    meta = MetaData()
    col = Column('col_0', Numeric(), primary_key=True)
    table_cd613e30 = Table('table_cd613e30', meta, col)
    a = (table_cd613e30.c.col_0 < 779)
    b = (table_cd613e30.c.col_0 < 779)

    assert ast_equal(a, b)
    
