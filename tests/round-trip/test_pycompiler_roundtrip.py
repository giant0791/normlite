from tqdm import tqdm

import pytest
import pdb

from normlite.sql.elements import (
    BindParameter,
    ColumnElement,
    BinaryExpression,
    UnaryExpression,
    BooleanClauseList,
    and_,
    or_,
    not_
)
from normlite.sql.schema import Column
from tests.support.ast_equal import ast_equal
from tests.support.exec_utils import exec_expression
from tests.support.generators import ASTGenerator, EntityRandomGenerator, ExpressionGenerator
from tests.reference.compiler import PythonExpressionCompiler, assert_compile_equal

@pytest.fixture
def seed() -> int:
    return 28

@pytest.fixture
def astgen(seed) -> ASTGenerator:
    generator = EntityRandomGenerator.create_astgen(seed)
    return generator

# ------------------------------------------------------------------
# ExpressionGenerator bootstrap tests
# ------------------------------------------------------------------

def test_exprgen_exec_function_style():
    """Basic execution (function style)."""
    gen = ExpressionGenerator(seed=10, bool_style="function")

    expr_src = gen.generate(max_depth=3)
    gen_table = gen.metadata[gen.last_generated_table]

    namespace = {
        "and_": and_,
        "or_": or_,
        "not_": not_,
        gen_table.name: gen_table,          # <----- IMPORTANT: Add the randomly generated table to the namespace
    }

    result = exec_expression(expr_src, namespace)

    assert isinstance(result, ColumnElement)
    
def test_exprgen_exec_operator_style():
    """Basic execution (operator style)."""
    gen = ExpressionGenerator(seed=11, bool_style="operator")

    expr_src = gen.generate(max_depth=3)
    gen_table = gen.metadata[gen.last_generated_table]

    namespace = {
        gen_table.name: gen_table,          # <----- IMPORTANT: Add the randomly generated table to the namespace
    }

    result = exec_expression(expr_src, namespace)

    assert isinstance(result, ColumnElement)

def test_exprgen_parentheses_safety():
    """This test guards against operator-precedence bugs"""
    gen = ExpressionGenerator(seed=12, bool_style="operator")
    namespace = {}

    for _ in range(50):
        expr_src = gen.generate(max_depth=4)
        gen_table = gen.metadata[gen.last_generated_table]

        namespace[gen_table.name] = gen_table

        try:
            result = exec_expression(expr_src, namespace)
        except Exception as e:
            pytest.fail(f"Illegal expression:\n{expr_src}\n{e}")

        assert isinstance(result, ColumnElement)

def test_ast_and_expr_generators_compatible():
    astgen = ASTGenerator(seed=20)
    exprgen = ExpressionGenerator(seed=20)

    ast_expr = astgen.generate(max_depth=4)
    expr_src = exprgen.generate(max_depth=4)
    gen_table = exprgen.metadata[exprgen.last_generated_table]

    namespace = {
        "and_": and_,
        "or_": or_,
        "not_": not_,
        gen_table.name: gen_table,          # <----- IMPORTANT: Add the randomly generated table to the namespace
    }

    expr_expr = exec_expression(expr_src, namespace)

    assert isinstance(ast_expr, ColumnElement)
    assert isinstance(expr_expr, ColumnElement)

# ------------------------------------------------------------
# Failure payload
# ------------------------------------------------------------

class RoundTripFailure(AssertionError):
    def __init__(self, original, source, reconstructed):
        super().__init__(
            "\n\nROUND-TRIP FAILURE\n"
            f"\nOriginal AST:\n{original!r}"
            f"\n\nGenerated Python:\n{source}"
            f"\n\nReconstructed AST:\n{reconstructed!r}"
        )

def ast_pretty(expr: ColumnElement, indent: int = 0) -> str:
    pad = "  " * indent

    if isinstance(expr, Column):
        return f"{pad}Column({expr.parent.name}.{expr.name})"

    if isinstance(expr, BindParameter):
        if expr.callable_ is not None:
            return f"{pad}Bind(callable={expr.callable_.__name__})"
        return f"{pad}Bind(value={expr.value!r})"

    if isinstance(expr, BinaryExpression):
        return (
            f"{pad}Binary({expr.operator})\n"
            f"{ast_pretty(expr.column, indent + 1)}\n"
            f"{ast_pretty(expr.value, indent + 1)}"
        )

    if isinstance(expr, UnaryExpression):
        return (
            f"{pad}Unary({expr.operator})\n"
            f"{ast_pretty(expr.element, indent + 1)}"
        )

    if isinstance(expr, BooleanClauseList):
        lines = [f"{pad}Bool({expr.operator})"]
        for c in expr.clauses:
            lines.append(ast_pretty(c, indent + 1))
        return "\n".join(lines)

    return f"{pad}{expr!r}"

class ASTDiff:
    def __init__(self, path: str, reason: str, left, right):
        self.path = path
        self.reason = reason
        self.left = left
        self.right = right

    def __str__(self):
        return (
            f"AST DIFFERENCE at {self.path}\n"
            f"Reason: {self.reason}\n"
            f"Left:\n{ast_pretty(self.left)}\n\n"
            f"Right:\n{ast_pretty(self.right)}"
        )

def ast_diff(a, b, path="root") -> ASTDiff | None:
    if type(a) is not type(b):
        return ASTDiff(path, "type mismatch", a, b)

    if isinstance(a, Column):
        if a.name != b.name or a.parent.name != b.parent.name:
            return ASTDiff(path, "column mismatch", a, b)
        return None

    if isinstance(a, BindParameter):
        if a.callable_ or b.callable_:
            if a.callable_ is not b.callable_:
                return ASTDiff(path, "callable mismatch", a, b)
        elif a.value != b.value:
            return ASTDiff(path, "bind value mismatch", a, b)
        return None

    if isinstance(a, BinaryExpression):
        if a.operator != b.operator:
            return ASTDiff(path, "binary operator mismatch", a, b)

        return (
            ast_diff(a.column, b.column, f"{path}.column")
            or ast_diff(a.value, b.value, f"{path}.value")
        )

    if isinstance(a, UnaryExpression):
        if a.operator != b.operator:
            return ASTDiff(path, "unary operator mismatch", a, b)

        return ast_diff(a.element, b.element, f"{path}.element")

    if isinstance(a, BooleanClauseList):
        if a.operator != b.operator:
            return ASTDiff(path, "boolean operator mismatch", a, b)

        if len(a.clauses) != len(b.clauses):
            return ASTDiff(path, "clause count mismatch", a, b)

        for i, (ac, bc) in enumerate(zip(a.clauses, b.clauses)):
            diff = ast_diff(ac, bc, f"{path}.clauses[{i}]")
            if diff:
                return diff

        return None

    return ASTDiff(path, "unknown node type", a, b)

# ------------------------------------------------------------
# Massive differential test
# ------------------------------------------------------------

def test_pycompiler_roundtrip_massive(astgen: ASTGenerator):
    """
    AST → Python → AST massive round-trip differential test.

    This test is intentionally expensive.
    Run only with:  pytest -m massive
    """

    NUM_ITERATIONS = 10_000
    MAX_DEPTH = 8

    compiler = PythonExpressionCompiler()

    # shared namespace (tables added dynamically)
    namespace = {}

    print()

    for i in tqdm(range(NUM_ITERATIONS), desc="AST round-trip"):
        # --------------------------------------------------
        # 1. Generate AST
        # --------------------------------------------------
        expr = astgen.generate(max_depth=MAX_DEPTH)
        #pdb.set_trace()

        # retrieve generated table
        gen_table = astgen._impl.metadata[astgen._impl.last_generated_table]
        namespace[gen_table.name] = gen_table

        # --------------------------------------------------
        # 2. Compile to Python
        # --------------------------------------------------
        source = compiler.compile(expr)

        # --------------------------------------------------
        # 3. Execute Python → AST
        # --------------------------------------------------
        reconstructed = exec_expression(source, namespace)

        # --------------------------------------------------
        # 4. Structural equality
        # --------------------------------------------------
        diff = ast_diff(expr, reconstructed)
        if diff:
            print("\n=== GENERATED PYTHON ===\n", source)
            print("\n=== AST DIFF ===\n", diff)
            raise RoundTripFailure(expr, source, reconstructed)
