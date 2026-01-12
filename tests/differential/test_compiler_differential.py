from datetime import datetime, date
from decimal import Decimal
from tqdm import tqdm

import pytest
import pdb
import statistics

from normlite.sql.compiler import NotionCompiler
from normlite.sql.elements import (
    BindParameter,
    ColumnElement,
    BinaryExpression,
    Operator,
    UnaryExpression,
    BooleanClauseList,
    and_,
    or_,
    not_
)
from normlite.sql.schema import Column, Table
from tests.support.ast_equal import ast_equal
from tests.support.exec_utils import exec_expression
from tests.support.generators import ASTGenerator, ExpressionGenerator, ReferenceGenerator, EntityRandomGenerator
from tests.reference.compiler import PythonExpressionCompiler, ReferenceCompiler, assert_compile_equal


@pytest.fixture
def seed() -> int:
    return 28

@pytest.fixture
def ref_compiler() -> ReferenceCompiler:
    return ReferenceCompiler()

@pytest.fixture
def prod_compiler() -> NotionCompiler:
    return NotionCompiler()

@pytest.fixture
def astgen(seed) -> ASTGenerator:
    generator = EntityRandomGenerator.create_astgen(seed)
    return generator

def is_iso_format(date_val: dict) -> bool:
    date_str = None
    try:
        date_str = date_val['date']['start']
    except KeyError:
        return False

    try:
        datetime.fromisoformat(date_str)
        return True
    except ValueError:
        return False
    
# ------------------------------------------------------------------
# ASTGenerator bootstrap tests
# ------------------------------------------------------------------
    
def walk_ast(expr: ColumnElement):
    yield expr
    if isinstance(expr, BinaryExpression):
        yield from walk_ast(expr.column)
        yield from walk_ast(expr.value)
    elif isinstance(expr, UnaryExpression):
        yield from walk_ast(expr.element)
    elif isinstance(expr, BooleanClauseList):
        for c in expr.clauses:
            yield from walk_ast(c)

def test_astgen_basic():
    """Basic generation."""
    gen = ASTGenerator(seed=1)

    expr = gen.generate(max_depth=3)

    assert isinstance(expr, ColumnElement)

def test_astgen_structure():
    """Structural validity."""
    gen = ASTGenerator(seed=2)

    expr = gen.generate(max_depth=4)

    for node in walk_ast(expr):
        assert isinstance(node, ColumnElement)

def test_astgen_operator_legality():
    """Operator legality.
    
    Ensures no invalid operator strings leak in.
    """
    gen = ASTGenerator(seed=3)

    expr = gen.generate(max_depth=5)

    for node in walk_ast(expr):
        if isinstance(node, BinaryExpression):
            assert isinstance(node.operator, Operator)
        if isinstance(node, UnaryExpression):
            assert node.operator == "not"

def depth(expr: ColumnElement) -> int:
    if isinstance(expr, (BinaryExpression,)):
        return 1
    if isinstance(expr, UnaryExpression):
        return 1 + depth(expr.element)
    if isinstance(expr, BooleanClauseList):
        return 1 + max(depth(c) for c in expr.clauses)
    return 1

def test_astgen_depth_respected():
    """Depth is bound."""
    gen = ASTGenerator(seed=4)

    for max_depth in range(1, 6):
        expr = gen.generate(max_depth=max_depth)
        assert depth(expr) <= max_depth + 1

def test_gen_table():
    """Table generation."""
    MAX_COLS = 32
    gen = ASTGenerator(seed=3)
    table: Table = gen._gen_table(max_cols=MAX_COLS)
    
    assert table.name.startswith('table_')
    assert len(table.get_user_defined_colums()) <= MAX_COLS
    # not all columns comply to the generation naming scheme: special columns are added behind the scenes
    assert all([col_name.startswith('col_') for col_name in table.get_user_defined_colums().keys()])

def test_compile_differential_random_dryrun(astgen, ref_compiler, prod_compiler):
    expr = astgen.generate(4)

    assert_compile_equal(expr, ref_compiler.process, prod_compiler.process)

def test_compile_differential_random_massive(astgen, ref_compiler, prod_compiler):
    MAX_COLS = 16
    MAX_DEPTH = 32
    MAX_TREES = 10_000
    depths = []

    print()         # <--- CRITICAL: Force a newline so tqdm doesn't overwrite the test name
    for _ in tqdm(range(MAX_TREES), desc="Comparing ref vs prod compilers", unit="tree"):
        depth = astgen._impl.rng.randint(1, MAX_DEPTH)
        expr = astgen.generate(max_cols=MAX_COLS, max_depth=depth)
        depths.append(depth)
        assert_compile_equal(expr, ref_compiler.process, prod_compiler.process)

    print('\n')
    print(f'Expression depth stats: min = {min(depths)}, max = {max(depths)}, mean = {statistics.mean(depths)}')

