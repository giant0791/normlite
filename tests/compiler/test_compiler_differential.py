from datetime import datetime
import random
import statistics
from typing import Union

import pytest
from tqdm import tqdm

from normlite.sql.compiler import NotionCompiler
from normlite.sql.elements import BinaryExpression
from normlite.sql.schema import Table
from tests.generators.genutils import ReferenceGenerator
from tests.generators.schema import gen_ast
from tests.reference.compiler import ReferenceCompiler, assert_compile_equal


@pytest.fixture
def seed() -> int:
    return 10_000

@pytest.fixture
def ref_compiler() -> ReferenceCompiler:
    return ReferenceCompiler()

@pytest.fixture
def prod_compiler() -> NotionCompiler:
    return NotionCompiler()

def is_iso_format(date_val: str) -> bool:
    try:
        datetime.fromisoformat(date_val)
        return True
    except ValueError:
        return False

def test_compile_differential(seed, ref_compiler, prod_compiler):
    rng = random.Random(seed)

    _, expr = gen_ast(rng)

    assert_compile_equal(expr, ref_compiler.process, prod_compiler.process)

def test_gen_table(seed):
    refgen = ReferenceGenerator(seed)
    MAX_COLS = 32
    table: Table = refgen.gen_table(max_cols=MAX_COLS)
    
    assert table.name.startswith('table_')
    assert len(table.columns) <= MAX_COLS
    # not all columns comply to the generation naming scheme: special columns are added behind the scenes
    assert all([col_name.startswith('col_') for col_name in table.get_non_special_colums().keys()])

def test_gen_bindparam_value(seed):
    refgen = ReferenceGenerator(seed)
    MAX_VALUES = 100
    date_values = [refgen._gen_bindparam_value('date') for _ in range(MAX_VALUES)]

    assert all([
        (val == {} or is_iso_format(val))
        for val in date_values
    ])

def test_gen_bin_expr(seed):
    refgen = ReferenceGenerator(seed)
    MAX_COLS = 32
    MAX_EXPRS = 100
    table: Table = refgen.gen_table(max_cols=MAX_COLS)
    binexprs = [refgen.gen_binary_expr(table.get_user_defined_colums()) for _ in range(MAX_EXPRS)]

    assert all([expr.column.name in table.columns for expr in binexprs])

def test_compile_differential_massive(seed, ref_compiler, prod_compiler):
    refgen = ReferenceGenerator(seed)
    MAX_COLS = 32
    MAX_TREES = 10_000
    depths = []
    
    for _ in tqdm(range(MAX_TREES), desc="Comparing ref vs prod compilers", unit="tree"):
        _, expr, depth = refgen.gen_ast(max_cols=MAX_COLS)
        depths.append(depth)
        assert_compile_equal(expr, ref_compiler.process, prod_compiler.process)

    print('\n')
    print(f'Expression depth stats: min = {min(depths)}, max = {max(depths)}, mean = {statistics.mean(depths)}')