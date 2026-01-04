import random
import statistics

import pytest

from normlite.sql.compiler import NotionCompiler
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
    assert not all([col_name.startswith('col_') for col_name in table.columns.keys()])

def test_stress_compile_differential(seed, ref_compiler, prod_compiler):
    rng = random.Random(seed)
    depths = []
    
    for _ in range(100000):
        _, expr, depth = gen_ast(rng)
        depths.append(depth)
        assert_compile_equal(expr, ref_compiler.process, prod_compiler.process)

    print('\n')
    print(f'Expression depth stats: min = {min(depths)}, max = {max(depths)}, mean = {statistics.mean(depths)}')