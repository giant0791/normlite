import random
import statistics

import pytest

from normlite.sql.compiler import NotionCompiler
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


def test_stress_compile_differential(seed, ref_compiler, prod_compiler):
    rng = random.Random(seed)
    depths = []
    
    for _ in range(100000):
        _, expr, depth = gen_ast(rng)
        depths.append(depth)
        assert_compile_equal(expr, ref_compiler.process, prod_compiler.process)

    print('\n')
    print(f'Expression depth stats: min = {min(depths)}, max = {max(depths)}, mean = {statistics.mean(depths)}')