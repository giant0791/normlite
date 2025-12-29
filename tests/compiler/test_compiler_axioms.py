import pdb
import pytest

from normlite.sql.compiler import NotionCompiler
from normlite.sql.schema import Column
from normlite.sql.type_api import Integer, String
from tests.reference.compiler import ReferenceCompiler, assert_compile_equal, reference_compile

@pytest.fixture
def name_col() -> Column:
    return Column(
        'name',
        String(is_title=True)
    )

@pytest.fixture
def id_col() -> Column:
    return Column(
        'id',
        Integer()
    )

@pytest.fixture
def grade_col() -> Column:
    return Column(
        'name',
        String()
    )

@pytest.fixture
def ref_compiler() -> ReferenceCompiler:
    return ReferenceCompiler()

@pytest.fixture
def prod_compiler() -> NotionCompiler:
    return NotionCompiler()

#-------------------------------------------
# Leaf axioms
#-------------------------------------------
def test_compile_does_not_leak_bind_values(name_col):
    """Literal value must never appear in JSON."""
    expr = name_col == "Galilei"

    compiled = reference_compile(expr)

    assert "Galilei" not in str(compiled)
    assert ":param_" in str(compiled)

def test_compile_leaf_equals(name_col, ref_compiler, prod_compiler):
    """Differential test: leaf equality."""
    expr = name_col == "Galilei"
    assert_compile_equal(expr, ref_compiler.process, prod_compiler.process)

def test_compile_leaf_greater_than(id_col, ref_compiler, prod_compiler):
    """Differential test: numeric comparison."""
    expr = id_col > 100
    assert_compile_equal(expr, ref_compiler.process, prod_compiler.process)

#-------------------------------------------
# Boolean operator axioms
#-------------------------------------------
def test_and_compiles_to_list(name_col, grade_col):
    """Axiom: AND produces a list."""
    expr = (name_col == "A") & (grade_col == "B")

    compiled = reference_compile(expr)

    assert "and" in compiled
    assert isinstance(compiled["and"], list)
    assert len(compiled["and"]) == 2

def test_compile_and(name_col, grade_col, ref_compiler, prod_compiler):
    """Differential test: AND."""
    expr = (name_col == "A") & (grade_col == "B")
    assert_compile_equal(expr, ref_compiler.process, prod_compiler.process)

def test_compile_or(name_col, grade_col, ref_compiler, prod_compiler):
    """Differential test: OR."""
    expr = (name_col == "A") | (grade_col == "B")
    assert_compile_equal(expr, ref_compiler.process, prod_compiler.process)

#-------------------------------------------
# NOT operator axioms
#-------------------------------------------
def test_not_wraps_single_expression(name_col):
    expr = ~(name_col == "Galilei")

    compiled = reference_compile(expr)

    assert "not" in compiled
    assert isinstance(compiled["not"], dict)

def test_compile_not(name_col, ref_compiler, prod_compiler):
    """Differential test: OR."""
    expr = ~(name_col == "Galilei")
    assert_compile_equal(expr, ref_compiler.process, prod_compiler.process)

#-------------------------------------------
# Associativity invariants
#-------------------------------------------
def test_and_associativity(name_col, grade_col, id_col, ref_compiler, prod_compiler):
    """AND associativity"""
    e1 = (name_col == "A") & ((grade_col == "B") & (id_col > 10))
    e2 = ((name_col == "A") & (grade_col == "B")) & (id_col > 10)

    assert_compile_equal(e1, ref_compiler.process, prod_compiler.process)
    assert_compile_equal(e2, ref_compiler.process, prod_compiler.process)

    assert reference_compile(e1) == reference_compile(e2)

def test_or_associativity(name_col, grade_col, id_col, ref_compiler, prod_compiler):
    """OR associativity"""
    e1 = (name_col == "A") | ((grade_col == "B") | (id_col > 10))
    e2 = ((name_col == "A") | (grade_col == "B")) | (id_col > 10)

    assert_compile_equal(e1, ref_compiler.process, prod_compiler.process)
    assert_compile_equal(e2, ref_compiler.process, prod_compiler.process)

    assert reference_compile(e1) == reference_compile(e2)

