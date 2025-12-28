from normlite.sql.elements import BooleanClauseList, UnaryExpression
from normlite.sql.schema import Column
from normlite.sql.type_api import Integer
from tests.reference.ast_evaluator import reference_eval_ast, extract_page_value, ast_equal

def test_reference_evaluator_uses_only_col_spec():
    assert "get_col_spec" in extract_page_value.__code__.co_names

def test_and_flattening():
    """Boolean normalization axioms"""
    a, b, c = Column("a", Integer()), Column("b", Integer()), Column("c", Integer())
    expr = (a == 1) & ((b == 2) & (c == 3))

    assert isinstance(expr, BooleanClauseList)
    assert expr.operator == "and"
    assert len(expr.clauses) == 3

def test_not_precedence():
    """Operator precedence"""
    a, b = Column("a", Integer()), Column("b", Integer())
    expr = ~(a == 1) & (b == 2)

    assert isinstance(expr, BooleanClauseList)
    assert isinstance(expr.clauses[0], UnaryExpression)

def test_and_associativity_ast():
    a, b, c = Column("a", Integer()), Column("b", Integer()), Column("c", Integer())

    e1 = (a == 1) & ((b == 2) & (c == 3))
    e2 = ((a == 1) & (b == 2)) & (c == 3)

    assert ast_equal(e1, e2)

