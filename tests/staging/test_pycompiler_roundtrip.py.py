from datetime import date
from decimal import Decimal
import pdb

from tqdm import tqdm
from normlite.sql.elements import BinaryExpression, BooleanClauseList, ColumnElement, UnaryExpression, and_, not_, or_
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, Money, Numeric, String
from tests.generators.genutils import ReferenceGenerator
from tests.reference.compiler import PythonExpressionCompiler, ast_equal, exec_expression

def exec_namespace(table: Table):
    return {
        table.name: table,
        "Decimal": Decimal,
        "date": date,
        "and_": and_,
        "or_": or_,
        "not_": not_,
    }

class RoundTripFailure(Exception):
    def __init__(self, original, compiled, reconstructed):
        self.original = original
        self.compiled = compiled
        self.reconstructed = reconstructed
        super().__init__("AST round-trip mismatch")

def assert_round_trip(expr: ColumnElement, compiler: PythonExpressionCompiler, namespace: dict):
    #pdb.set_trace()
    source = compiler.compile(expr)
    reconstructed = exec_expression(source, namespace)

    should_be_equal = ast_equal(expr, reconstructed)
    if not should_be_equal:
        raise RoundTripFailure(expr, source, reconstructed)

#---------------------------------------------
# Failure minimization
#---------------------------------------------
def ast_children(expr):
    if isinstance(expr, UnaryExpression):
        return [expr.element]

    if isinstance(expr, BinaryExpression):
        return [expr.column, expr.value]

    if isinstance(expr, BooleanClauseList):
        return list(expr.clauses)

    return []

def replace_child(expr, old, new):
    if isinstance(expr, UnaryExpression):
        return UnaryExpression(expr.operator, new)

    if isinstance(expr, BinaryExpression):
        if expr.column is old:
            return BinaryExpression(new, expr.operator, expr.value)
        if expr.value is old:
            return BinaryExpression(expr.column, expr.operator, new)

    if isinstance(expr, BooleanClauseList):
        new_clauses = [
            new if c is old else c
            for c in expr.clauses
        ]
        return BooleanClauseList(expr.operator, new_clauses)

    return expr

def minimize_failure(expr, compiler, namespace):
    """Zeller's delta debugging for AST"""
    def fails(e):
        try:
            assert_round_trip(e, compiler, namespace)
            return False
        except RoundTripFailure:
            return True

    assert fails(expr), "Not a failing expression"

    changed = True
    current = expr

    while changed:
        changed = False

        for child in ast_children(current):
            # Try replacing with child
            if fails(child):
                current = child
                changed = True
                break

            # Try structural shrink
            for grandchild in ast_children(child):
                candidate = replace_child(current, child, grandchild)
                if fails(candidate):
                    current = candidate
                    changed = True
                    break

            if changed:
                break

    return current

def test_roundtrip_differential_bug():
    metadata = MetaData()

    students = Table(
        "students",
        metadata,
        Column("age", Integer()),
        Column("grade", Numeric()),
        Column("balance", Money(currency="euro")),
        Column("start_on", Date()),
        Column("isactive", Boolean()),
        Column("name", String()),
    )
    source = """
        and_(
            (students.c.isactive == False), 
            or_(
                (students.c.start_on.after(None)), 
                (students.c.name.endswith('Douglas Mcdonald'))
            )
        )
    """
    namespace = {
        "students": students,
        "Decimal": Decimal,
        "date": date,
        "and_": and_,
        "or_": or_,
        "not_": not_,
    }

    expr = exec_expression(source, namespace)
    assert isinstance(expr , BooleanClauseList)

def test_roundtrip_differential_massive():
    MAX_COLS = 4
    MAX_ITER = 100
 
    compiler = PythonExpressionCompiler()
    generator = ReferenceGenerator(seed=1)

    for _ in tqdm(range(MAX_ITER), desc='Comparing generated vs compiled ASTs', unit='AST'):
        table, expr, _ = generator.gen_ast(max_cols=MAX_COLS, max_depth=2)
        namespace = exec_namespace(table)

        try:
            assert_round_trip(expr, compiler, namespace)
        except RoundTripFailure as e:
            minimized = minimize_failure(
                e.original,
                compiler,
                namespace,
            )
            raise RoundTripFailure(
                minimized,
                compiler.compile(minimized),
                exec_expression(
                    compiler.compile(minimized),
                    namespace,
                ),
            ) from None
