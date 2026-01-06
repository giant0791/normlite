from datetime import date
import json
import difflib
from typing import Callable, NoReturn

from normlite.sql.elements import BinaryExpression, BindParameter, BooleanClauseList, ColumnElement, UnaryExpression
from normlite.sql.schema import Column

def normalize_json(obj: dict) -> str:
    """
    Canonical JSON representation suitable for equality comparison.

    * Prevents meaningless whitespace differences using ``separators``.
    * Makes diffs cleaner if tests fail.
    """
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),  # remove insignificant whitespace
        ensure_ascii=False,
    )

def assert_compile_equal(
        ast: ColumnElement, 
        reference_compiler: Callable[[ColumnElement], dict], 
        production_compiler: Callable[[ColumnElement], dict]
) -> NoReturn:
    """Assert compilation equality.

    Args:
        ast (ColumnElement): The AST representing an expression
        reference_compiler (Callable): callable for reference compile function
        production_compiler (Callable): callable for production compile method

    Raises:
        AssertionError: If reference and production compilation results differ.
            The diff is embedded into the assertion error message.
    """
    ref_norm = normalize_json(reference_compiler(ast))
    prod_norm = normalize_json(production_compiler(ast))

    if ref_norm != prod_norm:
        diff = "\n".join(
            difflib.unified_diff(
                ref_norm.splitlines(),
                prod_norm.splitlines(),
                fromfile="reference",
                tofile="production",
                lineterm=""
            )
        )
        raise AssertionError(f"Compiler outputs differ:\n{diff}")
    
class ReferenceCompiler:
    def __init__(self):
        self._bind_counter = 0
        self._bind_map: dict[BindParameter, str] = {}

    def _add_bindparam(self, bindparam: BindParameter) -> str:
        if bindparam not in self._bind_map:
            key = f"param_{self._bind_counter}"
            self._bind_counter += 1
            self._bind_map[bindparam] = key
        return self._bind_map[bindparam]

    def process(self, expr: ColumnElement) -> dict:
        method = getattr(self, f"_compile_{expr.__visit_name__}")
        return method(expr)

    def _compile_binary_expression(self, expr: BinaryExpression) -> dict:
        column = expr.column
        operator = expr.operator
        bindparam = expr.value

        notion_type = column.type_.get_col_spec()
        key = self._add_bindparam(bindparam)

        return {
            "property": column.name,
            notion_type: {
                operator: f":{key}"
            }
        }

    def _compile_unary_expression(self, expr: UnaryExpression) -> dict:
        inner = self.process(expr.element)

        if expr.operator == "not":
            return {"not": inner}

        raise NotImplementedError(expr.operator)

    def _compile_boolean_clause_list(self, expr: BooleanClauseList) -> dict:
        return {
            expr.operator: [self.process(c) for c in expr.clauses]
        }

def reference_compile(expr: ColumnElement) -> dict:
    compiler = ReferenceCompiler()
    return compiler.process(expr)


class PythonExpressionCompiler:
    INFIX_OPS = {
        "equals": "==",
        "does_not_equal": "!=",
        "greater_than": ">",
        "less_than": "<",
    }

    METHOD_OPS = {
        "after": "after",
        "before": "before",
        "contains": "in_",
        "does_not_contain": "not_in",
        "starts_with": "startswith",
        "ends_with": "endswith",
        "is_empty": "is_empty",
        "is_not_empty": "is_not_empty",
    }

    def __init__(self):
        self._param_counter = 0

    def compile(self, expr) -> str:
        return self.visit(expr)

    def visit(self, expr: ColumnElement) -> str:
        method = f"visit_{expr.__class__.__name__}"
        try:
            return getattr(self, method)(expr)
        except AttributeError:
            raise NotImplementedError(f"No compiler for {expr.__class__.__name__}")

    def visit_Column(self, col: Column) -> str:
        # students.c.start_on
        return f"{col.parent.name}.c.{col.name}"

    def visit_BindParameter(self, bp: BindParameter) -> str:
        if bp.callable_ is not None:
            return f"{bp.callable_.__name__}()"

        value = bp.value

        if isinstance(value, str):
            return repr(value)

        if isinstance(value, (int, float, bool)):
            return repr(value)

        if hasattr(value, "isoformat"):  # date / datetime
            return f"date.fromisoformat({repr(value.isoformat())})"

        raise TypeError(f"Unsupported bind value: {value!r}")

    def visit_UnaryExpression(self, expr) -> str:
        operand = self.visit(expr.element)
        if expr.operator == "not":
            return f"(~{operand})"
        raise ValueError(f"Unsupported unary operator: {expr.operator}")

    def visit_BooleanClauseList(self, expr) -> str:
        op = "&" if expr.operator == "and" else "|"
        compiled = [self.visit(c) for c in expr.clauses]
        return "(" + f" {op} ".join(compiled) + ")"

def exec_expression(source: str, namespace: dict):
    globals_ = {
        "__builtins__": {},
        "date": date,
    }
    locals_ = dict(namespace)
    exec(f"result = {source}", globals_, locals_)
    return locals_["result"]

def ast_equal(a, b) -> bool:
    if a is b:
        return True

    if type(a) is not type(b):
        return False

    method = f"_eq_{type(a).__name__}"
    try:
        return globals()[method](a, b)
    except KeyError:
        raise NotImplementedError(f"No equality defined for {type(a).__name__}")

def _eq_Column(a, b):
    return (
        a.name == b.name
        and a.table.name == b.table.name
    )

def _eq_BindParameter(a, b):
    if a.callable_ is not None or b.callable_ is not None:
        return a.callable_ is b.callable_

    return (
        a.value == b.value
        and a.type == b.type
    )

def _eq_BinaryExpression(a, b):
    return (
        a.operator == b.operator
        and ast_equal(a.column, b.column)
        and ast_equal(a.value, b.value)
    )

def _eq_UnaryExpression(a, b):
    return (
        a.operator == b.operator
        and ast_equal(a.element, b.element)
    )

def _eq_BooleanClauseList(a, b):
    if a.operator != b.operator:
        return False

    if len(a.clauses) != len(b.clauses):
        return False

    return all(
        ast_equal(x, y)
        for x, y in zip(a.clauses, b.clauses)
    )

class RoundTripFailure(Exception):
    def __init__(self, original, compiled, reconstructed):
        self.original = original
        self.compiled = compiled
        self.reconstructed = reconstructed
        super().__init__("AST round-trip mismatch")


def assert_round_trip(expr, compiler, namespace):
    source = compiler.compile(expr)
    reconstructed = exec_expression(source, namespace)

    if not ast_equal(expr, reconstructed):
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
