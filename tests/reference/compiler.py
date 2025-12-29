import json
import difflib
from typing import Callable, NoReturn

from normlite.sql.elements import BinaryExpression, BindParameter, BooleanClauseList, ColumnElement, UnaryExpression

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
