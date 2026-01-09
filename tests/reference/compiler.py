from datetime import date
import json
import difflib
import pdb
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
    """AST to JSON Reference compiler.

    Simple ground-truth compiler to be used in differential testing to validate the
    production compiler implemented in the :class:`normlite.sql.compiler.NotionCompiler` class.
    """
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
    """AST to Python code compiler.

    This compiler produces a string containing Python code corresponding to
    the initial AST.
    :class:`PythonExpressionCompiler` is used in round-trip differential testing.

    Example:
        >>> ast = BinaryExpression(students.c.name, "equals", BindParameter("Isaac Newton"))
        >>> pec = PythonExpressionCompiler()
        >>> compiled = pec.compile(ast)
        >>> print(compiled)
        "students.c.name == 'Isaac Newton'"

    """
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

        # IMPORTANT: 
        # None values (e.g. empty Notion date) must render to "None"
        if value is None:
            return "None"

        if isinstance(value, str):
            return repr(value)

        if isinstance(value, (int, float, bool)):
            return repr(value)

        if hasattr(value, "isoformat"):  # date / datetime
            return f"date.fromisoformat({repr(value.isoformat())})"

        raise TypeError(f"Unsupported bind value: {value!r}")

    def visit_UnaryExpression(self, expr) -> str:
        if expr.operator == "not":
            return f"not_({self.visit(expr.element)})"
        raise ValueError(f"Unsupported unary operator: {expr.operator}")

    def visit_BooleanClauseList(self, expr) -> str:
        fn = "and_" if expr.operator == "and" else "or_"
        args = ", ".join(self.visit(c) for c in expr.clauses)
        return f"{fn}({args})"

    def visit_BinaryExpression(self, expr) -> str:
        left = self.visit(expr.column)
        right = self.visit(expr.value)
        op = expr.operator

        if op in self.INFIX_OPS:
            return f"({left} {self.INFIX_OPS[op]} {right})"

        if op in self.METHOD_OPS:
            if op in ("is_empty", "is_not_empty"):
                return f"({left}.{op}())"
            return f"({left}.{op}({right}))"

        raise ValueError(f"Unsupported operator: {op}")

