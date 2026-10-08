from datetime import date
from decimal import Decimal
import json
import difflib
import pdb
from typing import Callable, NoReturn

from normlite.exceptions import UnsupportedCompilationError
from normlite.sql.compiler import _is_pushable
from normlite.sql.elements import BinaryExpression, BindParameter, BooleanClauseList, ColumnElement, Operator, UnaryExpression
from normlite.sql.schema import Column
from normlite.sql.type_api import String

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

    It models the production EMITTER (``NotionCompiler.process``), not a whole
    SELECT site: it drops unpushable terms, repairs ``!=``, and folds. It does NOT
    model ``_prune`` (depth). The emitter expects a root that the gate admits, so
    a caller must ask :meth:`is_pushable` first (see :func:`assert_gated_compile_equal`).

    Every rule here is written independently of the production code, on purpose.
    """
    OPS = {
        Operator.EQ: "equals",
        Operator.NE: "does_not_equal",
        Operator.GT: "greater_than",
        Operator.LT: "less_than",
        Operator.LE: "less_than_or_equal_to",
        Operator.GE: "greater_than_or_equal_to",
        Operator.IN: "contains",
        Operator.NOT_IN: "does_not_contain",
        Operator.STARTSWITH: "starts_with",
        Operator.ENDSWITH: "ends_with",
        Operator.AFTER: "after",
        Operator.BEFORE: "before",
        Operator.IS_EMPTY: "is_empty",
        Operator.IS_NOT_EMPTY: "is_not_empty",
    }
    def __init__(self):
        self._bind_counter = 0
        self._bind_map: dict[BindParameter, str] = {}

    def _add_bindparam(self, bindparam: BindParameter) -> str:
        if bindparam not in self._bind_map:
            key = f"param_{self._bind_counter}"
            self._bind_counter += 1
            self._bind_map[bindparam] = key
        return self._bind_map[bindparam]

    def is_pushable(self, expr: ColumnElement) -> bool:
        """The SELECT (lax) pushability rule.

        * leaf: pushable, unless it compares a ``None`` literal under ``==`` or ``!=``
          (Notion answers HTTP 400). A text (``title`` / ``rich_text``) comparison
          with the empty-string literal ``""`` is not pushable either, under any
          operator. Both rules judge the operand that Notion receives: if the bind
          parameter has a ``callable_``, they judge the value that it returns.
          ``is_empty()``/``is_not_empty()`` also carry a ``None`` operand, but it is
          a placeholder, so they stay pushable.
        * not: never pushable.
        * and: pushable if at least one child is.
        * or: pushable only if every child is.
        """
        if isinstance(expr, BinaryExpression):
            bindparam = expr.value
            operand = bindparam.callable_() if bindparam.callable_ else bindparam.value

            if isinstance(expr.column.type_, String) and operand == "":
                return False

            if expr.operator in (Operator.EQ, Operator.NE) and operand is None:
                return False
            return True

        if isinstance(expr, UnaryExpression):
            return False

        if isinstance(expr, BooleanClauseList):
            pushable = [self.is_pushable(c) for c in expr.clauses]
            return any(pushable) if expr.operator == "and" else all(pushable)

        raise NotImplementedError(type(expr).__name__)

    def process(self, expr: ColumnElement) -> dict:
        method = getattr(self, f"_compile_{expr.__visit_name__}")
        return method(expr)

    def _leaf(self, column: Column, operator: str, value: str) -> dict:
        return {
            "property": column.name,
            column.type_.get_col_spec(): {operator: value},
        }

    def _compile_binary_expression(self, expr: BinaryExpression) -> dict:
        column = expr.column
        notion_type = column.type_.get_col_spec()

        try:
            operator = self.OPS[expr.operator]
        except KeyError:
            raise TypeError(f'Unsupported operator: {expr.operator} for Notion type: "{notion_type}"')

        key = self._add_bindparam(expr.value)
        leaf = self._leaf(column, operator, f":{key}")

        # Notion's does_not_equal also matches a valueless cell; SQL's <> does not.
        # A type that can hold a valueless cell needs is_not_empty to exclude it.
        if expr.operator is Operator.NE and column.type_.supports_valueless_cells:
            return {"and": [leaf, self._leaf(column, "is_not_empty", True)]}

        return leaf

    def _compile_unary_expression(self, expr: UnaryExpression) -> dict:
        # A Notion filter has no "not".
        raise UnsupportedCompilationError(f"Reference compiler refuses unary '{expr.operator}'")

    def _compile_boolean_clause_list(self, expr: BooleanClauseList) -> dict:
        op = expr.operator
        clauses = [self.process(c) for c in expr.clauses if self.is_pushable(c)]

        # fold: drop an empty child, splice a child with the same operator,
        # unwrap a single survivor, and give {} when nothing survives
        folded = []
        for clause in clauses:
            if not clause:
                continue
            if op in clause:
                folded.extend(clause[op])
            else:
                folded.append(clause)

        if not folded:
            return {}
        if len(folded) == 1:
            return folded[0]
        return {op: folded}

def reference_compile(expr: ColumnElement) -> dict:
    compiler = ReferenceCompiler()
    return compiler.process(expr)

def assert_gated_compile_equal(
        ast: ColumnElement,
        reference_compiler: ReferenceCompiler,
        production_compiler: Callable[[ColumnElement], dict],
) -> bool:
    """Assert that both gates agree on ``ast``, then compare a pushable root.

    The production emitter expects a root that ``_is_pushable`` admits. A random
    root may not be one. The two gates must give the same answer. Only a pushable
    root goes to :func:`assert_compile_equal`.

    Returns:
        ``True`` if the root is pushable and the outputs were compared.
    """
    reference_pushable = reference_compiler.is_pushable(ast)
    production_pushable = _is_pushable(ast)
    assert reference_pushable == production_pushable, (
        f"Gates differ: reference={reference_pushable}, production={production_pushable}"
    )

    if reference_pushable:
        assert_compile_equal(ast, reference_compiler.process, production_compiler)

    return reference_pushable

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
        Operator.EQ: "==",
        Operator.NE: "!=",
        Operator.GT: ">",
        Operator.LT: "<",
        Operator.LE: "<=",
        Operator.GE: ">=",
    }

    METHOD_OPS = {
        Operator.IN: "in_",
        Operator.NOT_IN: "not_in",
        Operator.STARTSWITH: "startswith",
        Operator.ENDSWITH: "endswith",
        Operator.AFTER: "after",
        Operator.BEFORE: "before",
        Operator.IS_EMPTY: "is_empty",
        Operator.IS_NOT_EMPTY: "is_not_empty",
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

        if isinstance(value, (int, float, bool, Decimal)):
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
            if op in (Operator.IS_EMPTY, Operator.IS_NOT_EMPTY):
                return f"({left}.{self.METHOD_OPS[op]}())"
            return f"({left}.{self.METHOD_OPS[op]}({right}))"

        raise ValueError(f"Unsupported operator: {op}")

