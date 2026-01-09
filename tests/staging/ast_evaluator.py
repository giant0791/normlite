import pdb
from normlite.sql.elements import BinaryExpression, BindParameter, BooleanClauseList, ColumnElement, UnaryExpression

def bind_equal(a: BindParameter, b: BindParameter) -> bool:
    """Structural equality for bind parameters."""
    if a is b:
        return True

    av = a.value
    ac = a.callable_
    bv = b.value
    bc = b.callable_
    
    if av is None and bv is None:
        return ac is bc

    return av == bv

def ast_equal(a: ColumnElement, b: ColumnElement) -> bool:
    """Structural equality test."""
    if type(a) is not type(b):
        return False

    if isinstance(a, BinaryExpression):
        return (
            ast_equal(a.column, b.column)
            and a.operator == b.operator
            and bind_equal(a.value, b.value)
        )

    if isinstance(a, UnaryExpression):
        return (
            a.operator == b.operator
            and ast_equal(a.element, b.element)
        )

    if isinstance(a, BooleanClauseList):
        return (
            a.operator == b.operator
            and len(a.clauses) == len(b.clauses)
            and all(
                ast_equal(ac, bc)
                for ac, bc in zip(a.clauses, b.clauses)
            )
        )

    if isinstance(a, ColumnElement):
        return (
            a.name == b.name
            and type(a.type_) is type(b.type_)
            and a.primary_key == b.primary_key
        )

    raise AssertionError(f"Unhandled AST node: {type(a)}")

def extract_page_value(page: dict, column: ColumnElement):
    col_spec = column.type_.get_col_spec()   # ← SAFE

    try:
        prop = page["properties"][column.name]
    except KeyError:
        raise KeyError(f"Column '{column.name}' not found in page")

    if col_spec in ("title", "rich_text"):
        try:
            return prop[col_spec][0]["text"]["content"]
        except (KeyError, IndexError, TypeError):
            raise ValueError(
                f"Malformed {col_spec} property for column '{column.name}': {prop}"
            )

    if col_spec == "number":
        try:
            return prop["number"]
        except KeyError:
            raise ValueError(
                f"Malformed number property for column '{column.name}': {prop}"
            )

    raise NotImplementedError(f"Unsupported column type: {col_spec}")

def apply_operator(operator: str, left, right) -> bool:
    """
    Apply a binary operator to scalar values.
    """
    if operator == "eq":
        return left == right

    if operator == "ne":
        return left != right

    if operator == "lt":
        return left < right

    if operator == "gt":
        return left > right

    if operator == "contains":
        return right in left

    if operator == "does_not_contain":
        return right not in left

    if operator == "endswith":
        return isinstance(left, str) and left.endswith(right)

    if operator == "startswith":
        return isinstance(left, str) and left.startswith(right)

    if operator == "in":
        return left in right

    if operator == "not_in":
        return left not in right

    raise NotImplementedError(f"Unsupported operator: {operator}")

def reference_eval_ast(page: dict, expr: ColumnElement) -> bool:
    if isinstance(expr, BooleanClauseList):
        if expr.operator == "and":
            return all(reference_eval_ast(page, c) for c in expr.clauses)
        if expr.operator == "or":
            return any(reference_eval_ast(page, c) for c in expr.clauses)

    if isinstance(expr, UnaryExpression):
        assert expr.operator == "not"
        return not reference_eval_ast(page, expr.element)

    if isinstance(expr, BinaryExpression):
        value = extract_page_value(
            page,
            expr.column.name,
            expr.column.type_
        )
        return apply_operator(expr.operator, value, expr.value)

    raise TypeError(f"Unsupported AST node: {type(expr).__name__}")
