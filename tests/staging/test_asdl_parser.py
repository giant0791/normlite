import pytest

from normlite.future.asdl_compiler import compile_asdl

def test_asdl_select_statement():

    # 1. Define the SELECT statement with ASDL
    asdl_def = """
        module sql {
            stmt = SelectStmt(columns: column*, from_table: identifier, where: expr?, order_by: order_item*, limit: int?)
            
            column = Column(name: identifier, alias: identifier?)
            
            order_item = OrderItem(expr: expr, direction: order_dir)
            
            expr = BinaryOp(left: expr, op: operator, right: expr)
                | Identifier(name: identifier)
                | Literal(value: constant)

            order_dir = ASC | DESC

            operator = Eq | Gt | Lt | Ge | Le | Ne | And | Or

            identifier = Identifier(value: string)

            constant = Constant(value: string | int | float)
        }
    """

    # 2. Define a sample SELECT statement to 
    sql_sample = """
        SELECT id, name AS username 
        FROM users 
        WHERE id > 0 AND status = 'active' 
        ORDER BY name ASC 
        LIMIT 10
    """

    ast_tree = """
    SelectStmt(
        columns=[
            Column(name=Identifier("id"), alias=None),
            Column(name=Identifier("name"), alias=Identifier("username"))
        ],
        from_table=Identifier("users"),
        where=BinaryOp(
            left=BinaryOp(
                left=Identifier("id"),
                op=Gt,
                right=Constant(0)
            ),
            op=And,
            right=BinaryOp(
                left=Identifier("status"),
                op=Eq,
                right=Constant("active")
            )
        ),
        order_by=[
            OrderItem(expr=Identifier("name"), direction=ASC)
        ],
        limit=10
    )
    """

    generated_code = compile_asdl(asdl_def)
    assert ast_tree == generated_code