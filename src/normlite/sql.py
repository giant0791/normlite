# sql.py
# Copyright (C) 2025 Gianmarco Antonini
#
# This module is part of normlite and is released under the GNU Affero General Public License.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Provide tokenizer and parser to generate an SQL AST and a cross-compiler for 
the Notion API.

Central module providing SQL parsing as well as cross-compiling SQL-to-JSON capabilities.
To generate ``INSERT`` constructs, the cross-compiler needs a repository where all 
the table metada are stored and accessible.

Example usage for the :class:`Parser`:
    >>> # create an AST for a supported SQL construct
    >>> sql = "create table students (id int, name title_varchar(255), grade varchar(1))"
    >>> parser = Parser(tokenize(sql))
    >>> ast = parser.parse()

    >>> assert isinstance(ast, CreateTable)
    >>> assert ast.table_name == 'students'

Example usage of the cross-compiler :class:`SqlToJsonVisitor`:
    >>> # cross-compile create table
    >>> sql = "create table students (id int, name varchar(255), grade varchar(1))"
    >>> ast = Parser(tokenize(sql)).parse()
    >>> visitor = SqlToJsonVisitor()
    >>> output = visitor.visit(ast)
    >>> print(output)

    >>> # cross-compine insert into
    >>> # Create the table and add it to the table catalog
    >>> sql = "create table students (id int, name varchar(255), grade varchar(1))"
    >>> students_table = Parser(tokenize(sql)).parse()
    >>> table_catalog: MetaData = MetaData()
    >>> table_catalog.add(students_table)
    >>> # Create the insert statement
    >>> sql = "insert into students (id, name, grade) values (1, 'Isaac Newton', 'B')"
    >>> ast = Parser(tokenize(sql)).parse()
    >>> visitor = SqlToJsonVisitor(table_catalog)
    >>> output = visitor.visit(ast)
    >>> print(output)
"""

from __future__ import annotations
import pdb
import re
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple, Iterator, Union
from abc import ABC, abstractmethod

class TokenType(Enum):
    """Enum for token types used in the tokenization."""
    KEYWORD = auto()
    IDENTIFIER = auto()
    SYMBOL = auto()
    NUMBER = auto()
    STRING = auto()
    EOF = auto()

Token = Tuple[TokenType, str]
"""Type alias for a token used by :func:`tokenize()`."""

KEYWORDS = {
    "create", "table", "insert", "into", "values", 
    "int", "varchar", "title_varchar",
    "where" , "and", "or"
}
"""The set of supported SQL keywords (incl. dialect specific ones like ``"title_varchar"``)."""

COMPARISON_OPS = {">=", "<=", "!=", "=", "<", ">"}
"""The set of supported comparison operators in ``WHERE`` clause expressions."""

LOGICAL_OPS = {"and", "or"}
"""The set of supported logical operators in ``WHERE`` clause expressions."""

TOKEN_REGEX = re.compile(r"""
    (?P<SPACE>\s+)
  | (?P<NUMBER>\d+)
  | (?P<STRING>'[^']*')
  | (?P<IDENTIFIER>[a-zA-Z_][a-zA-Z0-9_]*)
  | (?P<SYMBOL>>=|<=|!=|=|<|>|[(),;])
""", re.IGNORECASE | re.VERBOSE)
"""Regular expression representing a single SQL token."""

def tokenize(sql: str) -> Iterator[Token]:
    """Provide a :class:`Token` iterator from the supplied SQL construct.

    Args:
        sql (str): The SQL construct to be tokenized

    Raises:
        SyntaxError: Unexpected charachter at position index.

    Yields:
        Iterator[Token]: The :class:`Token` iterator 
    """
    pos = 0
    while pos < len(sql):
        match = TOKEN_REGEX.match(sql, pos)
        if not match:
            raise SyntaxError(f"Unexpected character at {pos}")
        kind = match.lastgroup
        value = match.group(kind)
        if kind == "SPACE":
            pass  # skip
        elif kind == "NUMBER":
            yield TokenType.NUMBER, value
        elif kind == "STRING":
            yield TokenType.STRING, value.strip("'")
        elif kind == "IDENTIFIER":
            token_type = TokenType.KEYWORD if value.lower() in KEYWORDS else TokenType.IDENTIFIER
            yield token_type, value.lower()
        elif kind == "SYMBOL":
            yield TokenType.SYMBOL, value
        pos = match.end()
    yield TokenType.EOF, ""

class SqlNode(ABC):
    """Base class for an AST node."""

    def __init__(self) -> None:
        self._operation = dict()
        """The compiled operation as dictionary."""

    @property
    def operation(self) -> dict:
        """Provide the compiled operation.

        This read-only attribute holds the result of the compilation.
        It delivers the compiled JSON code after the :meth:`compile()` has been called.

        Returns:
            dict: The compiled JSON code or ``{}``, if :meth:`compile()` has not previously been called.  
        """
        return self._operation

    @abstractmethod
    def accept(self, visitor: Visitor) -> dict:
        """Provide abstract interface for cross-compilation from SQL to JSON.

        Args:
            visitor (Visitor): The visitor object performing the cross-compilation (see :class:`SqlToJsonVisitor`.)

        Returns:
            dict: The cross-compiled JSON code as dictionary.
        """
        raise NotImplementedError
    
    @abstractmethod
    def compile(self) -> None:
        """Compile the node to an executable JSON object.

        Subclasses use this method to create the dictionary representing the operation.
        The following example shows the generated JSON code for the SQL statement ``CREATE TABLE``:

        .. code-block:: json
        
            {
                "endpoint": "databases",
                "request": "create",
                "payload": {
                    "title": [
                        {
                            "type": "text",
                            "text": {"content": "students"}
                        }
                    ],
                    "properties": {
                        "studentid": {"number": {}},
                        "name": {"title": {}},
                        "grade": {"rich_text": {}}
                    }
                }
            }
        
        """
        raise NotImplementedError
        
class Visitor:
    def __init__(self, table_catalog: Optional[MetaData] = None):
        self._table_catalog = table_catalog

    def visit(self, node: SqlNode) -> dict:
        return node.accept(self)
    
    def visit_ColumnDef(self, node: ColumnDef) -> dict:
        raise NotImplementedError

    def visit_CreateTable(self, node: CreateTable) -> dict:
        raise NotImplementedError

    def visit_InsertStatement(self, node: InsertStatement) -> dict:
        raise NotImplementedError

class MetaData():
    """Provide a repository to store table metadata.

    This class represents a table catalog where all table-related metadata are stored.
    It provides a dictionary-like interface to check existence and access stored metadata.
    """
    def __init__(self):
        self.tables: Dict[str, CreateTable] = {}

    def add(self, table: CreateTable) -> None:
        table_name = table.table_name
        if table_name in self.tables:
            raise KeyError(f'MetaData does not allow duplicate tables: {table_name} already exists')    

        self.tables[table_name] = table

    def __contains__(self, table_ident: Union[CreateTable, str]) -> bool:
        name = table_ident.name if isinstance(table_ident, CreateTable) else table_ident
        return (name in self.tables)
    
    def __getitem__(self, tablename: str) -> CreateTable:
        if tablename in self.tables:
            return self.tables[tablename]
        else:
            return None

class ColumnDef(SqlNode):
    """Provide the AST node for SQL constructs like ``studentid int`` as table column."""

    def __init__(self, name: str, type: str):
        super().__init__()
        self.name = name
        self.type = type

    def compile(self) -> None:
        # No implementation as columns are not executable
        ...

    def __eq__(self, value: SqlNode) -> bool:
        if isinstance(value, ColumnDef):
            return self.name == value.name and self.type == value.type

        return False
    
    def __repr__(self) -> str:
        return f'ColumnDef(name="{self.name}, type="{self.type}")'

    def accept(self, visitor) -> dict:
        return visitor.visit_ColumnDef(self)    

class CreateTable(SqlNode):
    """Provide the AST node for the SQL construct ``CREATE TABLE``."""

    def __init__(self, table_name: str, columns: List[ColumnDef]):
        super().__init__()
        self.table_name = table_name
        self.columns = columns

    def compile(self):
        visitor = SqlToJsonVisitor()
        self._operation['endpoint'] = 'databases'
        self._operation['request'] = 'create'
        self._operation['payload'] = self.accept(visitor)

    def __getitem__(self, key: str) -> Optional[ColumnDef]:
        column_list: List[ColumnDef] = [col for col in self.columns if col.name == key]
        if len(column_list) != 1:
            raise KeyError(f'Unknown column name: "{key}"')

        return column_list[0]

    def accept(self, visitor: Visitor) -> dict:
        return visitor.visit_CreateTable(self)

class InsertStatement(SqlNode):
    def __init__(
            self, 
            table_name: str, 
            columns: List[str], 
            values: List[Union[int, str]]
    ):
        self.table_name = table_name
        self.columns = columns
        self.values = values

    def accept(self, visitor) -> dict:
        return visitor.visit_InsertStatement(self)
    
class BinaryOp(SqlNode):
    """Provide the AST node for SQL binary operators in expressions allowed in ``WHERE`` clauses."""
    def __init__(self, left: SqlNode, op: str, right: SqlNode):
        self.left = left
        """The left operand node."""

        self.op = op
        """The operator."""

        self.right = right
        """The right operand node."""

        self.table_clause: CreateTable = None
        """The table clause the columns in the ``WHERE`` clause belong to."""

    def accept(self, visitor) -> dict:
        return visitor.visit_binary_op(self)

    def compile(self) -> None:
        pass

class Where(SqlNode):
    def __init__(self, expr: SqlNode):
        self.expr = expr
        """The AST node for the SQL expression."""

        self.table_clause: CreateTable = None
        """The table clause the ``WHERE`` clause is referred to."""

    def accept(self, visitor):
        return visitor.visit_where(self)

    def compile(self) -> None:
        pass

class Expression(SqlNode):
    """Provide the AST node for an SQL expression in a ``WHERE`` clause."""

    def __init__(self, column: str, operator: str, value: Union[int, float, str]):
        self.column = column
        """The column name in the expression."""

        self.operator = operator
        """The operator in the expression."""

        self.value = value
        """The literal value in the expression."""

        self.table_clause: CreateTable = None
        """The table clause the columns in the ``WHERE`` clause belong to."""

    def accept(self, visitor):
        return visitor.visit_expression(self)
    
    def compile(self) -> None:
        pass
    


class Parser:
    """Create an SQL AST for a given SQL construct."""
    def __init__(self, tokens: Iterator[Token]):
        self.tokens = iter(tokens)
        """The tokenized string as returned by :func:`tokenize()`."""

        self.current = next(self.tokens)
        """The current token being parsed."""

    def eat(self, expected_type, expected_value=None):
        typ, val = self.current
        if typ != expected_type or (expected_value and val != expected_value):
            raise SyntaxError(f"Expected {expected_value or expected_type}, got {val}")
        self.current = next(self.tokens)
        return val

    def parse(self) -> SqlNode:
        """Construct the AST from the input token stream.

        Raises:
            SyntaxError: If the input token stream is syntactically incorrect.

        Returns:
            SqlNode: The root node to the constructed AST.
        """
        if self.current[1] == "create":
            return self.parse_create_table()
        elif self.current[1] == "insert":
            return self.parse_insert()
        elif self.current[1] == "where":
            return self.parse_where()
        else:
            raise SyntaxError("Unknown statement")

    def parse_create_table(self) -> CreateTable:
        self.eat(TokenType.KEYWORD, "create")
        self.eat(TokenType.KEYWORD, "table")
        table_name = self.eat(TokenType.IDENTIFIER)
        self.eat(TokenType.SYMBOL, "(")
        # pdb.set_trace()

        columns = []
        while True:
            col_name = self.eat(TokenType.IDENTIFIER)
            col_type = self.eat(TokenType.KEYWORD)
            if col_type in ["varchar", "title_varchar"]:
                self.eat(TokenType.SYMBOL, "(")
                self.eat(TokenType.NUMBER)
                self.eat(TokenType.SYMBOL, ")")
                col_type += "(n)"
            columns.append(ColumnDef(col_name, col_type))
            if self.current[1] == ")":
                break
            self.eat(TokenType.SYMBOL, ",")

        self.eat(TokenType.SYMBOL, ")")
        return CreateTable(table_name, columns)

    def parse_insert(self) -> InsertStatement:
        self.eat(TokenType.KEYWORD, "insert")
        self.eat(TokenType.KEYWORD, "into")
        table_name = self.eat(TokenType.IDENTIFIER)

        self.eat(TokenType.SYMBOL, "(")
        columns = []
        while True:
            columns.append(self.eat(TokenType.IDENTIFIER))
            if self.current[1] == ")":
                break
            self.eat(TokenType.SYMBOL, ",")
        self.eat(TokenType.SYMBOL, ")")

        self.eat(TokenType.KEYWORD, "values")
        self.eat(TokenType.SYMBOL, "(")
        values = []
        while True:
            typ, val = self.current
            if typ == TokenType.NUMBER:
                values.append(int(val))
            elif typ == TokenType.STRING:
                values.append(val)
            else:
                raise SyntaxError("Invalid value")
            self.eat(typ)
            if self.current[1] == ")":
                break
            self.eat(TokenType.SYMBOL, ",")
        self.eat(TokenType.SYMBOL, ")")

        return InsertStatement(table_name, columns, values)

    def parse_where(self):
        self.eat(TokenType.KEYWORD, "where")
        expr = self.parse_expression()
        return Where(expr)

    def parse_expression(self):
        left = self.parse_primary()

        while self.current[0] == TokenType.KEYWORD and self.current[1] in LOGICAL_OPS:
            op = self.eat(TokenType.KEYWORD)
            right = self.parse_primary()
            left = BinaryOp(left, op, right)

        return left

    def parse_primary(self):
        if self.current == (TokenType.SYMBOL, "("):
            self.eat(TokenType.SYMBOL, "(")
            expr = self.parse_expression()
            self.eat(TokenType.SYMBOL, ")")
            return expr
        return self.parse_comparison()

    def parse_comparison(self):
        left = self.eat(TokenType.IDENTIFIER)
        op = self.eat(TokenType.SYMBOL)
        if op not in COMPARISON_OPS:
            raise SyntaxError(f"Uknown or unsupported operator {op}")
        if self.current[0] == TokenType.NUMBER:
            right = int(self.eat(TokenType.NUMBER))
        elif self.current[0] == TokenType.STRING:
            right = self.eat(TokenType.STRING)
        else:
            raise SyntaxError("Expected a number or string as value")
        return Expression(left, op, right)

class SqlToJsonVisitor(Visitor):
    _op_map: dict = {
        '=': 'equals',
        '!=': 'does_not_equal',
        '>': 'greater_than',
        '<': 'less_than',
        '>=': 'greater_than',
        '<=': 'less_than_or_equal_to',
        'and': 'and',
        'or': 'or'
    }
    """Internal SQL-to-Notion operator mapping."""

    def __init__(self, table_catalog: Optional[MetaData] = None):
        super().__init__(table_catalog)

    def visit_CreateTable(self, node: CreateTable) -> dict:
        title_count = 0
        for col in node.columns:
            if col.type.startswith('title_varchar'):
                title_count += 1
        
        if title_count != 1:
            raise ValueError(
                f"Invalid table schema: expected exactly one 'title_varchar' column, found {title_count}"
            )

        obj = {
            "title": [
                {
                    "type": "text",
                    "text": {"content": node.table_name}
                }
            ],
            "properties": {
                col.name: col.accept(self) 
                for col in node.columns
            }
        }
        return obj

    def visit_ColumnDef(self, node: ColumnDef) -> dict:
        if node.type.startswith("int"):
            return {"number": {}}
        elif node.type.startswith('title_varchar'):
            return {"title": {}}
        elif node.type.startswith("varchar"):
            return {"rich_text": {}}
        else:
            raise ValueError(f"Unsupported column type: {node.type}")

    def visit_InsertStatement(self, node: InsertStatement) -> dict:
        if not self._table_catalog:
            # The insert statement visitor requires a table catalog to 
            # properly process the column type when cross-compiling
            raise AttributeError(
                'No table catalog defined. '
                'Table catalog is required to compile and InsertStatement. '
                'Initialize the visitor with a MetaData object containing '
                'the table you are referring to in the InsertStatement.'
            )
        
        table_def: CreateTable = self._table_catalog[node.table_name]
        if not table_def:
            raise KeyError(
                f'Unknown table: {node.table_name}: '
                f'{node.table_name} must be defined, then added to a MetaData object '
                'prior to compiling the InsertStatement.'
            )

        properties = {}
        column_type_map = {col.name: col.type for col in table_def.columns}

        # TODO: Add check that values have been provided for all columns

        for col, val in zip(node.columns, node.values):
            col_type = column_type_map.get(col, None)

            if isinstance(val, int):
                properties[col] = {"number": val}
            elif isinstance(val, str):
                if col_type.startswith("title_varchar"):
                    properties[col] = {
                        "title": [{"text": {"content": val}}]
                    }
                elif col_type.startswith("varchar"):
                    properties[col] = {
                        "rich-text": [{"text": {"content": val}}]
                    }
                else:
                    raise ValueError(f"Cannot infer mapping for column '{col}' with type '{col_type}'")
            else:
                raise ValueError(f"Unsupported value type for column '{col}': {type(val)}")

        return {
            "parent": {
                "type": "database_name",
                "database_name": node.table_name
            },
            "properties": properties
        }

    def visit_where(self, node: Where) -> dict:
        """Cross-compile an SQL ``WHERE`` clause into the Notion JSON object.

        _description_Args:
            node (Where): The AST node representing the ``WHERE`` clause.

        Returns:
            dict: The cross-compiled Notion JSON ``"filter"`` object.
        """

        # propagate the table clause to the expression
        node.expr.table_clause = node.table_clause
        return {"filter": node.expr.accept(self)}

    def visit_expression(self, node: Expression) -> dict:
        """Cross-compile an SQL expression into the corresponding Notion JSON object.

        Args:
            node (Expression): The AST node representing the expression.

        Returns:
            dict: The cross-compiled Notion JSON ``"property"`` object.
        """
        #pdb.set_trace()

        column_type = node.table_clause[node.column].type
        return {
            "property": node.column,
             column_type: {
                self._op_map[node.operator]: node.value
            }
        }
    
    def visit_binary_op(self, node: BinaryOp) -> dict:
        """Cross-compile a SQL binary operator contained in a SQl expression. 

        Args:
            node (BinaryOp): The AST node representing the binary operator.

        Returns:
            dict: The cross-compiled Notion JSON binary operator string (example: ``"equals"``).
        """
        
        # propagate the table clause to the left and right arms
        node.left.table_clause = node.table_clause
        node.right.table_clause = node.table_clause

        return {
            SqlToJsonVisitor._op_map[node.op]: [
                node.left.accept(self),
                node.right.accept(self)
            ]
        }
            
def text(sqlcode: str) -> SqlNode:
    """Construct a new :class:`SqlNode` node from a textual SQL string directly.

    The main benefits of the :func:`text()` are support for bind parameters, and 
    result-column typing behavior.
    The :func:`text()` enables the simplified SQL code execution as follows, as shown by the 
    following example::

        >>> from normlite.sql import text
        >>> result = connection.execute(text("SELECT * FROM students"))
    
    Bind parameters are specified by name (named parameter style). 
    Example::
    
        >>> from normlite.sql import text
        >>> t = text("SELECT * FROM students WHERE studentid=:studentid")
        >>> result = connection.execute(t, {"studentid": 1234})

    Note:
        :func:`text()` was inspired by the brilliant homonymous SqlAlchemy construct `text() <https://docs.sqlalchemy.org/en/20/core/sqlelement.html#sqlalchemy.sql.expression.text>`_.

    Args:
        sqlcode (str): The string representing an SQL statement.

    Returns:
        SqlNode: The constructed node.
    """
    parser = Parser(tokenize(sqlcode))
    return parser.parse()

