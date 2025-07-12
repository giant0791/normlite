from __future__ import annotations
from dataclasses import dataclass
import re
from enum import Enum, auto
from typing import List, Tuple, Iterator, Union
from abc import ABC, abstractmethod
import json

class TokenType(Enum):
    KEYWORD = auto()
    IDENTIFIER = auto()
    SYMBOL = auto()
    NUMBER = auto()
    STRING = auto()
    EOF = auto()

Token = Tuple[TokenType, str]

KEYWORDS = {"create", "table", "insert", "into", "values", "int", "varchar"}

TOKEN_REGEX = re.compile(r"""
    (?P<SPACE>\s+)
  | (?P<NUMBER>\d+)
  | (?P<STRING>'[^']*')
  | (?P<IDENTIFIER>[a-zA-Z_][a-zA-Z0-9_]*)
  | (?P<SYMBOL>[(),;])
""", re.IGNORECASE | re.VERBOSE)

def tokenize(sql: str) -> Iterator[Token]:
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
    @abstractmethod
    def accept(self, visitor: Visitor) -> str:
        raise NotImplementedError
    
class Visitor:
    def visit(self, node: SqlNode) -> str:
        return node.accept(self)
    
    def visit_ColumnDef(self, node: ColumnDef) -> str:
        raise NotImplementedError

    def visit_CreateTable(self, node: CreateTable) -> str:
        raise NotImplementedError

    def visit_InsertStatement(self, node: InsertStatement) -> str:
        raise NotImplementedError


@dataclass
class ColumnDef(SqlNode):
    name: str
    type: str

    def accept(self, visitor):
        return visitor.visit_ColumnDef(self)

@dataclass
class CreateTable(SqlNode):
    table_name: str
    columns: List[ColumnDef]

    def accept(self, visitor: Visitor):
        return visitor.visit_CreateTable(self)

@dataclass
class InsertStatement(SqlNode):
    table_name: str
    columns: List[str]
    values: List[Union[int, str]]

    def accept(self, visitor):
        return visitor.visit_InsertStatement(self)

class Parser:
    def __init__(self, tokens: Iterator[Token]):
        self.tokens = iter(tokens)
        self.current = next(self.tokens)

    def eat(self, expected_type, expected_value=None):
        typ, val = self.current
        if typ != expected_type or (expected_value and val != expected_value):
            raise SyntaxError(f"Expected {expected_value or expected_type}, got {val}")
        self.current = next(self.tokens)
        return val

    def parse(self):
        if self.current[1] == "create":
            return self.parse_create_table()
        elif self.current[1] == "insert":
            return self.parse_insert()
        else:
            raise SyntaxError("Unknown statement")

    def parse_create_table(self) -> CreateTable:
        self.eat(TokenType.KEYWORD, "create")
        self.eat(TokenType.KEYWORD, "table")
        table_name = self.eat(TokenType.IDENTIFIER)
        self.eat(TokenType.SYMBOL, "(")

        columns = []
        while True:
            col_name = self.eat(TokenType.IDENTIFIER)
            col_type = self.eat(TokenType.KEYWORD)
            if col_type == "varchar":
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

class SqlToJsonVisitor(Visitor):
    def visit_CreateTable(self, node: CreateTable) -> str:
        obj = {
            "title": [
                {
                    "type": "text",
                    "text": {"content": node.table_name}
                }
            ],
            "properties": {
                col.name: json.loads(col.accept(self))  # string to dict
                for col in node.columns
            }
        }
        return json.dumps(obj)

    def visit_ColumnDef(self, node: ColumnDef) -> str:
        if node.type.startswith("int"):
            return '{"number": {}}'
        elif node.type.startswith("varchar"):
            return '{"rich_text": {}}'
        else:
            raise ValueError(f"Unsupported column type: {node.type}")
