# normlite/future/asdl_compiler.py
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
"""Prototype implementation of a Zephyr ASDL to Python AST node classes parser.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

# ===========================
#  ASDL AST classes
# ===========================

@dataclass
class Module:
    name: str
    types: list[TypeDef]

@dataclass
class TypeDef:
    name: str
    alts: list[Constructor] | None      # sum type (constructors) OR None
    product: Constructor | None         # product type (single constructor) OR None
    is_enum: bool = False                   # all alts have no fields => enum

@dataclass
class Constructor:
    name: str
    fields: list[Field]

@dataclass
class Field:
    name: str
    type: TypeRef

@dataclass
class TypeRef:
    union: list[SingleType]               # e.g. string | int | expr
    opt: bool = False                       # ?
    seq: bool = False                       # *

@dataclass
class SingleType:
    name: str


# ===========================
#  ASDL Lexer/Parser (subset)
# ===========================

_TOKEN_RE = re.compile(r"""
    (?P<SPACE>\s+)           |
    (?P<COMMENT>\-\-[^\n]*)  |
    (?P<SYM>[\{\}\(\)\:\,\=\|\*\?]) |
    (?P<IDENT>[A-Za-z_][A-Za-z0-9_]*)
""", re.VERBOSE)

PRIMITIVES = {"string": "str", "int": "int", "float": "float", "bool": "bool"}

class Tok:
    def __init__(self, kind: str, val: str):
        self.kind = kind
        self.val = val
    
    def __repr__(self): 
        return f"Tok({self.kind!r},{self.val!r})"

def lex_asdl(src: str) -> list[Tok]:
    i = 0
    out: list[Tok] = []
    while i < len(src):
        m = _TOKEN_RE.match(src, i)
        if not m:
            raise SyntaxError(f"ASDL lexer stopped at {i}: {src[i:i+40]!r}")
        kind = m.lastgroup
        val = m.group(kind)
        i = m.end()
        if kind in ("SPACE", "COMMENT"):
            continue
        out.append(Tok(kind, val))
    return out

class Parser:
    def __init__(self, toks: list[Tok]):
        self.toks = toks
        self.i = 0

    def peek(self) -> Tok | None:
        return self.toks[self.i] if self.i < len(self.toks) else None

    def eat(self, kind: str = None, val: str = None) -> Tok:
        t = self.peek()
        if t is None:
            raise SyntaxError("Unexpected end of ASDL")
        if kind and t.kind != kind:
            raise SyntaxError(f"Expected kind {kind}, got {t}")
        if val and t.val != val:
            raise SyntaxError(f"Expected {val}, got {t}")
        self.i += 1
        return t

    def parse(self) -> Module:
        # module <name> { ... }
        kw = self.eat("IDENT").val
        if kw.lower() != "module":
            raise SyntaxError("ASDL must start with 'module'")
        name = self.eat("IDENT").val
        self.eat("SYM", "{")
        tdefs: list[TypeDef] = []
        while True:
            t = self.peek()
            if not t:
                raise SyntaxError("Unclosed module block")
            if t.kind == "SYM" and t.val == "}":
                self.eat("SYM", "}")
                break
            tdefs.append(self.parse_typedef())
        return Module(name=name, types=tdefs)

    def parse_typedef(self) -> TypeDef:
        # typename = Alt(...) | Alt(...) | ...  (sum)  OR
        # typename = Alt(fields...)             (product) OR
        # typename = A | B | C                  (enum)
        name = self.eat("IDENT").val
        self.eat("SYM", "=")

        alts: list[Constructor] = []
        while True:
            alts.append(self.parse_alt())
            t = self.peek()
            if t and t.kind == "SYM" and t.val == "|":
                self.eat("SYM", "|")
                continue
            break

        is_enum = all(len(a.fields) == 0 for a in alts)
        if is_enum:
            return TypeDef(name=name, alts=alts, product=None, is_enum=True)

        # product iff exactly one constructor and its name is used as the concrete node
        if len(alts) == 1:
            return TypeDef(name=name, alts=None, product=alts[0], is_enum=False)

        return TypeDef(name=name, alts=alts, product=None, is_enum=False)

    def parse_alt(self) -> Constructor:
        ident = self.eat("IDENT").val
        t = self.peek()
        if t and t.kind == "SYM" and t.val == "(":
            self.eat("SYM", "(")
            fields: list[Field] = []
            if not (self.peek() and self.peek().kind == "SYM" and self.peek().val == ")"):
                fields.append(self.parse_field())
                while self.peek() and self.peek().kind == "SYM" and self.peek().val == ",":
                    self.eat("SYM", ",")
                    fields.append(self.parse_field())
            self.eat("SYM", ")")
            return Constructor(name=ident, fields=fields)
        # enum alt (bare)
        return Constructor(name=ident, fields=[])

    def parse_field(self) -> Field:
        name = self.eat("IDENT").val
        self.eat("SYM", ":")
        typeref = self.parse_typeref()
        return Field(name=name, type=typeref)

    def parse_typeref(self) -> TypeRef:
        singles = [self.parse_single()]
        while self.peek() and self.peek().kind == "SYM" and self.peek().val == "|":
            self.eat("SYM", "|")
            singles.append(self.parse_single())
        opt = False
        seq = False
        if self.peek() and self.peek().kind == "SYM" and self.peek().val in ("?", "*"):
            sym = self.eat("SYM").val
            opt = (sym == "?")
            seq = (sym == "*")
        return TypeRef(union=singles, opt=opt, seq=seq)

    def parse_single(self) -> SingleType:
        return SingleType(self.eat("IDENT").val)

class BaseAST:
    """Base class for all AST nodes."""
    _fields: tuple[str, ...] = ()

    def __init__(self, **kwargs):
        for field in self._fields:
            setattr(self, field, kwargs.get(field))
    
    def iter_fields(self):
        for f in self._fields:
            yield f, getattr(self, f)

    def __repr__(self):
        args = ", ".join(f"{f}={getattr(self, f)!r}" for f in self._fields)
        return f"{self.__class__.__name__}({args})"

class SelectStmt(BaseAST):
    _fields = ("columns", "from_table", "where", "order_by", "limit")

class Column(BaseAST):
    _fields = ("name", "alias")

class OrderItem(BaseAST):
    _fields = ("expr", "direction")

class BinaryOp(BaseAST):
    _fields = ("left", "op", "right")

class Identifier(BaseAST):
    _fields = ("value",)

class Constant(BaseAST):
    _fields = ("value",)


ASC = "ASC"
DESC = "DESC"

Eq = "="
Gt = ">"
Lt = "<"
Ge = ">="
Le = "<="
Ne = "!="
And = "AND"
Or = "OR"
