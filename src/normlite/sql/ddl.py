# sql/ddl.py
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
from __future__ import annotations
import json
import pdb

from normlite.sql.base import DDLVisitor, Visitable
from normlite.sql.schema import Column, Table

class NotionDDLVisitor(DDLVisitor):
    def visit_create_table(self, ddl_stmt: CreateTable) -> dict:
        # emit code for parent object
        no_db_obj = {}
        no_db_obj['parent'] = {
            'type': 'page_id', 
            'page_id': ddl_stmt.table._db_parent_id
        }

        # emit code for title object
        no_db_obj['title'] = {
            'text': {
                'content': ddl_stmt.table.name
            }
        }

        # emit code for properties object
        no_prop_obj = {}
        for col in ddl_stmt.columns:
            no_prop_obj.update(self.visit_create_column(col))
        
        no_db_obj['properties'] = no_prop_obj
        return no_db_obj
    
    def visit_create_column(self, ddl_stmt: CreateColumn):
        column = ddl_stmt.column
        no_prop_obj = {}
        no_prop_obj[column.name] = column.type_.get_col_spec(None)
        return no_prop_obj
    
class CreateTable(Visitable):
    def __init__(self, table: Table):
        super().__init__()
        self.table = table
        self.columns = [
            CreateColumn(col) 
            for col in self.table.c 
            if not col.name.startswith('_no_')      # skip Notion-specific columns
        ]

    def _accept_impl(self, visitor: DDLVisitor) -> dict:
        return visitor.visit_create_table(self)
    
class CreateColumn(Visitable):
    def __init__(self, column: Column):
        super().__init__()
        self.column = column

    def _accept_impl(self, visitor: DDLVisitor) -> dict:
        return visitor.visit_create_column(self)
