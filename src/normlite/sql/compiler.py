# sql/compiler.py
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
import pdb
from typing import TYPE_CHECKING

from normlite._constants import SpecialColumns

from normlite.sql.base import SQLCompiler

if TYPE_CHECKING:
    from normlite.sql.ddl import CreateColumn, CreateTable
    from normlite.sql.dml import Insert

class NotionCompiler(SQLCompiler):
    """Notion compiler for SQL statements.

    This class compiles SQL AST of statements into a Notion API compatible payload.

    .. admonition:: Examples
        :collapsible: open

        .. rubric:: Example 1: Create a new Notion database

        .. code-block:: python

            # create new Notion database for the following Table object.
            metadata = MetaData()
            students = Table(
                'students',
                metadata,
                Column('id', Integer()),
                Column('name', String(is_title=True)),
                Column('grade', String()),
                Column('is_active', Boolean()),
                Column('started_on', Date())
            )

            ddl_stmt = CreateTable(students)
            compiled = ddl_stmt.compile(NotionCompiler())
            print(compiled.string)
            
            # this is the stringified version of the compiled object
            {
                "operation": {                                  # a dictionary specifying the Notion API 
                    "endpoint": "databases",                    # endpoint and 
                    "request": "create",                        # request
                    "template": {                               # parameterized template
                        "parent": {
                            "type": "page_id",
                            "page_id": "12345678-9090-0606-1111-123456789012"
                        },
                        "title": {                              # database name
                            "text": {
                            "content": "students"
                            }
                        },
                        "properties": {                         # database schema
                            "id": {
                                "number": {
                                    "format": "number"
                                }
                            },
                            "name": {
                                "title": {}
                            },
                            "grade": {
                                "rich_text": {}
                            },
                            "is_active": {
                                "checkbox": {}
                            },
                            "started_on": {
                                "date": {}
                            }
                        }
                    }
                },
                "parameters": {},                               # bind parameters, {} as tables do not have parameters
                "result_columns": []                            # no result columns specified
            }

        .. rubric:: Example 2: Add a new page to a Notion database

        .. code-block:: python
        
            # create a new page belonging to the "students" database
            bind_params = {'student_id': 1234567, 'name': 'Galileo Galilei', 'grade': 'A'}
            stmt: Insert = insert(students).values(**expected_params)
            compiled = stmt.compile(NotionCompiler())

            # 
            {
                "operation": {
                    "endpoint": "pages",                        # INSERT corresponds to pages.create
                    "request": "create",
                    "template": {
                        "parent": {                             # parent database to which this page belongs to
                            "type": "database_id",
                            "database_id": "12345678-9090-0606-1111-123456789012"
                        },
                        "properties": {
                            "student_id": {
                                "number": ":student_id"         # named parameterized value :stundent_id
                            },
                            "name": {
                                "title": [
                                    {
                                        "text": {
                                            "content": ":name"
                                        }
                                    }
                                ]
                            },
                            "grade": {
                                "rich_text": [
                                    {
                                        "text": {
                                            "content": ":grade"
                                        }
                                    }
                                ]
                            }
                        }
                    }
                },
                "parameters": {
                    "student_id": 1234567,
                    "name": "Galileo Galilei",
                    "grade": "A"
                }
            }




    """
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
        operation = dict(endpoint='databases', request='create', template=no_db_obj)
        parameters = {}
        result_columns = [colname for colname in ddl_stmt.table.c.keys() if colname in SpecialColumns.values()]
        result_columns = []
        
        return {'operation': operation, 'parameters': parameters, 'result_columns': result_columns}
    
    def visit_create_column(self, ddl_stmt: CreateColumn) -> dict:
        column = ddl_stmt.column
        no_prop_obj = {}
        no_prop_obj[column.name] = column.type_.get_col_spec(None)
        return no_prop_obj

    def visit_insert(self, insert: Insert) -> dict:
        no_insert_obj = {}
        no_insert_obj['parent'] = {
           'type': 'database_id',
           'database_id': insert._table._database_id
        }
        
        properties = {}
        for col in insert._table.c:
            if col.name.startswith('_no_'):
                continue
            bind_proc = col.type_.bind_processor()
            # bind the named parameter not the value yet
            properties[col.name] = bind_proc(f':{col.name}')        

        no_insert_obj['properties'] = properties
        operation = dict(endpoint='pages', request='create', template=no_insert_obj)
        parameters = dict(**insert._values) if insert._values else dict()
        # IMPORTANT: concatenate the values tuple containing the special columns WITH the returning tuple.
        # This ensures that the values for the special columns are always available even if the returning tuple is ().
        result_columns = SpecialColumns.values() + insert._returning
        return {'operation': operation, 'parameters': parameters, 'result_columns': result_columns}    