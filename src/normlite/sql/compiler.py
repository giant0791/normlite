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
from contextlib import contextmanager
import copy
import pdb
from typing import TYPE_CHECKING, Optional

from normlite._constants import SpecialColumns
from normlite.exceptions import CompileError
from normlite.sql import type_api
from normlite.sql.base import _CompileState, CompilerState, SQLCompiler
from normlite.sql.elements import _BindRole, _BindURoleType, BooleanClauseList, Operator
from normlite.sql.elements import BindParameter


if TYPE_CHECKING:
    from normlite.sql.ddl import CreateColumn, CreateTable, HasTable, ReflectTable
    from normlite.sql.dml import Insert, Select
    from normlite.sql.elements import ColumnElement, UnaryExpression, BinaryExpression, BindParameter

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

        .. rubric:: Example 3: Check whether a given database exists

        .. code-block:: python

            metadata = MetaData()
            students = Table('students', metadata)
            ddl_stmt = HasTable(
                students,
                '66666666-6666-6666-6666-666666666666',             # tables_id
                'university'                                        # table_catalog   
            )
            compiled = ddl_stmt.compile(NotionCompiler())
            compile_dict = compiled.as_dict()

            {
                # operation describes endpoint, request, and template
                # the template uses named parameters which are bound at execution time
                'operation': {
                    'endpoint': 'databases',
                    'request': 'query',
                    'template': {
                        'database_id': ':database_id',                    
                        'filter': {
                            'and': [
                                {
                                    'property': 'table_name',
                                    'title' : {
                                        'equals': ':table_name'
                                    }
                                },
                                {
                                    'property': 'table_catalog',
                                    'rich_text': {
                                        'equals': ':table_catalog'     
                                    }
                                }
                            ]
                        }
                    }
                }

                # bindings for the named parameters
                'parameters': {
                    'database_id': '12345678-9090-0606-1111-123456789012',
                    'table_name': 'students',
                    'table_catalog': 'university'

                }

                # this operation returns the oid of the database found
                'result_columns': ['_no_id']
            }
    """

    def __init__(self):
        self._compiler_state = CompilerState()
        self._bind_counter = 0

    def visit_create_table(self, ddl_stmt: CreateTable) -> dict:
        """Compile a ``CREATE TABLE`` statement.
        
        This visit method compiles the DDL :class:`normlite.sql.ddl.CreateTable` construct into the corresponding Notion payload.

        .. versionchanged:: 0.8.0
            This method now produces a fully parameterized template dictionary and provide the binds in the parameter dictionary.

        Args:
            ddl_stmt (CreateTable): The DDL statement to be compiled.

        Returns:
            dict: The compiled object as dictionary.
        """
        # emit code for parent object
        no_db_obj = {}
        no_db_obj['parent'] = {
            'type': 'page_id', 
            'page_id': ':page_id'
        }

        # emit code for title object
        no_db_obj['title'] = {
            'text': {
                'content': ':table_name'
            }
        }

        # emit code for properties object
        no_prop_obj = {}
        for col in ddl_stmt.columns:
            no_prop_obj.update(self.visit_create_column(col))
        
        no_db_obj['properties'] = no_prop_obj
        operation = dict(endpoint='databases', request='create', template=no_db_obj)
        parameters = dict(page_id=ddl_stmt.table._db_parent_id, table_name=ddl_stmt.table.name)
        result_columns = [colname for colname in ddl_stmt.table.c.keys() if colname in SpecialColumns.values()]
        result_columns = []
        
        return {'operation': operation, 'parameters': parameters, 'result_columns': result_columns}
    
    def visit_create_column(self, ddl_stmt: CreateColumn) -> dict:
        column = ddl_stmt.column
        no_prop_obj = {}
        no_prop_obj[column.name] = column.type_.get_col_spec(None)
        return no_prop_obj
    
    def visit_has_table(self, hastable: HasTable) -> dict:
        """Compile the pseudo DDL statement to check for table existence.

        This visit method compiles the DDL :class:`normlite.sql.ddl.HasTable` construct into a Notion database.query request.

        .. versionadded:: 0.8.0
            This method produces a fully parameterized template dictionary and provide the binds in the parameter dictionary.

        Args:
            hastable (HasTable): The table clause being searched for.

        Returns:
            dict: The dictionary containing the compiled object.
        """
        no_query_obj = {
            'database_id': ':database_id',                    # "tables" database id
            'filter': {
                'and': [
                    {
                        'property': 'table_name',
                        'title' : {
                            'equals': ':table_name'
                        }
                    },
                    {
                        'property': 'table_catalog',
                        'rich_text': {
                            'equals': ':table_catalog'     # _catalog_name is the database name containing this table
                        }
                    }
                ]
            }
        }
        
        operation = dict(endpoint='databases', request='query', template=no_query_obj)
        parameters = {
            'database_id': hastable._tables_id,
            'table_name': hastable.table_name,
            'table_catalog': hastable._table_catalog
        }


        # IMPORTANT: You need the table_id column which stores the found database's id
        result_columns = ['table_id']
        return {'operation': operation, 'parameters': parameters, 'result_columns': result_columns}
    
    def visit_reflect_table(self, reflect_table: ReflectTable) -> dict:
        operation = dict(endpoint='databases', request='retrieve', template={'database_id': ':database_id'})
        database_id = reflect_table.get_table().get_oid()
        parameters = {}
        if database_id:
            parameters = {'database_id': database_id}
            
        return {
            'operation': operation, 
            'parameters': parameters, 
            'is_ddl': True
        }     

    def visit_insert(self, insert: Insert) -> dict:
        """Compile the ``INSERT`` DML statement.

        This visit method compiles the DML :class:`normlite.sql.dml.Insert` construct into a Notion payload for the pages.create request.

        Args:
            insert (Insert): The DML statement to be compiled.

        .. versionchanged:: 0.8.0
            This method extends parameterization via named argument also to the "database_id" key. Thus, the "parameters" dictionary 
            now contains the binding for this key.

        .. versionadded:: 0.7.0
            Initial version supports binding of named arguments for insert values.

        Returns:
            dict: The dictionary containing the compiled object.
        """
        self._compiler_state.is_insert = True
        self._compiler_state.stmt = insert
        no_insert_obj = {}
        db_id_key = None

        with self._compiling(new_state=_CompileState.COMPILING_DBAPI_PARAM):
            db_id_key = self._add_bindparam(
                BindParameter(
                    key='database_id', 
                    value=insert._table.get_oid(), 
                )
            )

            no_insert_obj['parent'] = {
                'type': 'database_id',
                'database_id': f':{db_id_key}'
            }

        with self._compiling(new_state=_CompileState.COMPILING_VALUES):
            no_insert_obj['properties'] = self._compile_insert_update_values(insert._values)

        operation = dict(endpoint='pages', request='create')

        # IMPORTANT: concatenate the values tuple containing the special columns WITH the returning tuple.
        # This ensures that the values for the special columns are always available even if the returning tuple is ().
        self._compiler_state.result_columns = SpecialColumns.values() + insert._returning
        return {'operation': operation, 'payload': no_insert_obj}  

    def visit_select(self, select: Select) -> dict:
        self._compiler_state.is_select = select.is_select
        self._compiler_state.stmt_table = select.table
        operation = dict(endpoit='databases', request='query', template={})
        database_id = select.table.get_oid()
        if database_id is None:
            raise CompileError(f'Table: {select.table.name} has not been previously reflected.')
        parameters = dict(database_id=database_id)

        if select._whereclause.has_expression():
            with self._where_context():
                # emit the JSON code for the filter object of the query
                # in the right context
                filter_obj = select._whereclause.expression._compiler_dispatch(self)
                operation['template']['filter'] = filter_obj 

        self._compiler_state.result_columns = list(select.table.columns.keys())
        return {'operation': operation, 'parameters': parameters}

    def visit_binary_expression(self, expression: BinaryExpression) -> dict:
        return {
            "property": expression.column.name,
            **self._compile_type_filter(
                expression.column,
                expression.operator,
                expression.value
            )
        }
    
    def visit_unary_expression(self, expression: UnaryExpression) -> dict:
        if expression.operator != "not":
            raise NotImplementedError(
                f"Unsupported unary operator: {expression.operator}"
            )

        inner = expression.element._compiler_dispatch(self)

        return {
            "not": inner
        }
    
    def visit_boolean_clause_list(self, expression: BooleanClauseList) -> dict:
        with self._where_context():
            clauses = [
                clause._compiler_dispatch(self)
                for clause in expression.clauses
            ]
        return {
            expression.operator: clauses
        }

    def _next_bind_key(self) -> str:
        key = f"param_{self._bind_counter}"
        self._bind_counter += 1
        return key

    def _add_bindparam(
            self, 
            bindparam: BindParameter, 
            column_name: Optional[str] = None,
        ) -> str:

        if bindparam.role != _BindRole.NO_BINDROLE:
            raise CompileError("BindParameter role already assigned")

        state = self._compiler_state.compile_state

        if state == _CompileState.COMPILING_WHERE:
            # SELECT / WHERE: autogenerated key
            if not column_name is None:
                raise CompileError('Bind parameters in a where clause shall not have a column name.')
            
            key = self._next_bind_key()
            bindparam.key = key
            bindparam.role = _BindRole.COLUMN_FILTER
        
        elif state == _CompileState.COMPILING_VALUES:
           # INSERT / UPDATE: key must be column name
            if not column_name:
                raise CompileError(
                    "BindParameter in insert/update requires a column name"
                )
            key = column_name
            bindparam.key = key
            stmt_table = getattr(self._compiler_state.stmt, '_table', None)
            if stmt_table is None:
                stmt = self._compiler_state.stmt
                raise CompileError(
                    'Expected an insert or update statement, '
                    f'received: {repr(stmt)}'
                )

            try:
                column = stmt_table.c[column_name]
                bindparam.type_ = column.type_
                bindparam.role = _BindRole.COLUMN_VALUE
            except KeyError as ke:
                raise CompileError(
                    f'Column name: {ke.args[0]} not found in table: {stmt_table.name}'
                )

        elif state == _CompileState.COMPILING_DBAPI_PARAM:
            # DBAPI parameter: use the already available key
            if bindparam.key is None:
                raise CompileError('Bind parameter supplied for DBAPI has a None key.')

            key = bindparam.key
            bindparam.role = _BindRole.DBAPI_PARAM
            
        else:
            stmt = self._compiler_state.stmt
            raise CompileError(
                f'Invalid compiler state detected: {state}, '
                f'while compiling statement: {repr(stmt)}.'
            )

        self._compiler_state.execution_binds[key] = bindparam
        return key

    def _compile_type_filter(
            self, 
            column: ColumnElement, 
            operator: Operator, 
            bindparam: BindParameter
    ) -> dict:
        type_ = column.type_
        notion_type = type_.get_col_spec()
        notion_op = type_.supported_ops[operator]

        # allocate placeholder
        key = self._add_bindparam(bindparam)

        # IMPORTANT: No processing here.
        # Compiler must stay syntactic, binding (and processing) is done at execution time
        return {
            notion_type: {
                notion_op: f':{key}'
            }
        }
    
    def _compile_insert_update_values(self, values: dict) -> dict:
        properties = {}
        stmt_table = self._compiler_state.stmt._table
        user_cols = stmt_table.get_user_defined_colums()
        if len(user_cols) != len(values):
            raise CompileError(
                'Not enough values supplied for all user defined columns '
                f'in INSERT statment: {len(user_cols)} != {len(values)}')

        # reorder the keys in values according to the order in user_cols
        ordered_values = {}
        try:
            ordered_values = {
                col.name: values[col.name]
                for col in user_cols
            }
        except KeyError as ke:
            raise CompileError(
                f'No value for column "{ke.args[0]}" found in values {values}'
            ) from ke

        for col, bindparam in zip(user_cols, ordered_values.values()):
            param_key = self._add_bindparam(bindparam, col.name)
            properties[col.name] = f':{param_key}'

        return properties
    
    @contextmanager
    def _compiling(self, new_state: _CompileState):
        prev = self._compiler_state.compile_state
        self._compiler_state.compile_state = new_state
        try:
            yield
        finally:
            self._compiler_state.compile_state = prev
