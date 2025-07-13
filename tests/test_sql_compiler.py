import json

import pytest
from normlite.sql import ColumnDef, CreateTable, InsertStatement, MetaData, tokenize, Parser, SqlToJsonVisitor


def test_create_table_to_json():
    sql = "create table students (id int, name varchar(255), grade varchar(1))"
    ast = Parser(tokenize(sql)).parse()
    visitor = SqlToJsonVisitor()
    output = visitor.visit(ast)

    expected = {
        "title": [
            {
                "type": "text",
                "text": {"content": "students"}
            }
        ],
        "properties": {
            "id": {"number": {}},
            "name": {"rich_text": {}},
            "grade": {"rich_text": {}}
        }
    }
    expected_json = json.dumps(expected)

    assert output == expected_json

def test_insert_stmt_to_json():
    sql = "create table students (id int, name varchar(255), grade varchar(1))"
    students_table = Parser(tokenize(sql)).parse()
    table_catalog: MetaData = MetaData()
    table_catalog.add(students_table)

    sql = "insert into students (id, name, grade) values (1, 'Isaac Newton', 'B')"
    ast = Parser(tokenize(sql)).parse()

    visitor = SqlToJsonVisitor(table_catalog)
    output = visitor.visit(ast)

    properties = {
        'id': {'number': 1},
        'name': {'title': [{'text': {'content': 'Isaac Newton'}}]},
        'grade': {'rich_text': [{'text': {'content': 'B'}}]}
    }

    expected = {
        "parent": {
            "type": "database_name",
            "database_name": 'students'
        },
        "properties": properties
    }

def test_compiler_table_def_misspelled():
    # Create a table and add it to the MetaData object
    table_def = Parser(tokenize(
        "create table students (id int, name varchar(255), grade varchar(1))"
    )).parse()
    table_catalog: MetaData = MetaData()
    table_catalog.add(table_def)

    # Create the InsertStatement to be compiled with the misspelled table name
    sql = "insert into student (id, name, grade) values (1, 'Isaac Newton', 'B')"
    ast = Parser(tokenize(sql)).parse()

    # Compile the InsertStatement
    visitor = SqlToJsonVisitor(table_catalog)
    with pytest.raises(KeyError) as exc:
        output = visitor.visit(ast)

    assert f'Unknown table: {ast.table_name}' in str(exc.value)

def test_compiler_table_not_added_to_catalog():
    # Create an empty MetaData object 
    table_catalog: MetaData = MetaData()

    # Create the InsertStatement to be compiled with the misspelled table name
    sql = "insert into students (id, name, grade) values (1, 'Isaac Newton', 'B')"
    ast = Parser(tokenize(sql)).parse()

    # Compile the InsertStatement
    visitor = SqlToJsonVisitor(table_catalog)
    with pytest.raises(KeyError) as exc:
        output = visitor.visit(ast)

    assert f'Unknown table: {ast.table_name}' in str(exc.value)

def test_compiler_no_catalog_provided():
    # Create the InsertStatement to be compiled with the misspelled table name
    sql = "insert into students (id, name, grade) values (1, 'Isaac Newton', 'B')"
    ast = Parser(tokenize(sql)).parse()

    # Compile the InsertStatement
    visitor = SqlToJsonVisitor()
    with pytest.raises(AttributeError) as exc:
        output = visitor.visit(ast)

    assert 'No table catalog defined' in str(exc.value)
