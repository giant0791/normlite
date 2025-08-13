import pytest
from normlite.sql.dml import Insert, insert
from normlite.sql.sql import CreateTable, text

@pytest.fixture
def insert_stmt() -> Insert:
    students: CreateTable = text("""
        CREATE TABLE students (
            student_id int, 
            name TITLE_VARCHAR(32), 
            grade VARCHAR(1)
        )
    """)

    # monkey-patch the table object as there is no _database_id memeber yet
    students._database_id = 'd9824bdc-8445-4327-be8b-5b47500af6ce'

    stmt: Insert = insert(students)
    stmt.bindparams({'student_id': 1234567, 'name': 'Galileo Galilei', 'grade': 'A'})
    stmt.prepare()
    return stmt

@pytest.fixture
def insert_op(insert_stmt: Insert) -> dict:
    return insert_stmt.operation()

@pytest.fixture
def insert_params(insert_stmt: Insert) -> dict:
    return insert_stmt.parameters()


def test_insert_can_compile():
    students: CreateTable = text("""
        CREATE TABLE students (
            student_id int, 
            name TITLE_VARCHAR(32), 
            grade VARCHAR(1)
        )
    """)

    # monkey-patch the table object as there is no _database_id memeber yet
    students._database_id = 'd9824bdc-8445-4327-be8b-5b47500af6ce'

    stmt: Insert = insert(students)
    stmt.bindparams({'student_id': 1234567, 'name': 'Galileo Galilei', 'grade': 'A'})
    stmt.prepare()
    assert 'endpoint' in stmt.operation().keys() and 'request' in stmt.operation().keys()
    assert 'payload' in stmt.parameters().keys() and 'params' in stmt.parameters().keys()
