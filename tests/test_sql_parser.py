from normlite.sql.sql import Parser, tokenize, CreateTable, InsertStatement, ColumnDef

def test_parse_create_table_ast():
    sql = "create table students (id int, name title_varchar(255), grade varchar(1))"
    parser = Parser(tokenize(sql))
    ast = parser.parse()

    assert isinstance(ast, CreateTable)
    assert ast.table_name == 'students'
    assert ast.columns == [
        ColumnDef(name='id', type='int'),
        ColumnDef(name='name', type='title_varchar(n)'),
        ColumnDef(name='grade', type='varchar(n)'),
    ]

def test_parse_insert_statement_ast():
    sql = "insert into students (id, name, grade) values (1, 'Isaac Newton', 'B')"
    parser = Parser(tokenize(sql))
    ast = parser.parse()

    assert isinstance(ast, InsertStatement)
    assert ast.table_name == 'students'
    assert ast.columns == ['id', 'name', 'grade']
    assert ast.values == [1, 'Isaac Newton', 'B']
