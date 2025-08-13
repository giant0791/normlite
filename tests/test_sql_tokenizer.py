# tests/test_sqlparser.py

import pytest
from normlite.sql.sql import tokenize, TokenType

@pytest.mark.parametrize("sql,expected_tokens", [
    (
        "create table students (id int, name varchar(255), grade varchar(1))",
        [
            (TokenType.KEYWORD, 'create'),
            (TokenType.KEYWORD, 'table'),
            (TokenType.IDENTIFIER, 'students'),
            (TokenType.SYMBOL, '('),
            (TokenType.IDENTIFIER, 'id'),
            (TokenType.KEYWORD, 'int'),
            (TokenType.SYMBOL, ','),
            (TokenType.IDENTIFIER, 'name'),
            (TokenType.KEYWORD, 'varchar'),
            (TokenType.SYMBOL, '('),
            (TokenType.NUMBER, '255'),
            (TokenType.SYMBOL, ')'),
            (TokenType.SYMBOL, ','),
            (TokenType.IDENTIFIER, 'grade'),
            (TokenType.KEYWORD, 'varchar'),
            (TokenType.SYMBOL, '('),
            (TokenType.NUMBER, '1'),
            (TokenType.SYMBOL, ')'),
            (TokenType.SYMBOL, ')'),
            (TokenType.EOF, ''),
        ]
    ),
    (
        "insert into students (id, name, grade) values (1, 'Isaac Newton', 'B')",
        [
            (TokenType.KEYWORD, 'insert'),
            (TokenType.KEYWORD, 'into'),
            (TokenType.IDENTIFIER, 'students'),
            (TokenType.SYMBOL, '('),
            (TokenType.IDENTIFIER, 'id'),
            (TokenType.SYMBOL, ','),
            (TokenType.IDENTIFIER, 'name'),
            (TokenType.SYMBOL, ','),
            (TokenType.IDENTIFIER, 'grade'),
            (TokenType.SYMBOL, ')'),
            (TokenType.KEYWORD, 'values'),
            (TokenType.SYMBOL, '('),
            (TokenType.NUMBER, '1'),
            (TokenType.SYMBOL, ','),
            (TokenType.STRING, 'Isaac Newton'),
            (TokenType.SYMBOL, ','),
            (TokenType.STRING, 'B'),
            (TokenType.SYMBOL, ')'),
            (TokenType.EOF, ''),
        ]
    )
])
def test_tokenize_correct_sql(sql, expected_tokens):
    """Test that a syntactically  correct SQL text can be tokenized"""
    assert list(tokenize(sql)) == expected_tokens

@pytest.mark.parametrize("sql,error_contains", [
    ("insert into students (id) values ('unclosed", "Unexpected character"),  # Unterminated string
])
def test_tokenizer_invalid_sql(sql, error_contains):
    with pytest.raises(SyntaxError) as exc:
        tokens = list(tokenize(sql))

    assert error_contains.lower() in str(exc.value).lower()


def test_tokenize_where():
    sql = "where id > 0 and grade = 'C'"

    assert list(tokenize(sql)) == [
        (TokenType.KEYWORD, 'where'),
        (TokenType.IDENTIFIER, 'id'),
        (TokenType.SYMBOL, '>'),
        (TokenType.NUMBER, '0'),
        (TokenType.KEYWORD, 'and'),
        (TokenType.IDENTIFIER, 'grade'),
        (TokenType.SYMBOL, '='),
        (TokenType.STRING, 'C'),
        (TokenType.EOF, '')
    ]


