import json
from normlite.sql import tokenize, Parser, SqlToJsonVisitor


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



