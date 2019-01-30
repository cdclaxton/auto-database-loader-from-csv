from database_loader.database_utilities import create_table_statement, safe_name, insert_data_statement
from database_loader.type_inference import DataType


def test_create_table_statement():
    schema = {"field1": DataType.int,
              "field2": DataType.float,
              "field3": DataType.string,
              "field4": DataType.boolean}

    stmt = create_table_statement("MYTABLE", schema)
    assert stmt == "CREATE TABLE MYTABLE (MYTABLE____ID INT NOT NULL AUTO_INCREMENT, field1 BIGINT, field2 DOUBLE, field3 TEXT, field4 BOOLEAN, PRIMARY KEY (MYTABLE____ID));"


def test_safe_name():
    assert safe_name("this-is-a-test") == "this_is_a_test"
    assert safe_name("this*is/a&test") == "this_is_a_test"


def test_insert_data_statement():
    table_name = "MYDATA"
    schema = {"field1": DataType.string,
              "field2": DataType.float}
    data = {"field1": "example data", "field2": "3"}
    true_values = ["True"]
    false_values = ["False"]

    stmt = insert_data_statement(table_name, schema, data, true_values, false_values)
    assert stmt == """INSERT INTO MYDATA (field1, field2) VALUES ("example data", "3");"""
