from database_loader.loader import table_name_from_filename, build_schema_from_file, build_schema_from_files
from database_loader.type_inference import DataType


def test_table_name_from_filename():
    assert table_name_from_filename("m001_test.csv") == "m001_test"
    assert table_name_from_filename("m001_test_1.csv") == "m001_test"
    assert table_name_from_filename("C:/test/m001_test_1.csv") == "m001_test"
    assert table_name_from_filename("C:/test/m001_test.csv") == "m001_test"


def test_build_schema_from_file():
    filepath = "./database_loader/test_data/test_data_1.csv"
    schema = build_schema_from_file(filepath, ",", "|", "utf-8", ["True"], ["False"])
    assert schema == {'ID': DataType.int,
                      'Pedal name': DataType.string,
                      'Manufacturer': DataType.string,
                      'Type of effect': DataType.string,
                      'Own': DataType.boolean}


def test_build_schema_from_files():
    files = ["./database_loader/test_data/test_data_1.csv",
             "./database_loader/test_data/test_data_2.csv"]

    schema = build_schema_from_files(files, ",", "|", "utf-8", ["True"], ["False"])
    assert schema == {'ID': DataType.int,
                      'Pedal name': DataType.string,
                      'Manufacturer': DataType.string,
                      'Type of effect': DataType.string,
                      'Own': DataType.boolean}
